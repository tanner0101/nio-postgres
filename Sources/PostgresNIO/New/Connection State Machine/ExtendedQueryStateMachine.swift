import NIOCore

struct ExtendedQueryStateMachine {
    
    private enum State {
        case initialized(ExtendedQueryContext)
        case messagesSent(ExtendedQueryContext)
        
        case parseCompleteReceived(ExtendedQueryContext)
        case parameterDescriptionReceived(ExtendedQueryContext)
        case rowDescriptionReceived(ExtendedQueryContext, [RowDescription.Column])
        case noDataMessageReceived(ExtendedQueryContext)
        case emptyQueryResponseReceived

        /// A state that is used if a noData message was received before. If a row description was received `bufferingRows` is
        /// used after receiving a `bindComplete` message
        case bindCompleteReceived(ExtendedQueryContext)
        case streaming([RowDescription.Column], RowStreamStateMachine)
        /// Indicates that the current query was cancelled and we want to drain rows from the connection ASAP
        case drain([RowDescription.Column])
        
        case commandComplete(commandTag: String)
        case error(PSQLError)
        
        case modifying
    }
    
    enum Action {
        case sendParseDescribeBindExecuteSync(PostgresQuery, promise: EventLoopPromise<Void>?)
        case sendParseDescribeSync(name: String, query: String, bindingDataTypes: [PostgresDataType], promise: EventLoopPromise<Void>?)
        case sendBindExecuteSync(PSQLExecuteStatement, promise: EventLoopPromise<Void>?)

        // --- general actions
        case failQuery(EventLoopPromise<PSQLRowStream>, with: PSQLError)
        case succeedQuery(EventLoopPromise<PSQLRowStream>, with: QueryResult)

        case evaluateErrorAtConnectionLevel(PSQLError)

        case succeedPreparedStatementCreation(EventLoopPromise<RowDescription?>, with: RowDescription?)
        case failPreparedStatementCreation(EventLoopPromise<RowDescription?>, with: PSQLError)

        // --- streaming actions
        // actions if query has requested next row but we are waiting for backend
        case forwardRows([DataRow])
        case forwardStreamComplete([DataRow], commandTag: String)
        case forwardStreamError(PSQLError, read: Bool)

        case read
        case wait
    }
    
    private var state: State
    private var isCancelled: Bool
    
    init(queryContext: ExtendedQueryContext) {
        self.isCancelled = false
        self.state = .initialized(queryContext)
    }
    
    mutating func start(_ promise: EventLoopPromise<Void>?) -> Action {
        guard case .initialized(let queryContext) = self.state else {
            preconditionFailure("Start should only be called, if the query has been initialized")
        }
        
        switch queryContext.query {
        case .unnamed(let query, _):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .messagesSent(queryContext)
                return .sendParseDescribeBindExecuteSync(query, promise: promise)
            }

        case .executeStatement(let prepared, _):
            return self.avoidingStateMachineCoW { state -> Action in
                switch prepared.rowDescription {
                case .some(let rowDescription):
                    state = .rowDescriptionReceived(queryContext, rowDescription.columns)
                case .none:
                    state = .noDataMessageReceived(queryContext)
                }
                return .sendBindExecuteSync(prepared, promise: promise)
            }

            /// Not my code, but this is ignoring the last argument which is a promise? is that fine?
        case .prepareStatement(let name, let query, let bindingDataTypes, _):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .messagesSent(queryContext)
                return .sendParseDescribeSync(name: name, query: query, bindingDataTypes: bindingDataTypes, promise: promise)
            }
        }
    }

    mutating func cancel() -> Action {
        switch self.state {
        case .initialized:
            preconditionFailure("Start must be called immediatly after the query was created")

        case .messagesSent(let queryContext),
             .parseCompleteReceived(let queryContext),
             .parameterDescriptionReceived(let queryContext),
             .rowDescriptionReceived(let queryContext, _),
             .noDataMessageReceived(let queryContext),
             .bindCompleteReceived(let queryContext):
            guard !self.isCancelled else {
                return .wait
            }

            self.isCancelled = true
            switch queryContext.query {
            case .unnamed(_, let eventLoopPromise), .executeStatement(_, let eventLoopPromise):
                return .failQuery(eventLoopPromise, with: .queryCancelled)

            case .prepareStatement(_, _, _, let eventLoopPromise):
                return .failPreparedStatementCreation(eventLoopPromise, with: .queryCancelled)
            }

        case .streaming(let columns, var streamStateMachine):
            precondition(!self.isCancelled)
            self.isCancelled = true
            self.state = .drain(columns)
            switch streamStateMachine.fail() {
            case .wait:
                return .forwardStreamError(.queryCancelled, read: false)
            case .read:
                return .forwardStreamError(.queryCancelled, read: true)
            }

        case .commandComplete, .emptyQueryResponseReceived, .error, .drain:
            // the stream has already finished.
            return .wait

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    mutating func parseCompletedReceived() -> Action {
        guard case .messagesSent(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parseComplete))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            state = .parseCompleteReceived(queryContext)
            return .wait
        }
    }
    
    mutating func parameterDescriptionReceived(_ parameterDescription: PostgresBackendMessage.ParameterDescription) -> Action {
        guard case .parseCompleteReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parameterDescription(parameterDescription)))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            state = .parameterDescriptionReceived(queryContext)
            return .wait
        }
    }
    
    mutating func noDataReceived() -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.noData))
        }
        
        switch queryContext.query {
        case .unnamed, .executeStatement:
            return self.avoidingStateMachineCoW { state -> Action in
                state = .noDataMessageReceived(queryContext)
                return .wait
            }

        case .prepareStatement(_, _, _, let promise):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .noDataMessageReceived(queryContext)
                return .succeedPreparedStatementCreation(promise, with: nil)
            }
        }
    }
    
    mutating func rowDescriptionReceived(_ rowDescription: RowDescription) -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.rowDescription(rowDescription)))
        }

        // In Postgres extended queries we receive the `rowDescription` before we send the
        // `Bind` message. Well actually it's vice versa, but this is only true since we do
        // pipelining during a query.
        //
        // In the actual protocol description we receive a rowDescription before the Bind

        // In Postgres extended queries we always request the response rows to be returned in
        // `.binary` format.
        let columns = rowDescription.columns.map { column -> RowDescription.Column in
            var column = column
            column.format = .binary
            return column
        }

        self.avoidingStateMachineCoW { state in
            state = .rowDescriptionReceived(queryContext, columns)
        }

        switch queryContext.query {
        case .unnamed, .executeStatement:
            return .wait

        case .prepareStatement(_, _, _, let eventLoopPromise):
            return .succeedPreparedStatementCreation(eventLoopPromise, with: rowDescription)
        }
    }
    
    mutating func bindCompleteReceived() -> Action {
        switch self.state {
        case .rowDescriptionReceived(let queryContext, let columns):
            switch queryContext.query {
            case .unnamed(_, let eventLoopPromise), .executeStatement(_, let eventLoopPromise):
                return self.avoidingStateMachineCoW { state -> Action in
                    state = .streaming(columns, .init())
                    let result = QueryResult(value: .rowDescription(columns), logger: queryContext.logger)
                    return .succeedQuery(eventLoopPromise, with: result)
                }

            case .prepareStatement:
                return .evaluateErrorAtConnectionLevel(.unexpectedBackendMessage(.bindComplete))
            }

        case .noDataMessageReceived(let queryContext):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .bindCompleteReceived(queryContext)
                return .wait
            }
        case .initialized,
             .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .emptyQueryResponseReceived,
             .bindCompleteReceived,
             .streaming,
             .drain,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.bindComplete))

        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func dataRowReceived(_ dataRow: DataRow) -> Action {
        switch self.state {
        case .streaming(let columns, var demandStateMachine):
            // When receiving a data row, we must ensure that the data row column count
            // matches the previously received row description column count.
            guard dataRow.columnCount == columns.count else {
                return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
            }
            
            return self.avoidingStateMachineCoW { state -> Action in
                demandStateMachine.receivedRow(dataRow)
                state = .streaming(columns, demandStateMachine)
                return .wait
            }

        case .drain(let columns):
            guard dataRow.columnCount == columns.count else {
                return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
            }
            // we ignore all rows and wait for readyForQuery
            return .wait
            
        case .initialized,
             .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .emptyQueryResponseReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func commandCompletedReceived(_ commandTag: String) -> Action {
        switch self.state {
        case .bindCompleteReceived(let context):
            switch context.query {
            case .unnamed(_, let eventLoopPromise), .executeStatement(_, let eventLoopPromise):
                return self.avoidingStateMachineCoW { state -> Action in
                    state = .commandComplete(commandTag: commandTag)
                    let result = QueryResult(value: .noRows(.tag(commandTag)), logger: context.logger)
                    return .succeedQuery(eventLoopPromise, with: result)
                }

            case .prepareStatement:
                preconditionFailure("Invalid state: \(self.state)")
            }
            
        case .streaming(_, var demandStateMachine):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .commandComplete(commandTag: commandTag)
                return .forwardStreamComplete(demandStateMachine.end(), commandTag: commandTag)
            }

        case .drain:
            precondition(self.isCancelled)
            self.state = .commandComplete(commandTag: commandTag)
            return .wait
        
        case .initialized,
             .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .emptyQueryResponseReceived,
             .rowDescriptionReceived,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.commandComplete(commandTag)))
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func emptyQueryResponseReceived() -> Action {
        guard case .bindCompleteReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.emptyQueryResponse))
        }

        switch queryContext.query {
        case .unnamed(_, let eventLoopPromise),
             .executeStatement(_, let eventLoopPromise):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .emptyQueryResponseReceived
                let result = QueryResult(value: .noRows(.emptyResponse), logger: queryContext.logger)
                return .succeedQuery(eventLoopPromise, with: result)
            }

        case .prepareStatement(_, _, _, _):
            return self.setAndFireError(.unexpectedBackendMessage(.emptyQueryResponse))
        }
    }
    
    mutating func errorReceived(_ errorMessage: PostgresBackendMessage.ErrorResponse) -> Action {
        let error = PSQLError.server(errorMessage)
        switch self.state {
        case .initialized:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
        case .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .bindCompleteReceived:
            return self.setAndFireError(error)
        case .rowDescriptionReceived, .noDataMessageReceived:
            return self.setAndFireError(error)
        case .streaming, .drain:
            return self.setAndFireError(error)
        case .commandComplete, .emptyQueryResponseReceived:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
        case .error:
            preconditionFailure("""
                This state must not be reached. If the query `.isComplete`, the
                ConnectionStateMachine must not send any further events to the substate machine.
                """)
            
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func noticeReceived(_ notice: PostgresBackendMessage.NoticeResponse) -> Action {
        //self.queryObject.noticeReceived(notice)
        return .wait
    }
    
    mutating func errorHappened(_ error: PSQLError) -> Action {
        return self.setAndFireError(error)
    }
            
    // MARK: Customer Actions
    
    mutating func requestQueryRows() -> Action {
        switch self.state {
        case .streaming(let columns, var demandStateMachine):
            return self.avoidingStateMachineCoW { state -> Action in
                let action = demandStateMachine.demandMoreResponseBodyParts()
                state = .streaming(columns, demandStateMachine)
                switch action {
                case .read:
                    return .read
                case .wait:
                    return .wait
                }
            }

        case .drain:
            return .wait

        case .initialized,
             .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .emptyQueryResponseReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived:
            preconditionFailure("Requested to consume next row without anything going on.")
            
        case .commandComplete, .error:
            preconditionFailure("The stream is already closed or in a failure state; rows can not be consumed at this time.")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    // MARK: Channel actions
    
    mutating func channelReadComplete() -> Action {
        switch self.state {
        case .initialized,
             .commandComplete,
             .drain,
             .error,
             .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .emptyQueryResponseReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived:
            return .wait
            
        case .streaming(let columns, var demandStateMachine):
            return self.avoidingStateMachineCoW { state -> Action in
                let rows = demandStateMachine.channelReadComplete()
                state = .streaming(columns, demandStateMachine)
                switch rows {
                case .some(let rows):
                    return .forwardRows(rows)
                case .none:
                    return .wait
                }
            }

        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func readEventCaught() -> Action {
        switch self.state {
        case .messagesSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived:
            return .read
        case .streaming(let columns, var demandStateMachine):
            precondition(!self.isCancelled)
            return self.avoidingStateMachineCoW { state -> Action in
                let action = demandStateMachine.read()
                state = .streaming(columns, demandStateMachine)
                switch action {
                case .wait:
                    return .wait
                case .read:
                    return .read
                }
            }
        case .initialized,
             .commandComplete,
             .emptyQueryResponseReceived,
             .drain,
             .error:
            // we already have the complete stream received, now we are waiting for a
            // `readyForQuery` package. To receive this we need to read!
            return .read
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    // MARK: Private Methods
    
    private mutating func setAndFireError(_ error: PSQLError) -> Action {
        switch self.state {
        case .initialized(let context),
             .messagesSent(let context),
             .parseCompleteReceived(let context),
             .parameterDescriptionReceived(let context),
             .rowDescriptionReceived(let context, _),
             .noDataMessageReceived(let context),
             .bindCompleteReceived(let context):
            self.state = .error(error)
            if self.isCancelled {
                return .evaluateErrorAtConnectionLevel(error)
            } else {
                switch context.query {
                case .unnamed(_, let eventLoopPromise), .executeStatement(_, let eventLoopPromise):
                    return .failQuery(eventLoopPromise, with: error)
                case .prepareStatement(_, _, _, let eventLoopPromise):
                    return .failPreparedStatementCreation(eventLoopPromise, with: error)
                }
            }

        case .drain:
            self.state = .error(error)
            return .evaluateErrorAtConnectionLevel(error)
            
        case .streaming(_, var streamStateMachine):
            self.state = .error(error)
            switch streamStateMachine.fail() {
            case .wait:
                return .forwardStreamError(error, read: false)
            case .read:
                return .forwardStreamError(error, read: true)
            }
            
        case .commandComplete, .emptyQueryResponseReceived, .error:
            preconditionFailure("""
                This state must not be reached. If the query `.isComplete`, the
                ConnectionStateMachine must not send any further events to the substate machine.
                """)
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    var isComplete: Bool {
        switch self.state {
        case .commandComplete, .emptyQueryResponseReceived, .error:
            return true

        case .noDataMessageReceived(let context), .rowDescriptionReceived(let context, _):
            switch context.query {
            case .prepareStatement:
                return true
            case .unnamed, .executeStatement:
                return false
            }

        case .initialized, .messagesSent, .parseCompleteReceived, .parameterDescriptionReceived, .bindCompleteReceived, .streaming, .drain:
            return false

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
}

extension ExtendedQueryStateMachine {
    /// So, uh...this function needs some explaining.
    ///
    /// While the state machine logic above is great, there is a downside to having all of the state machine data in
    /// associated data on enumerations: any modification of that data will trigger copy on write for heap-allocated
    /// data. That means that for _every operation on the state machine_ we will CoW our underlying state, which is
    /// not good.
    ///
    /// The way we can avoid this is by using this helper function. It will temporarily set state to a value with no
    /// associated data, before attempting the body of the function. It will also verify that the state machine never
    /// remains in this bad state.
    ///
    /// A key note here is that all callers must ensure that they return to a good state before they exit.
    ///
    /// Sadly, because it's generic and has a closure, we need to force it to be inlined at all call sites, which is
    /// not ideal.
    @inline(__always)
    private mutating func avoidingStateMachineCoW<ReturnType>(_ body: (inout State) -> ReturnType) -> ReturnType {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self.state)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}
