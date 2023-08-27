import NIOCore

struct ConnectionStateMachine {
    
    typealias TransactionState = PostgresBackendMessage.TransactionState
    
    struct ConnectionContext {
        let backendKeyData: Optional<BackendKeyData>
        var parameters: [String: String]
        var transactionState: TransactionState
    }
    
    struct BackendKeyData {
        let processID: Int32
        let secretKey: Int32
    }
    
    enum State {
        enum TLSConfiguration {
            case prefer
            case require
        }

        case initialized
        case sslRequestSent(TLSConfiguration)
        case sslNegotiated
        case sslHandlerAdded
        case waitingToStartAuthentication
        case authenticating(AuthenticationStateMachine)
        case authenticated(BackendKeyData?, [String: String])
        
        case readyForQuery(ConnectionContext)
        case extendedQuery(ExtendedQueryStateMachine, ConnectionContext)
        case closeCommand(CloseStateMachine, ConnectionContext)

        case closing(PSQLError?)
        case closed(clientInitiated: Bool, error: PSQLError?)

        case modifying
    }
    
    enum QuiescingState {
        case notQuiescing
        case quiescing(closePromise: EventLoopPromise<Void>?)
    }
    
    enum ConnectionAction {
        
        struct CleanUpContext {
            enum Action {
                case close
                case fireChannelInactive
            }
            
            let action: Action
            
            /// Tasks to fail with the error
            let tasks: [PSQLTask]
            
            let error: PSQLError
            
            let closePromise: EventLoopPromise<Void>?
        }
        
        case read
        case wait
        case sendSSLRequest
        case establishSSLConnection
        case provideAuthenticationContext
        case forwardNotificationToListeners(PostgresBackendMessage.NotificationResponse)
        case fireEventReadyForQuery
        case fireChannelInactive
        /// Close the connection by sending a `Terminate` message and then closing the connection. This is for clean shutdowns.
        case closeConnection(EventLoopPromise<Void>?)
        
        /// Close connection because of an error state. Fail all tasks with the provided error.
        case closeConnectionAndCleanup(CleanUpContext)
        
        // Auth Actions
        case sendStartupMessage(AuthContext)
        case sendPasswordMessage(PasswordAuthencationMode, AuthContext)
        case sendSaslInitialResponse(name: String, initialResponse: [UInt8])
        case sendSaslResponse([UInt8])
        
        // Connection Actions
        
        // --- general actions
        case sendParseDescribeBindExecuteSync(PostgresQuery)
        case sendBindExecuteSync(PSQLExecuteStatement)
        case failQuery(EventLoopPromise<PSQLRowStream>, with: PSQLError, cleanupContext: CleanUpContext?)
        case succeedQuery(EventLoopPromise<PSQLRowStream>, with: QueryResult)

        // --- streaming actions
        // actions if query has requested next row but we are waiting for backend
        case forwardRows([DataRow])
        case forwardStreamComplete([DataRow], commandTag: String)
        case forwardStreamError(PSQLError, read: Bool, cleanupContext: CleanUpContext?)
        
        // Prepare statement actions
        case sendParseDescribeSync(name: String, query: String)
        case succeedPreparedStatementCreation(EventLoopPromise<RowDescription?>, with: RowDescription?)
        case failPreparedStatementCreation(EventLoopPromise<RowDescription?>, with: PSQLError, cleanupContext: CleanUpContext?)

        // Close actions
        case sendCloseSync(CloseTarget)
        case succeedClose(CloseCommandContext)
        case failClose(CloseCommandContext, with: PSQLError, cleanupContext: CleanUpContext?)
    }
    
    private var state: State
    private let requireBackendKeyData: Bool
    private var taskQueue = CircularBuffer<PSQLTask>()
    private var quiescingState: QuiescingState = .notQuiescing
    
    init(requireBackendKeyData: Bool) {
        self.state = .initialized
        self.requireBackendKeyData = requireBackendKeyData
    }

    #if DEBUG
    /// for testing purposes only
    init(_ state: State, requireBackendKeyData: Bool = true) {
        self.state = state
        self.requireBackendKeyData = requireBackendKeyData
    }
    #endif

    enum TLSConfiguration {
        case disable
        case prefer
        case require
    }
    
    mutating func connected(tls: TLSConfiguration) -> ConnectionAction {
        switch self.state {
        case .initialized:
            switch tls {
            case .disable:
                self.state = .waitingToStartAuthentication
                return .provideAuthenticationContext

            case .prefer:
                self.state = .sslRequestSent(.prefer)
                return .sendSSLRequest

            case .require:
                self.state = .sslRequestSent(.require)
                return .sendSSLRequest
            }

        case .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .extendedQuery,
             .closeCommand,
             .closing,
             .closed,
             .modifying:
            return .wait
        }
    }
    
    mutating func provideAuthenticationContext(_ authContext: AuthContext) -> ConnectionAction {
        self.startAuthentication(authContext)
    }
    
    mutating func gracefulClose(_ promise: EventLoopPromise<Void>?) -> ConnectionAction {
        switch self.state {
        case .closing, .closed:
            // we are already closed, but sometimes an upstream handler might want to close the
            // connection, though it has already been closed by the remote. Typical race condition.
            return .closeConnection(promise)
        case .readyForQuery:
            precondition(self.taskQueue.isEmpty, """
                The state should only be .readyForQuery if there are no more tasks in the queue
                """)
            self.state = .closing(nil)
            return .closeConnection(promise)
        default:
            switch self.quiescingState {
            case .notQuiescing:
                self.quiescingState = .quiescing(closePromise: promise)
            case .quiescing(.some(let closePromise)):
                closePromise.futureResult.cascade(to: promise)
            case .quiescing(.none):
                self.quiescingState = .quiescing(closePromise: promise)
            }
            return .wait
        }
    }

    mutating func close(promise: EventLoopPromise<Void>?) -> ConnectionAction {
        return self.closeConnectionAndCleanup(.clientClosedConnection(underlying: nil), closePromise: promise)
    }

    mutating func closed() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure("How can a connection be closed, if it was never connected.")
        
        case .closed:
            return .wait
        
        case .authenticated,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .readyForQuery,
             .extendedQuery,
             .closeCommand:
            return self.errorHappened(.serverClosedConnection(underlying: nil))

        case .closing(let error):
            self.state = .closed(clientInitiated: true, error: error)
            self.quiescingState = .notQuiescing
            return .fireChannelInactive
            
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func sslSupportedReceived(unprocessedBytes: Int) -> ConnectionAction {
        switch self.state {
        case .sslRequestSent:
            if unprocessedBytes > 0 {
                return self.closeConnectionAndCleanup(.receivedUnencryptedDataAfterSSLRequest)
            }
            self.state = .sslNegotiated
            return .establishSSLConnection
            
        case .initialized,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .extendedQuery,
             .closeCommand,
             .closing,
             .closed:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.sslSupported))
            
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    mutating func sslUnsupportedReceived() -> ConnectionAction {
        switch self.state {
        case .sslRequestSent(.require):
            return self.closeConnectionAndCleanup(.sslUnsupported)

        case .sslRequestSent(.prefer):
            self.state = .waitingToStartAuthentication
            return .provideAuthenticationContext
        
        case .initialized,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .extendedQuery,
             .closeCommand,
             .closing,
             .closed:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.sslSupported))
            
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    mutating func sslHandlerAdded() -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .extendedQuery,
             .closeCommand,
             .closing,
             .closed:
            preconditionFailure("Can only add a ssl handler after negotiation: \(self.state)")
            
        case .sslNegotiated:
            self.state = .sslHandlerAdded
            return .wait

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    mutating func sslEstablished() -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .extendedQuery,
             .closeCommand,
             .closing,
             .closed:
            preconditionFailure("Can only establish a ssl connection after adding a ssl handler: \(self.state)")
            
        case .sslHandlerAdded:
            self.state = .waitingToStartAuthentication
            return .provideAuthenticationContext

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    mutating func authenticationMessageReceived(_ message: PostgresBackendMessage.Authentication) -> ConnectionAction {
        guard case .authenticating(var authState) = self.state else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.authentication(message)))
        }
        
        return self.avoidingStateMachineCoW { machine in
            let action = authState.authenticationMessageReceived(message)
            machine.state = .authenticating(authState)
            return machine.modify(with: action)
        }
    }
    
    mutating func backendKeyDataReceived(_ keyData: PostgresBackendMessage.BackendKeyData) -> ConnectionAction {
        guard case .authenticated(_, let parameters) = self.state else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.backendKeyData(keyData)))
        }
        
        let keyData = BackendKeyData(
            processID: keyData.processID,
            secretKey: keyData.secretKey)
        
        self.state = .authenticated(keyData, parameters)
        return .wait
    }
    
    mutating func parameterStatusReceived(_ status: PostgresBackendMessage.ParameterStatus) -> ConnectionAction {
        switch self.state {
        case .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .closing:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.parameterStatus(status)))
        case .authenticated(let keyData, var parameters):
            return self.avoidingStateMachineCoW { machine in
                parameters[status.parameter] = status.value
                machine.state = .authenticated(keyData, parameters)
                return .wait
            }
        case .readyForQuery(var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .readyForQuery(connectionContext)
                return .wait
            }
        case .extendedQuery(let query, var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .extendedQuery(query, connectionContext)
                return .wait
            }
        case .closeCommand(let closeState, var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .closeCommand(closeState, connectionContext)
                return .wait
            }
        case .initialized,
             .closed:
            preconditionFailure("We shouldn't receive messages if we are not connected")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func errorReceived(_ errorMessage: PostgresBackendMessage.ErrorResponse) -> ConnectionAction {
        switch self.state {
        case .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery:
            return self.closeConnectionAndCleanup(.server(errorMessage))
        case .authenticating(var authState):
            if authState.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = authState.errorReceived(errorMessage)
                machine.state = .authenticating(authState)
                return machine.modify(with: action)
            }
        case .closeCommand(var closeStateMachine, let connectionContext):
            if closeStateMachine.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = closeStateMachine.errorReceived(errorMessage)
                machine.state = .closeCommand(closeStateMachine, connectionContext)
                return machine.modify(with: action)
            }
        case .extendedQuery(var extendedQueryState, let connectionContext):
            if extendedQueryState.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = extendedQueryState.errorReceived(errorMessage)
                machine.state = .extendedQuery(extendedQueryState, connectionContext)
                return machine.modify(with: action)
            }
        case .closing:
            // If the state machine is in state `.closing`, the connection shutdown was initiated
            // by the client. This means a `TERMINATE` message has already been sent and the
            // connection close was passed on to the channel. Therefore we await a channelInactive
            // as the next event.
            // Since a connection close was already issued, we should keep cool and just wait.
            return .wait
        case .initialized, .closed:
            preconditionFailure("We should not receive server errors if we are not connected")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func errorHappened(_ error: PSQLError) -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery:
            return self.closeConnectionAndCleanup(error)
        case .authenticating(var authState):
            let action = authState.errorHappened(error)
            return self.modify(with: action)
        case .extendedQuery(var queryState, _):
            if queryState.isComplete {
                return self.closeConnectionAndCleanup(error)
            } else {
                let action = queryState.errorHappened(error)
                return self.modify(with: action)
            }
        case .closeCommand(var closeState, _):
            if closeState.isComplete {
                return self.closeConnectionAndCleanup(error)
            } else {
                let action = closeState.errorHappened(error)
                return self.modify(with: action)
            }
        case .closing:
            // If the state machine is in state `.closing`, the connection shutdown was initiated
            // by the client. This means a `TERMINATE` message has already been sent and the
            // connection close was passed on to the channel. Therefore we await a channelInactive
            // as the next event.
            // For some reason Azure Postgres does not end ssl cleanly when terminating the
            // connection. More documentation can be found in the issue:
            // https://github.com/vapor/postgres-nio/issues/150
            // Since a connection close was already issued, we should keep cool and just wait.
            return .wait
        case .closed:
            return self.closeConnectionAndCleanup(error)
        
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func noticeReceived(_ notice: PostgresBackendMessage.NoticeResponse) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = extendedQuery.noticeReceived(notice)
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return .wait
        }
    }
    
    mutating func notificationReceived(_ notification: PostgresBackendMessage.NotificationResponse) -> ConnectionAction {
        return .forwardNotificationToListeners(notification)
    }
    
    mutating func readyForQueryReceived(_ transactionState: PostgresBackendMessage.TransactionState) -> ConnectionAction {
        switch self.state {
        case .authenticated(let backendKeyData, let parameters):
            if self.requireBackendKeyData && backendKeyData == nil {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            let connectionContext = ConnectionContext(
                backendKeyData: backendKeyData,
                parameters: parameters,
                transactionState: transactionState)
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .extendedQuery(let extendedQuery, var connectionContext):
            guard extendedQuery.isComplete else {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .closeCommand(let closeStateMachine, var connectionContext):
            guard closeStateMachine.isComplete else {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
            
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
        }
    }
    
    mutating func enqueue(task: PSQLTask) -> ConnectionAction {
        let psqlErrror: PSQLError

        // check if we are quiescing. if so fail task immidiatly
        switch self.quiescingState {
        case .quiescing:
            psqlErrror = PSQLError.clientClosedConnection(underlying: nil)

        case .notQuiescing:
            switch self.state {
            case .initialized,
                 .authenticated,
                 .authenticating,
                 .closeCommand,
                 .extendedQuery,
                 .sslNegotiated,
                 .sslHandlerAdded,
                 .sslRequestSent,
                 .waitingToStartAuthentication:
                self.taskQueue.append(task)
                return .wait

            case .readyForQuery:
                return self.executeTask(task)

            case .closing(let error):
                psqlErrror = PSQLError.clientClosedConnection(underlying: error)

            case .closed(clientInitiated: true, error: let error):
                psqlErrror = PSQLError.clientClosedConnection(underlying: error)

            case .closed(clientInitiated: false, error: let error):
                psqlErrror = PSQLError.serverClosedConnection(underlying: error)

            case .modifying:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        switch task {
        case .extendedQuery(let queryContext):
            switch queryContext.query {
            case .executeStatement(_, let promise), .unnamed(_, let promise):
                return .failQuery(promise, with: psqlErrror, cleanupContext: nil)
            case .prepareStatement(_, _, let promise):
                return .failPreparedStatementCreation(promise, with: psqlErrror, cleanupContext: nil)
            }
        case .closeCommand(let closeContext):
            return .failClose(closeContext, with: psqlErrror, cleanupContext: nil)
        }
    }
    
    mutating func channelReadComplete() -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .authenticated,
             .readyForQuery,
             .closeCommand,
             .closing,
             .closed:
            return .wait
            
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = extendedQuery.channelReadComplete()
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func readEventCaught() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure("Received a read event on a connection that was never opened.")
        case .sslRequestSent:
            return .read
        case .sslNegotiated:
            return .read
        case .sslHandlerAdded:
            return .read
        case .waitingToStartAuthentication:
            return .read
        case .authenticating:
            return .read
        case .authenticated:
            return .read
        case .readyForQuery:
            return .read
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = extendedQuery.readEventCaught()
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        case .closeCommand(var closeState, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = closeState.readEventCaught()
                machine.state = .closeCommand(closeState, connectionContext)
                return machine.modify(with: action)
            }
        case .closing:
            return .read
        case .closed:
            preconditionFailure("How can we receive a read, if the connection is closed")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    // MARK: - Running Queries -
    
    mutating func parseCompleteReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.parseCompletedReceived()
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.parseComplete))
        }
    }
    
    mutating func bindCompleteReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.bindComplete))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.bindCompleteReceived()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func parameterDescriptionReceived(_ description: PostgresBackendMessage.ParameterDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.parameterDescriptionReceived(description)
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.parameterDescription(description)))
        }
    }
    
    mutating func rowDescriptionReceived(_ description: RowDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.rowDescriptionReceived(description)
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.rowDescription(description)))
        }
    }
    
    mutating func noDataReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.noDataReceived()
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.noData))
        }
    }

    mutating func portalSuspendedReceived() -> ConnectionAction {
        self.closeConnectionAndCleanup(.unexpectedBackendMessage(.portalSuspended))
    }
    
    mutating func closeCompletedReceived() -> ConnectionAction {
        guard case .closeCommand(var closeState, let connectionContext) = self.state, !closeState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.closeComplete))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = closeState.closeCompletedReceived()
            machine.state = .closeCommand(closeState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func commandCompletedReceived(_ commandTag: String) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.commandComplete(commandTag)))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.commandCompletedReceived(commandTag)
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func emptyQueryResponseReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.emptyQueryResponse))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.emptyQueryResponseReceived()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func dataRowReceived(_ dataRow: DataRow) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.dataRow(dataRow)))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.dataRowReceived(dataRow)
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    // MARK: Consumer
    
    mutating func cancelQueryStream() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state else {
            preconditionFailure("Tried to cancel stream without active query")
        }

        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.cancel()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func requestQueryRows() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            preconditionFailure("Tried to consume next row, without active query")
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.requestQueryRows()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    // MARK: - Private Methods -
    
    private mutating func startAuthentication(_ authContext: AuthContext) -> ConnectionAction {
        guard case .waitingToStartAuthentication = self.state else {
            preconditionFailure("Can only start authentication after connect or ssl establish")
        }
        
        return self.avoidingStateMachineCoW { machine in
            var authState = AuthenticationStateMachine(authContext: authContext)
            let action = authState.start()
            machine.state = .authenticating(authState)
            return machine.modify(with: action)
        }
    }
    
    private mutating func closeConnectionAndCleanup(_ error: PSQLError, closePromise: EventLoopPromise<Void>? = nil) -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery:
            let cleanupContext = self.setErrorAndCreateCleanupContext(error, closePromise: closePromise)
            return .closeConnectionAndCleanup(cleanupContext)

        case .authenticating(var authState):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error, closePromise: closePromise)

            if authState.isComplete {
                // in case the auth state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            let action = authState.errorHappened(error)
            guard case .reportAuthenticationError = action else {
                preconditionFailure("Expect to fail auth")
            }
            return .closeConnectionAndCleanup(cleanupContext)

        case .extendedQuery(var queryStateMachine, _):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error, closePromise: closePromise)

            if queryStateMachine.isComplete {
                // in case the query state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }

            let action = queryStateMachine.errorHappened(error)
            switch action {
            case .sendParseDescribeBindExecuteSync,
                 .sendParseDescribeSync,
                 .sendBindExecuteSync,
                 .succeedQuery,
                 .succeedPreparedStatementCreation,
                 .forwardRows,
                 .forwardStreamComplete,
                 .wait,
                 .read:
                preconditionFailure("Invalid query state machine action in state: \(self.state), action: \(action)")

            case .evaluateErrorAtConnectionLevel:
                return .closeConnectionAndCleanup(cleanupContext)

            case .failQuery(let queryContext, with: let error):
                return .failQuery(queryContext, with: error, cleanupContext: cleanupContext)

            case .forwardStreamError(let error, let read):
                return .forwardStreamError(error, read: read, cleanupContext: cleanupContext)

            case .failPreparedStatementCreation(let promise, with: let error):
                return .failPreparedStatementCreation(promise, with: error, cleanupContext: cleanupContext)
            }

        case .closeCommand(var closeStateMachine, _):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error, closePromise: closePromise)

            if closeStateMachine.isComplete {
                // in case the close state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            let action = closeStateMachine.errorHappened(error)
            switch action {
            case .sendCloseSync,
                 .succeedClose,
                 .read,
                 .wait:
                preconditionFailure("Invalid close state machine action in state: \(self.state), action: \(action)")
            case .failClose(let closeCommandContext, with: let error):
                return .failClose(closeCommandContext, with: error, cleanupContext: cleanupContext)
            }

        case .closing, .closed:
            // We might run into this case because of reentrancy. For example: After we received an
            // backend unexpected message, that we read of the wire, we bring this connection into
            // the error state and will try to close the connection. However the server might have
            // send further follow up messages. In those cases we will run into this method again
            // and again. We should just ignore those events.
            return .closeConnection(closePromise)

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
    
    private mutating func executeNextQueryFromQueue() -> ConnectionAction {
        guard case .readyForQuery = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        if let task = self.taskQueue.popFirst() {
            return self.executeTask(task)
        }
        
        // if we don't have anything left to do and we are quiescing, next we should close
        if case .quiescing(let promise) = self.quiescingState {
            self.state = .closing(nil)
            return .closeConnection(promise)
        }
        
        return .fireEventReadyForQuery
    }
    
    private mutating func executeTask(_ task: PSQLTask) -> ConnectionAction {
        guard case .readyForQuery(let connectionContext) = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        switch task {
        case .extendedQuery(let queryContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                var extendedQuery = ExtendedQueryStateMachine(queryContext: queryContext)
                let action = extendedQuery.start()
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        case .closeCommand(let closeContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                var closeStateMachine = CloseStateMachine(closeContext: closeContext)
                let action = closeStateMachine.start()
                machine.state = .closeCommand(closeStateMachine, connectionContext)
                return machine.modify(with: action)
            }
        }
    }
    
    struct Configuration {
        let requireTLS: Bool
    }
}

// MARK: CoW helpers

extension ConnectionStateMachine {
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
    private mutating func avoidingStateMachineCoW<ReturnType>(_ body: (inout ConnectionStateMachine) -> ReturnType) -> ReturnType {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}

extension ConnectionStateMachine {
    func shouldCloseConnection(reason error: PSQLError) -> Bool {
        switch error.code.base {
        case .failedToAddSSLHandler,
             .receivedUnencryptedDataAfterSSLRequest,
             .sslUnsupported,
             .messageDecodingFailure,
             .unexpectedBackendMessage,
             .unsupportedAuthMechanism,
             .authMechanismRequiresPassword,
             .saslError,
             .tooManyParameters,
             .invalidCommandTag,
             .connectionError,
             .uncleanShutdown,
             .unlistenFailed:
            return true
        case .queryCancelled:
            return false
        case .server, .listenFailed:
            guard let sqlState = error.serverInfo?[.sqlState] else {
                // any error message that doesn't have a sql state field, is unexpected by default.
                return true
            }
            
            if sqlState.starts(with: "28") {
                // these are authentication errors
                return true
            }
            
            return false
        case .clientClosedConnection:
            preconditionFailure("A pure client error was thrown directly in PostgresConnection, this shouldn't happen")
        case .serverClosedConnection:
            return true
        }
    }

    mutating func setErrorAndCreateCleanupContextIfNeeded(_ error: PSQLError) -> ConnectionAction.CleanUpContext? {
        if self.shouldCloseConnection(reason: error) {
            return self.setErrorAndCreateCleanupContext(error)
        }
        
        return nil
    }
    
    mutating func setErrorAndCreateCleanupContext(_ error: PSQLError, closePromise: EventLoopPromise<Void>? = nil) -> ConnectionAction.CleanUpContext {
        let tasks = Array(self.taskQueue)
        self.taskQueue.removeAll()
        
        var forwardedPromise: EventLoopPromise<Void>? = nil
        if case .quiescing(.some(let quiescePromise)) = self.quiescingState, let closePromise = closePromise {
            quiescePromise.futureResult.cascade(to: closePromise)
            forwardedPromise = quiescePromise
        } else if case .quiescing(.some(let quiescePromise)) = self.quiescingState {
            forwardedPromise = quiescePromise
        } else {
            forwardedPromise = closePromise
        }

        let action: ConnectionAction.CleanUpContext.Action
        if case .serverClosedConnection = error.code.base {
            self.state = .closed(clientInitiated: false, error: error)
            action = .fireChannelInactive
        } else {
            self.state = .closing(error)
            action = .close
        }

        return .init(action: action, tasks: tasks, error: error, closePromise: forwardedPromise)
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: ExtendedQueryStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendParseDescribeBindExecuteSync(let query):
            return .sendParseDescribeBindExecuteSync(query)
        case .sendBindExecuteSync(let executeStatement):
            return .sendBindExecuteSync(executeStatement)
        case .failQuery(let requestContext, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failQuery(requestContext, with: error, cleanupContext: cleanupContext)
        case .succeedQuery(let requestContext, with: let result):
            return .succeedQuery(requestContext, with: result)
        case .forwardRows(let buffer):
            return .forwardRows(buffer)
        case .forwardStreamComplete(let buffer, let commandTag):
            return .forwardStreamComplete(buffer, commandTag: commandTag)
        case .forwardStreamError(let error, let read):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .forwardStreamError(error, read: read, cleanupContext: cleanupContext)

        case .evaluateErrorAtConnectionLevel(let error):
            if let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error) {
                return .closeConnectionAndCleanup(cleanupContext)
            }
            return .wait
        case .read:
            return .read
        case .wait:
            return .wait
        case .sendParseDescribeSync(name: let name, query: let query):
            return .sendParseDescribeSync(name: name, query: query)
        case .succeedPreparedStatementCreation(let promise, with: let rowDescription):
            return .succeedPreparedStatementCreation(promise, with: rowDescription)
        case .failPreparedStatementCreation(let promise, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failPreparedStatementCreation(promise, with: error, cleanupContext: cleanupContext)
        }
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: AuthenticationStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendStartupMessage(let authContext):
            return .sendStartupMessage(authContext)
        case .sendPassword(let mode, let authContext):
            return .sendPasswordMessage(mode, authContext)
        case .sendSaslInitialResponse(let name, let initialResponse):
            return .sendSaslInitialResponse(name: name, initialResponse: initialResponse)
        case .sendSaslResponse(let bytes):
            return .sendSaslResponse(bytes)
        case .authenticated:
            self.state = .authenticated(nil, [:])
            return .wait
        case .wait:
            return .wait
        case .reportAuthenticationError(let error):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            return .closeConnectionAndCleanup(cleanupContext)
        }
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: CloseStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendCloseSync(let sendClose):
            return .sendCloseSync(sendClose)
        case .succeedClose(let closeContext):
            return .succeedClose(closeContext)
        case .failClose(let closeContext, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failClose(closeContext, with: error, cleanupContext: cleanupContext)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

struct SendPrepareStatement {
    let name: String
    let query: String
}

struct AuthContext: Equatable, CustomDebugStringConvertible {
    let username: String
    let password: String?
    let database: String?
    
    var debugDescription: String {
        """
        AuthContext(username: \(String(reflecting: self.username)), \
        password: \(self.password != nil ? "********" : "nil"), \
        database: \(self.database != nil ? String(reflecting: self.database!) : "nil"))
        """
    }
}

enum PasswordAuthencationMode: Equatable {
    case cleartext
    case md5(salt: UInt32)
}

extension ConnectionStateMachine.State: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .initialized:
            return ".initialized"
        case .sslRequestSent:
            return ".sslRequestSent"
        case .sslNegotiated:
            return ".sslNegotiated"
        case .sslHandlerAdded:
            return ".sslHandlerAdded"
        case .waitingToStartAuthentication:
            return ".waitingToStartAuthentication"
        case .authenticating(let authStateMachine):
            return ".authenticating(\(String(reflecting: authStateMachine)))"
        case .authenticated(let backendKeyData, let parameters):
            return ".authenticated(\(String(reflecting: backendKeyData)), \(String(reflecting: parameters)))"
        case .readyForQuery(let connectionContext):
            return ".readyForQuery(connectionContext: \(String(reflecting: connectionContext)))"
        case .extendedQuery(let subStateMachine, let connectionContext):
            return ".extendedQuery(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .closeCommand(let subStateMachine, let connectionContext):
            return ".closeCommand(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .closing:
            return ".closing"
        case .closed:
            return ".closed"
        case .modifying:
            return ".modifying"
        }
    }
}

extension ConnectionStateMachine.ConnectionContext: CustomDebugStringConvertible {
    var debugDescription: String {
        """
        (processID: \(self.backendKeyData?.processID != nil ? String(self.backendKeyData!.processID) : "nil")), \
        secretKey: \(self.backendKeyData?.secretKey != nil ? String(self.backendKeyData!.secretKey) : "nil")), \
        parameters: \(String(reflecting: self.parameters)))
        """
    }
}

extension ConnectionStateMachine.QuiescingState: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .notQuiescing:
            return ".notQuiescing"
        case .quiescing(let closePromise):
            return ".quiescing(\(closePromise != nil ? "\(closePromise!)" : "nil"))"
        }
    }
}

