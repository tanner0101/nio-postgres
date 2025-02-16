import NIOCore
import NIOTLS
import Crypto
import Logging

final class PostgresChannelHandler: ChannelDuplexHandler {
    typealias OutboundIn = HandlerTask
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let logger: Logger
    private let eventLoop: EventLoop
    private var state: ConnectionStateMachine
    
    /// A `ChannelHandlerContext` to be used for non channel related events. (for example: More rows needed).
    ///
    /// The context is captured in `handlerAdded` and released` in `handlerRemoved`
    private var handlerContext: ChannelHandlerContext?
    private var rowStream: PSQLRowStream?
    private var decoder: NIOSingleStepByteToMessageProcessor<PostgresBackendMessageDecoder>
    private var encoder: PostgresFrontendMessageEncoder!
    private let configuration: PostgresConnection.InternalConfiguration
    private let configureSSLCallback: ((Channel, PostgresChannelHandler) throws -> Void)?

    private var listenState = ListenStateMachine()
    private var preparedStatementState = PreparedStatementStateMachine()

    init(
        configuration: PostgresConnection.InternalConfiguration,
        eventLoop: EventLoop,
        logger: Logger,
        configureSSLCallback: ((Channel, PostgresChannelHandler) throws -> Void)?
    ) {
        self.state = ConnectionStateMachine(requireBackendKeyData: configuration.options.requireBackendKeyData)
        self.eventLoop = eventLoop
        self.configuration = configuration
        self.configureSSLCallback = configureSSLCallback
        self.logger = logger
        self.decoder = NIOSingleStepByteToMessageProcessor(PostgresBackendMessageDecoder())
    }
    
    #if DEBUG
    /// for testing purposes only
    init(
        configuration: PostgresConnection.InternalConfiguration,
        eventLoop: EventLoop,
        state: ConnectionStateMachine = .init(.initialized),
        logger: Logger = .psqlNoOpLogger,
        configureSSLCallback: ((Channel, PostgresChannelHandler) throws -> Void)?
    ) {
        self.state = state
        self.eventLoop = eventLoop
        self.configuration = configuration
        self.configureSSLCallback = configureSSLCallback
        self.logger = logger
        self.decoder = NIOSingleStepByteToMessageProcessor(PostgresBackendMessageDecoder())
    }
    #endif

    // MARK: Handler lifecycle
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.handlerContext = context
        self.encoder = PostgresFrontendMessageEncoder(buffer: context.channel.allocator.buffer(capacity: 256))
        
        if context.channel.isActive {
            self.connected(context: context)
        }
    }
    
    func handlerRemoved(context: ChannelHandlerContext) {
        self.handlerContext = nil
    }
    
    // MARK: Channel handler incoming
    
    func channelActive(context: ChannelHandlerContext) {
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a 
        context.fireChannelActive()
        
        self.connected(context: context)
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        do {
            try self.decoder.finishProcessing(seenEOF: true) { message in
                self.handleMessage(message, context: context)
            }
        } catch let error as PostgresMessageDecodingError {
            let action = self.state.errorHappened(.messageDecodingFailure(error))
            self.run(action, with: context)
        } catch {
            preconditionFailure("Expected to only get PSQLDecodingErrors from the PSQLBackendMessageDecoder.")
        }

        self.logger.trace("Channel inactive.")
        let action = self.state.closed()
        self.run(action, with: context)
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.debug("Channel error caught.", metadata: [.error: "\(error)"])
        let action = self.state.errorHappened(.connectionError(underlying: error))
        self.run(action, with: context)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        
        do {
            try self.decoder.process(buffer: buffer) { message in
                self.handleMessage(message, context: context)
            }
        } catch let error as PostgresMessageDecodingError {
            let action = self.state.errorHappened(.messageDecodingFailure(error))
            self.run(action, with: context)
        } catch {
            preconditionFailure("Expected to only get PSQLDecodingErrors from the PSQLBackendMessageDecoder.")
        }
    }

    private func handleMessage(_ message: PostgresBackendMessage, context: ChannelHandlerContext) {
        self.logger.trace("Backend message received", metadata: [.message: "\(message)"])
        let action: ConnectionStateMachine.ConnectionAction

        switch message {
        case .authentication(let authentication):
            action = self.state.authenticationMessageReceived(authentication)
        case .backendKeyData(let keyData):
            action = self.state.backendKeyDataReceived(keyData)
        case .bindComplete:
            action = self.state.bindCompleteReceived()
        case .closeComplete:
            action = self.state.closeCompletedReceived()
        case .commandComplete(let commandTag):
            action = self.state.commandCompletedReceived(commandTag)
        case .dataRow(let dataRow):
            action = self.state.dataRowReceived(dataRow)
        case .emptyQueryResponse:
            action = self.state.emptyQueryResponseReceived()
        case .error(let errorResponse):
            action = self.state.errorReceived(errorResponse)
        case .noData:
            action = self.state.noDataReceived()
        case .notice(let noticeResponse):
            action = self.state.noticeReceived(noticeResponse)
        case .notification(let notification):
            action = self.state.notificationReceived(notification)
        case .parameterDescription(let parameterDescription):
            action = self.state.parameterDescriptionReceived(parameterDescription)
        case .parameterStatus(let parameterStatus):
            action = self.state.parameterStatusReceived(parameterStatus)
        case .parseComplete:
            action = self.state.parseCompleteReceived()
        case .portalSuspended:
            action = self.state.portalSuspendedReceived()
        case .readyForQuery(let transactionState):
            action = self.state.readyForQueryReceived(transactionState)
        case .rowDescription(let rowDescription):
            action = self.state.rowDescriptionReceived(rowDescription)
        case .sslSupported:
            action = self.state.sslSupportedReceived(unprocessedBytes: self.decoder.unprocessedBytes)
        case .sslUnsupported:
            action = self.state.sslUnsupportedReceived()
        }

        self.run(action, with: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        let action = self.state.channelReadComplete()
        self.run(action, with: context)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.logger.trace("User inbound event received", metadata: [
            .userEvent: "\(event)"
        ])
        
        switch event {
        case TLSUserEvent.handshakeCompleted:
            let action = self.state.sslEstablished()
            self.run(action, with: context)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }
    
    // MARK: Channel handler outgoing
    
    func read(context: ChannelHandlerContext) {
        self.logger.trace("Channel read event received")
        let action = self.state.readEventCaught()
        self.run(action, with: context)
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let handlerTask = self.unwrapOutboundIn(data)
        let psqlTask: PSQLTask

        switch handlerTask {
        case .closeCommand(let command):
            psqlTask = .closeCommand(command)
        case .extendedQuery(let query):
            psqlTask = .extendedQuery(query)
        case .simpleQuery(let query):
            psqlTask = .simpleQuery(query)

        case .startListening(let listener):
            switch self.listenState.startListening(listener) {
            case .startListening(let channel):
                psqlTask = self.makeStartListeningQuery(channel: channel, context: context)

            case .none:
                return

            case .succeedListenStart(let listener):
                listener.startListeningSucceeded(handler: self)
                return
            }

        case .cancelListening(let channel, let id):
            switch self.listenState.cancelNotificationListener(channel: channel, id: id) {
            case .none:
                return

            case .stopListening(let channel, let listener):
                psqlTask = self.makeUnlistenQuery(channel: channel, context: context)
                listener.failed(CancellationError())

            case .cancelListener(let listener):
                listener.failed(CancellationError())
                return
            }
        case .executePreparedStatement(let preparedStatement):
            let action = self.preparedStatementState.lookup(
                preparedStatement: preparedStatement
            )
            switch action {
            case .prepareStatement:
                psqlTask = self.makePrepareStatementTask(
                    preparedStatement: preparedStatement,
                    context: context
                )
            case .waitForAlreadyInFlightPreparation:
                // The state machine already keeps track of this
                // and will execute the statement as soon as it's prepared
                return
            case .executeStatement(let rowDescription):
                psqlTask = self.makeExecutePreparedStatementTask(
                    preparedStatement: preparedStatement,
                    rowDescription: rowDescription
                )
            case .returnError(let error):
                preparedStatement.promise.fail(error)
                return
            }
        }

        let action = self.state.enqueue(task: psqlTask)
        self.run(action, with: context)
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.logger.trace("Close triggered by upstream.")
        guard mode == .all else {
            // TODO: Support also other modes ?
            promise?.fail(ChannelError.operationUnsupported)
            return
        }

        let action = self.state.close(promise: promise)
        self.run(action, with: context)
    }
    
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        self.logger.trace("User outbound event received", metadata: [.userEvent: "\(event)"])
        
        switch event {
        case PSQLOutgoingEvent.authenticate(let authContext):
            let action = self.state.provideAuthenticationContext(authContext)
            self.run(action, with: context)

        case PSQLOutgoingEvent.gracefulShutdown:
            let action = self.state.gracefulClose(promise)
            self.run(action, with: context)

        default:
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    // MARK: Listening

    func cancelNotificationListener(channel: String, id: Int) {
        self.eventLoop.preconditionInEventLoop()

        switch self.listenState.cancelNotificationListener(channel: channel, id: id) {
        case .cancelListener(let listener):
            listener.cancelled()

        case .stopListening(let channel, cancelListener: let listener):
            listener.cancelled()

            guard let context = self.handlerContext else {
                return
            }

            let query = self.makeUnlistenQuery(channel: channel, context: context)
            let action = self.state.enqueue(task: query)
            self.run(action, with: context)

        case .none:
            break
        }
    }

    // MARK: Channel handler actions
    
    private func run(_ action: ConnectionStateMachine.ConnectionAction, with context: ChannelHandlerContext) {
        self.logger.trace("Run action", metadata: [.connectionAction: "\(action)"])

        switch action {
        case .establishSSLConnection:
            self.establishSSLConnection(context: context)
        case .read:
            context.read()
        case .wait:
            break
        case .sendStartupMessage(let authContext):
            self.encoder.startup(user: authContext.username, database: authContext.database, options: authContext.additionalParameters)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        case .sendSSLRequest:
            self.encoder.ssl()
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        case .sendPasswordMessage(let mode, let authContext):
            self.sendPasswordMessage(mode: mode, authContext: authContext, context: context)
        case .sendSaslInitialResponse(let name, let initialResponse):
            self.encoder.saslInitialResponse(mechanism: name, bytes: initialResponse)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        case .sendSaslResponse(let bytes):
            self.encoder.saslResponse(bytes)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        case .closeConnectionAndCleanup(let cleanupContext):
            self.closeConnectionAndCleanup(cleanupContext, context: context)
        case .fireChannelInactive:
            context.fireChannelInactive()
        case .sendParseDescribeSync(let name, let query, let bindingDataTypes):
            self.sendParseDescribeAndSyncMessage(statementName: name, query: query, bindingDataTypes: bindingDataTypes, context: context)
        case .sendBindExecuteSync(let executeStatement):
            self.sendBindExecuteAndSyncMessage(executeStatement: executeStatement, context: context)
        case .sendParseDescribeBindExecuteSync(let query):
            self.sendParseDescribeBindExecuteAndSyncMessage(query: query, context: context)
        case .sendQuery(let query):
            self.sendQuery(query: query, context: context)
        case .succeedQuery(let promise, with: let result):
            self.succeedQuery(promise, result: result, context: context)
        case .failQuery(let promise, with: let error, let cleanupContext):
            promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        
        case .forwardRows(let rows):
            self.rowStream!.receive(rows)
            
        case .forwardStreamComplete(let buffer, let commandTag):
            guard let rowStream = self.rowStream else {
                // if the stream was cancelled we don't have it here anymore.
                return
            }
            self.rowStream = nil
            if buffer.count > 0 {
                rowStream.receive(buffer)
            }
            rowStream.receive(completion: .success(commandTag))
            
            
        case .forwardStreamError(let error, let read, let cleanupContext):
            self.rowStream!.receive(completion: .failure(error))
            self.rowStream = nil
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            } else if read {
                context.read()
            }
            
        case .provideAuthenticationContext:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForStartup)
            
            if let username = self.configuration.username {
                let authContext = AuthContext(
                    username: username,
                    password: self.configuration.password,
                    database: self.configuration.database,
                    additionalParameters: self.configuration.options.additionalStartupParameters
                )
                let action = self.state.provideAuthenticationContext(authContext)
                return self.run(action, with: context)
            }
        case .fireEventReadyForQuery:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForQuery)
        case .closeConnection(let promise):
            if context.channel.isActive {
                // The normal, graceful termination procedure is that the frontend sends a Terminate
                // message and immediately closes the connection. On receipt of this message, the
                // backend closes the connection and terminates.
                self.encoder.terminate()
                context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
            }
            context.close(mode: .all, promise: promise)
        case .succeedPreparedStatementCreation(let promise, with: let rowDescription):
            promise.succeed(rowDescription)
        case .failPreparedStatementCreation(let promise, with: let error, let cleanupContext):
            promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .sendCloseSync(let sendClose):
            self.sendCloseAndSyncMessage(sendClose, context: context)
        case .succeedClose(let closeContext):
            closeContext.promise.succeed(Void())
        case .failClose(let closeContext, with: let error, let cleanupContext):
            closeContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .forwardNotificationToListeners(let notification):
            self.forwardNotificationToListeners(notification, context: context)
        }
    }
    
    // MARK: - Private Methods -
    
    private func connected(context: ChannelHandlerContext) {
        let action = self.state.connected(tls: .init(self.configuration.tls))
        self.run(action, with: context)
    }
    
    private func establishSSLConnection(context: ChannelHandlerContext) {
        // This method must only be called, if we signalized the StateMachine before that we are
        // able to setup a SSL connection.
        do {
            try self.configureSSLCallback!(context.channel, self)
            let action = self.state.sslHandlerAdded()
            self.run(action, with: context)
        } catch {
            let action = self.state.errorHappened(.failedToAddSSLHandler(underlying: error))
            self.run(action, with: context)
        }
    }
    
    private func sendPasswordMessage(
        mode: PasswordAuthencationMode,
        authContext: AuthContext,
        context: ChannelHandlerContext
    ) {
        switch mode {
        case .md5(let salt):
            let hash1 = (authContext.password ?? "") + authContext.username
            let pwdhash = Insecure.MD5.hash(data: [UInt8](hash1.utf8)).asciiHexDigest()

            var hash2 = [UInt8]()
            hash2.reserveCapacity(pwdhash.count + 4)
            hash2.append(contentsOf: pwdhash)
            var saltNetworkOrder = salt.bigEndian
            withUnsafeBytes(of: &saltNetworkOrder) { ptr in
                hash2.append(contentsOf: ptr)
            }
            let hash = Insecure.MD5.hash(data: hash2).md5PrefixHexdigest()
            
            self.encoder.password(hash.utf8)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)

        case .cleartext:
            self.encoder.password((authContext.password ?? "").utf8)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        }
    }
    
    private func sendCloseAndSyncMessage(_ sendClose: CloseTarget, context: ChannelHandlerContext) {
        switch sendClose {
        case .preparedStatement(let name):
            self.encoder.closePreparedStatement(name)
            self.encoder.sync()
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
            
        case .portal(let name):
            self.encoder.closePortal(name)
            self.encoder.sync()
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
        }
    }
    
    private func sendParseDescribeAndSyncMessage(
        statementName: String,
        query: String,
        bindingDataTypes: [PostgresDataType],
        context: ChannelHandlerContext
    ) {
        precondition(self.rowStream == nil, "Expected to not have an open stream at this point")
        self.encoder.parse(preparedStatementName: statementName, query: query, parameters: bindingDataTypes)
        self.encoder.describePreparedStatement(statementName)
        self.encoder.sync()
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
    }
    
    private func sendBindExecuteAndSyncMessage(
        executeStatement: PSQLExecuteStatement,
        context: ChannelHandlerContext
    ) {
        self.encoder.bind(
            portalName: "",
            preparedStatementName: executeStatement.name,
            bind: executeStatement.binds
        )
        self.encoder.execute(portalName: "")
        self.encoder.sync()
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
    }
    
    private func sendParseDescribeBindExecuteAndSyncMessage(
        query: PostgresQuery,
        context: ChannelHandlerContext
    ) {
        precondition(self.rowStream == nil, "Expected to not have an open stream at this point")
        let unnamedStatementName = ""
        self.encoder.parse(
            preparedStatementName: unnamedStatementName,
            query: query.sql,
            parameters: query.binds.metadata.lazy.map(\.dataType)
        )
        self.encoder.describePreparedStatement(unnamedStatementName)
        self.encoder.bind(portalName: "", preparedStatementName: unnamedStatementName, bind: query.binds)
        self.encoder.execute(portalName: "")
        self.encoder.sync()
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
    }

    private func sendQuery(
        query: String,
        context: ChannelHandlerContext
    ) {
        precondition(self.rowStream == nil, "Expected to not have an open stream at this point")
        self.encoder.query(query)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flushBuffer()), promise: nil)
    }

    private func succeedQuery(
        _ promise: EventLoopPromise<PSQLRowStream>,
        result: QueryResult,
        context: ChannelHandlerContext
    ) {
        let rows: PSQLRowStream
        switch result.value {
        case .rowDescription(let columns):
            rows = PSQLRowStream(
                source: .stream(columns, self),
                eventLoop: context.channel.eventLoop,
                logger: result.logger
            )
            self.rowStream = rows

        case .noRows(let summary):
            rows = PSQLRowStream(
                source: .noRows(.success(summary)),
                eventLoop: context.channel.eventLoop,
                logger: result.logger
            )
        }

        promise.succeed(rows)
    }
    
    private func closeConnectionAndCleanup(
        _ cleanup: ConnectionStateMachine.ConnectionAction.CleanUpContext,
        context: ChannelHandlerContext
    ) {
        self.logger.debug("Cleaning up and closing connection.", metadata: [.error: "\(cleanup.error)"])
        
        // 1. fail all tasks
        cleanup.tasks.forEach { task in
            task.failWithError(cleanup.error)
        }

        // 2. stop all listeners
        for listener in self.listenState.fail(cleanup.error) {
            listener.failed(cleanup.error)
        }

        // 3. fire an error
        if cleanup.error.code != .clientClosedConnection {
            context.fireErrorCaught(cleanup.error)
        }

        // 4. close the connection or fire channel inactive
        switch cleanup.action {
        case .close:
            context.close(mode: .all, promise: cleanup.closePromise)
        case .fireChannelInactive:
            cleanup.closePromise?.succeed(())
            context.fireChannelInactive()
        }
    }

    private func makeStartListeningQuery(channel: String, context: ChannelHandlerContext) -> PSQLTask {
        let promise = context.eventLoop.makePromise(of: PSQLRowStream.self)
        let query = ExtendedQueryContext(
            query: PostgresQuery(unsafeSQL: #"LISTEN "\#(channel)";"#),
            logger: self.logger,
            promise: promise
        )
        let loopBound = NIOLoopBound((self, context), eventLoop: self.eventLoop)
        promise.futureResult.whenComplete { result in
            let (selfTransferred, context) = loopBound.value
            selfTransferred.startListenCompleted(result, for: channel, context: context)
        }

        return .extendedQuery(query)
    }

    private func startListenCompleted(_ result: Result<PSQLRowStream, Error>, for channel: String, context: ChannelHandlerContext) {
        switch result {
        case .success:
            switch self.listenState.startListeningSucceeded(channel: channel) {
            case .activateListeners(let listeners):
                for list in listeners {
                    list.startListeningSucceeded(handler: self)
                }

            case .stopListening:
                let task = self.makeUnlistenQuery(channel: channel, context: context)
                let action = self.state.enqueue(task: task)
                self.run(action, with: context)
            }

        case .failure(let error):
            let finalError: PSQLError
            if var psqlError = error as? PSQLError {
                psqlError.code = .listenFailed
                finalError = psqlError
            } else {
                var psqlError = PSQLError(code: .listenFailed)
                psqlError.underlying = error
                finalError = psqlError
            }
            let listeners = self.listenState.startListeningFailed(channel: channel, error: finalError)
            for list in listeners {
                list.failed(finalError)
            }
        }
    }

    private func makeUnlistenQuery(channel: String, context: ChannelHandlerContext) -> PSQLTask {
        let promise = context.eventLoop.makePromise(of: PSQLRowStream.self)
        let query = ExtendedQueryContext(
            query: PostgresQuery(unsafeSQL: #"UNLISTEN "\#(channel)";"#),
            logger: self.logger,
            promise: promise
        )
        let loopBound = NIOLoopBound((self, context), eventLoop: self.eventLoop)
        promise.futureResult.whenComplete { result in
            let (selfTransferred, context) = loopBound.value
            selfTransferred.stopListenCompleted(result, for: channel, context: context)
        }

        return .extendedQuery(query)
    }

    private func stopListenCompleted(
        _ result: Result<PSQLRowStream, Error>,
        for channel: String,
        context: ChannelHandlerContext
    ) {
        switch result {
        case .success:
            switch self.listenState.stopListeningSucceeded(channel: channel) {
            case .none:
                break

            case .startListening:
                let task = self.makeStartListeningQuery(channel: channel, context: context)
                let action = self.state.enqueue(task: task)
                self.run(action, with: context)
            }

        case .failure(let error):
            let action = self.state.errorHappened(.unlistenError(underlying: error))
            self.run(action, with: context)
        }
    }

    private func forwardNotificationToListeners(
        _ notification: PostgresBackendMessage.NotificationResponse,
        context: ChannelHandlerContext
    ) {
        switch self.listenState.notificationReceived(channel: notification.channel) {
        case .none:
            break

        case .notify(let listeners):
            for listener in listeners {
                listener.notificationReceived(notification)
            }
        }
    }

    private func makePrepareStatementTask(
        preparedStatement: PreparedStatementContext,
        context: ChannelHandlerContext
    ) -> PSQLTask {
        let promise = self.eventLoop.makePromise(of: RowDescription?.self)
        let loopBound = NIOLoopBound((self, context), eventLoop: self.eventLoop)
        promise.futureResult.whenComplete { result in
            let (selfTransferred, context) = loopBound.value
            switch result {
            case .success(let rowDescription):
                selfTransferred.prepareStatementComplete(
                    name: preparedStatement.name,
                    rowDescription: rowDescription,
                    context: context
                )
            case .failure(let error):
                let psqlError: PSQLError
                if let error = error as? PSQLError {
                    psqlError = error
                } else {
                    psqlError = .connectionError(underlying: error)
                }
                selfTransferred.prepareStatementFailed(
                    name: preparedStatement.name,
                    error: psqlError,
                    context: context
                )
            }
        }
        return .extendedQuery(.init(
            name: preparedStatement.name,
            query: preparedStatement.sql,
            bindingDataTypes: preparedStatement.bindingDataTypes,
            logger: preparedStatement.logger,
            promise: promise
        ))
    }

    private func makeExecutePreparedStatementTask(
        preparedStatement: PreparedStatementContext,
        rowDescription: RowDescription?
    ) -> PSQLTask {
        return .extendedQuery(.init(
            executeStatement: .init(
                name: preparedStatement.name,
                binds: preparedStatement.bindings,
                rowDescription: rowDescription
            ),
            logger: preparedStatement.logger,
            promise: preparedStatement.promise
        ))
    }

    private func prepareStatementComplete(
        name: String,
        rowDescription: RowDescription?,
        context: ChannelHandlerContext
    ) {
        let action = self.preparedStatementState.preparationComplete(
            name: name,
            rowDescription: rowDescription
        )
        for preparedStatement in action.statements {
            let action = self.state.enqueue(task: .extendedQuery(.init(
                executeStatement: .init(
                    name: preparedStatement.name,
                    binds: preparedStatement.bindings,
                    rowDescription: action.rowDescription
                ),
                logger: preparedStatement.logger,
                promise: preparedStatement.promise
            ))
            )
            self.run(action, with: context)
        }
    }

    private func prepareStatementFailed(
        name: String,
        error: PSQLError,
        context: ChannelHandlerContext
    ) {
        let action = self.preparedStatementState.errorHappened(
            name: name,
            error: error
        )
        for statement in action.statements {
            statement.promise.fail(action.error)
        }
    }
}

extension PostgresChannelHandler: PSQLRowsDataSource {
    func request(for stream: PSQLRowStream) {
        guard self.rowStream === stream, let handlerContext = self.handlerContext else {
            return
        }
        let action = self.state.requestQueryRows()
        self.run(action, with: handlerContext)
    }
    
    func cancel(for stream: PSQLRowStream) {
        guard self.rowStream === stream, let handlerContext = self.handlerContext else {
            return
        }
        let action = self.state.cancelQueryStream()
        self.run(action, with: handlerContext)
    }
}

private extension Insecure.MD5.Digest {
    
    private static let lowercaseLookup: [UInt8] = [
        UInt8(ascii: "0"), UInt8(ascii: "1"), UInt8(ascii: "2"), UInt8(ascii: "3"),
        UInt8(ascii: "4"), UInt8(ascii: "5"), UInt8(ascii: "6"), UInt8(ascii: "7"),
        UInt8(ascii: "8"), UInt8(ascii: "9"), UInt8(ascii: "a"), UInt8(ascii: "b"),
        UInt8(ascii: "c"), UInt8(ascii: "d"), UInt8(ascii: "e"), UInt8(ascii: "f"),
    ]
    
    func asciiHexDigest() -> [UInt8] {
        var result = [UInt8]()
        result.reserveCapacity(2 * Insecure.MD5Digest.byteCount)
        for byte in self {
            result.append(Self.lowercaseLookup[Int(byte >> 4)])
            result.append(Self.lowercaseLookup[Int(byte & 0x0F)])
        }
        return result
    }
    
    func md5PrefixHexdigest() -> String {
        // TODO: The array should be stack allocated in the best case. But we support down to 5.2.
        //       Given that this method is called only on startup of a new connection, this is an
        //       okay tradeoff for now.
        var result = [UInt8]()
        result.reserveCapacity(3 + 2 * Insecure.MD5Digest.byteCount)
        result.append(UInt8(ascii: "m"))
        result.append(UInt8(ascii: "d"))
        result.append(UInt8(ascii: "5"))
        
        for byte in self {
            result.append(Self.lowercaseLookup[Int(byte >> 4)])
            result.append(Self.lowercaseLookup[Int(byte & 0x0F)])
        }
        return String(decoding: result, as: Unicode.UTF8.self)
    }
}

extension ConnectionStateMachine.TLSConfiguration {
    fileprivate init(_ tls: PostgresConnection.Configuration.TLS) {
        switch (tls.isAllowed, tls.isEnforced) {
        case (false, _):
            self = .disable
        case (true, true):
            self = .require
        case (true, false):
            self = .prefer
        }
    }
}
