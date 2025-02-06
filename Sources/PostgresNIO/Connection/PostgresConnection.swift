import Atomics
import NIOCore
import NIOPosix
#if canImport(Network)
import NIOTransportServices
#endif
import NIOSSL
import Logging

/// A Postgres connection. Use it to run queries against a Postgres server.
///
/// Thread safety is achieved by dispatching all access to shared state onto the underlying EventLoop.
public final class PostgresConnection: @unchecked Sendable {
    /// A Postgres connection ID
    public typealias ID = Int

    /// The connection's underlying channel
    ///
    /// This should be private, but it is needed for `PostgresConnection` compatibility.
    internal let channel: Channel

    /// The underlying `EventLoop` of both the connection and its channel.
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }

    public var closeFuture: EventLoopFuture<Void> {
        return self.channel.closeFuture
    }

    /// A logger to use in case
    public var logger: Logger {
        get {
            self._logger
        }
        set {
            // ignore
        }
    }

    private let internalListenID = ManagedAtomic(0)

    public var isClosed: Bool {
        return !self.channel.isActive
    }

    public let id: ID

    private var _logger: Logger

    init(channel: Channel, connectionID: ID, logger: Logger) {
        self.channel = channel
        self.id = connectionID
        self._logger = logger
    }
    deinit {
        assert(self.isClosed, "PostgresConnection deinitialized before being closed.")
    }

    func start(configuration: InternalConfiguration) -> EventLoopFuture<Void> {
        // 1. configure handlers

        let configureSSLCallback: ((Channel, PostgresChannelHandler) throws -> ())?
        
        switch configuration.tls.base {
        case .prefer(let context), .require(let context):
            configureSSLCallback = { channel, postgresChannelHandler in
                channel.eventLoop.assertInEventLoop()

                let sslHandler = try NIOSSLClientHandler(
                    context: context,
                    serverHostname: configuration.serverNameForTLS
                )
                try channel.pipeline.syncOperations.addHandler(sslHandler, position: .before(postgresChannelHandler))
            }
        case .disable:
            configureSSLCallback = nil
        }

        let channelHandler = PostgresChannelHandler(
            configuration: configuration,
            eventLoop: channel.eventLoop,
            logger: logger,
            configureSSLCallback: configureSSLCallback
        )

        let eventHandler = PSQLEventsHandler(logger: logger)

        // 2. add handlers

        do {
            try self.channel.pipeline.syncOperations.addHandler(eventHandler)
            try self.channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }

        // 3. wait for startup future to succeed.

        return eventHandler.authenticateFuture.flatMapError { error in
            // in case of an startup error, the connection must be closed and after that
            // the originating error should be surfaced

            self.channel.closeFuture.flatMapThrowing { _ in
                throw error
            }
        }
    }

    /// Create a new connection to a Postgres server
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the request shall be created on
    ///   - configuration: A ``Configuration`` that shall be used for the connection
    ///   - connectionID: An `Int` id, used for metadata logging
    ///   - logger: A logger to log background events into
    /// - Returns: A SwiftNIO `EventLoopFuture` that will provide a ``PostgresConnection``
    ///            at a later point in time.
    public static func connect(
        on eventLoop: EventLoop,
        configuration: PostgresConnection.Configuration,
        id connectionID: ID,
        logger: Logger
    ) -> EventLoopFuture<PostgresConnection> {
        self.connect(
            connectionID: connectionID,
            configuration: .init(configuration),
            logger: logger,
            on: eventLoop
        )
    }

    static func connect(
        connectionID: ID,
        configuration: PostgresConnection.InternalConfiguration,
        logger: Logger,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {

        var mlogger = logger
        mlogger[postgresMetadataKey: .connectionID] = "\(connectionID)"
        let logger = mlogger

        // Here we dispatch to the `eventLoop` first before we setup the EventLoopFuture chain, to
        // ensure all `flatMap`s are executed on the EventLoop (this means the enqueuing of the
        // callbacks).
        //
        // This saves us a number of context switches between the thread the Connection is created
        // on and the EventLoop. In addition, it eliminates all potential races between the creating
        // thread and the EventLoop.
        return eventLoop.flatSubmit { () -> EventLoopFuture<PostgresConnection> in
            let connectFuture: EventLoopFuture<Channel>

            switch configuration.connection {
            case .resolved(let address):
                let bootstrap = self.makeBootstrap(on: eventLoop, configuration: configuration)
                connectFuture = bootstrap.connect(to: address)
            case .unresolvedTCP(let host, let port):
                let bootstrap = self.makeBootstrap(on: eventLoop, configuration: configuration)
                connectFuture = bootstrap.connect(host: host, port: port)
            case .unresolvedUDS(let path):
                let bootstrap = self.makeBootstrap(on: eventLoop, configuration: configuration)
                connectFuture = bootstrap.connect(unixDomainSocketPath: path)
            case .bootstrapped(let channel):
                guard channel.isActive else {
                    return eventLoop.makeFailedFuture(PostgresError.connectionError(underlying: ChannelError.alreadyClosed))
                }
                connectFuture = eventLoop.makeSucceededFuture(channel)
            }

            return connectFuture.flatMap { channel -> EventLoopFuture<PostgresConnection> in
                let connection = PostgresConnection(channel: channel, connectionID: connectionID, logger: logger)
                return connection.start(configuration: configuration).map { _ in connection }
            }.flatMapErrorThrowing { error -> PostgresConnection in
                switch error {
                case is PostgresError:
                    throw error
                default:
                    throw PostgresError.connectionError(underlying: error)
                }
            }
        }
    }

    static func makeBootstrap(
        on eventLoop: EventLoop,
        configuration: PostgresConnection.InternalConfiguration
    ) -> NIOClientTCPBootstrapProtocol {
        #if canImport(Network)
        if let tsBootstrap = NIOTSConnectionBootstrap(validatingGroup: eventLoop) {
            return tsBootstrap.connectTimeout(configuration.options.connectTimeout)
        }
        #endif

        if let nioBootstrap = ClientBootstrap(validatingGroup: eventLoop) {
            return nioBootstrap.connectTimeout(configuration.options.connectTimeout)
        }

        fatalError("No matching bootstrap found")
    }

    // MARK: Query

    private func queryStream(_ query: PostgresQuery, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"
        guard query.binds.count <= Int(UInt16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PostgresError(code: .tooManyParameters, query: query))
        }

        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: query,
            logger: logger,
            promise: promise
        )

        self.channel.write(HandlerTask.extendedQuery(context), promise: nil)

        return promise.futureResult
    }

    // MARK: Prepared statements

    func prepareStatement(_ query: String, with name: String, logger: Logger) -> EventLoopFuture<PSQLPreparedStatement> {
        let promise = self.channel.eventLoop.makePromise(of: RowDescription?.self)
        let context = ExtendedQueryContext(
            name: name,
            query: query,
            bindingDataTypes: [],
            logger: logger,
            promise: promise
        )

        self.channel.write(HandlerTask.extendedQuery(context), promise: nil)
        return promise.futureResult.map { rowDescription in
            PSQLPreparedStatement(name: name, query: query, connection: self, rowDescription: rowDescription)
        }
    }

    func execute(_ executeStatement: PSQLExecuteStatement, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        guard executeStatement.binds.count <= Int(UInt16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PostgresError(code: .tooManyParameters))
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            executeStatement: executeStatement,
            logger: logger,
            promise: promise)

        self.channel.write(HandlerTask.extendedQuery(context), promise: nil)
        return promise.futureResult
    }

    func close(_ target: CloseTarget, logger: Logger) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let context = CloseCommandContext(target: target, logger: logger, promise: promise)

        self.channel.write(HandlerTask.closeCommand(context), promise: nil)
        return promise.futureResult
    }


    /// Closes the connection to the server.
    ///
    /// - Returns: An EventLoopFuture that is succeeded once the connection is closed.
    public func close() -> EventLoopFuture<Void> {
        guard !self.isClosed else {
            return self.eventLoop.makeSucceededFuture(())
        }

        self.channel.close(mode: .all, promise: nil)
        return self.closeFuture
    }
}

// MARK: Connect

extension PostgresConnection {
    static let idGenerator = ManagedAtomic(0)
}

// MARK: Async/Await Interface

extension PostgresConnection {

    /// Creates a new connection to a Postgres server.
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the connection shall be created on.
    ///   - configuration: A ``Configuration`` that shall be used for the connection
    ///   - connectionID: An `Int` id, used for metadata logging
    ///   - logger: A logger to log background events into
    /// - Returns: An established  ``PostgresConnection`` asynchronously that can be used to run queries.
    public static func connect(
        on eventLoop: EventLoop = PostgresConnection.defaultEventLoopGroup.any(),
        configuration: PostgresConnection.Configuration,
        id connectionID: ID,
        logger: Logger
    ) async throws -> PostgresConnection {
        try await self.connect(
            connectionID: connectionID,
            configuration: .init(configuration),
            logger: logger,
            on: eventLoop
        ).get()
    }

    /// Closes the connection to the server.
    public func close() async throws {
        try await self.close().get()
    }

    /// Closes the connection to the server, _after all queries_ that have been created on this connection have been run.
    public func closeGracefully() async throws {
        try await withTaskCancellationHandler { () async throws -> () in
            let promise = self.eventLoop.makePromise(of: Void.self)
            self.channel.triggerUserOutboundEvent(PSQLOutgoingEvent.gracefulShutdown, promise: promise)
            return try await promise.futureResult.get()
        } onCancel: {
            self.close()
        }
    }

    /// Run a query on the Postgres server the connection is connected to.
    ///
    /// - Parameters:
    ///   - query: The ``PostgresQuery`` to run
    ///   - logger: The `Logger` to log into for the query
    ///   - file: The file, the query was started in. Used for better error reporting.
    ///   - line: The line, the query was started in. Used for better error reporting.
    /// - Returns: A ``PostgresRowSequence`` containing the rows the server sent as the query result.
    ///            The sequence  be discarded.
    @discardableResult
    public func query(
        _ query: PostgresQuery,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> PostgresRowSequence {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"

        guard query.binds.count <= Int(UInt16.max) else {
            throw PostgresError(code: .tooManyParameters, query: query, file: file, line: line)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: query,
            logger: logger,
            promise: promise
        )

        self.channel.write(HandlerTask.extendedQuery(context), promise: nil)

        do {
            return try await promise.futureResult.map({ $0.asyncSequence() }).get()
        } catch var error as PostgresError {
            error.file = file
            error.line = line
            error.query = query
            throw error // rethrow with more metadata
        }
    }

    /// Start listening for a channel
    public func listen(_ channel: String) async throws -> PostgresNotificationSequence {
        let id = self.internalListenID.loadThenWrappingIncrement(ordering: .relaxed)

        return try await withTaskCancellationHandler {
            try Task.checkCancellation()

            return try await withCheckedThrowingContinuation { continuation in
                let listener = NotificationListener(
                    channel: channel,
                    id: id,
                    eventLoop: self.eventLoop,
                    checkedContinuation: continuation
                )

                let task = HandlerTask.startListening(listener)

                self.channel.write(task, promise: nil)
            }
        } onCancel: {
            let task = HandlerTask.cancelListening(channel, id)
            self.channel.write(task, promise: nil)
        }
    }

    /// Execute a prepared statement, taking care of the preparation when necessary
    public func execute<Statement: PostgresPreparedStatement, Row>(
        _ preparedStatement: Statement,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> AsyncThrowingMapSequence<PostgresRowSequence, Row> where Row == Statement.Row {
        let bindings = try preparedStatement.makeBindings()
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let task = HandlerTask.executePreparedStatement(.init(
            name: Statement.name,
            sql: Statement.sql,
            bindings: bindings,
            bindingDataTypes: Statement.bindingDataTypes,
            logger: logger,
            promise: promise
        ))
        self.channel.write(task, promise: nil)
        do {
            return try await promise.futureResult
                .map { $0.asyncSequence() }
                .get()
                .map { try preparedStatement.decodeRow($0) }
        } catch var error as PostgresError {
            error.file = file
            error.line = line
            error.query = .init(
                unsafeSQL: Statement.sql,
                binds: bindings
            )
            throw error // rethrow with more metadata
        }
    }

    /// Execute a prepared statement, taking care of the preparation when necessary
    @_disfavoredOverload
    public func execute<Statement: PostgresPreparedStatement>(
        _ preparedStatement: Statement,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> String where Statement.Row == () {
        let bindings = try preparedStatement.makeBindings()
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let task = HandlerTask.executePreparedStatement(.init(
            name: Statement.name,
            sql: Statement.sql,
            bindings: bindings,
            bindingDataTypes: Statement.bindingDataTypes,
            logger: logger,
            promise: promise
        ))
        self.channel.write(task, promise: nil)
        do {
            return try await promise.futureResult
                .map { $0.commandTag }
                .get()
        } catch var error as PostgresError {
            error.file = file
            error.line = line
            error.query = .init(
                unsafeSQL: Statement.sql,
                binds: bindings
            )
            throw error // rethrow with more metadata
        }
    }
}

enum CloseTarget {
    case preparedStatement(String)
    case portal(String)
}

extension EventLoopFuture {
    func enrichPSQLError(query: PostgresQuery, file: String, line: Int) -> EventLoopFuture<Value> {
        return self.flatMapErrorThrowing { error in
            if var error = error as? PostgresError {
                error.file = file
                error.line = line
                error.query = query
                throw error
            } else {
                throw error
            }
        }
    }
}

extension PostgresConnection {
    /// Returns the default `EventLoopGroup` singleton, automatically selecting the best for the platform.
    ///
    /// This will select the concrete `EventLoopGroup` depending which platform this is running on.
    public static var defaultEventLoopGroup: EventLoopGroup {
#if canImport(Network)
        if #available(OSX 10.14, iOS 12.0, tvOS 12.0, watchOS 6.0, *) {
            return NIOTSEventLoopGroup.singleton
        } else {
            return MultiThreadedEventLoopGroup.singleton
        }
#else
        return MultiThreadedEventLoopGroup.singleton
#endif
    }
}
