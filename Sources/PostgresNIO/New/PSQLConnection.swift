import NIOCore
import NIOPosix
import NIOFoundationCompat
import NIOSSL
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder
import struct Foundation.UUID
import Logging

public final class PSQLConnection {
    
    public struct Configuration {
        public struct Authentication {
            public var username: String
            public var database: String? = nil
            public var password: String? = nil
            
            public init() {
                self.username = "postgres"
                self.database = nil
                self.password = nil
            }
        }
        
        public enum Connection {
            case unresolved(host: String, port: Int)
            case resolved(address: SocketAddress, serverName: String?)
        }
        
        public var connection: Connection
        
        /// The authentication properties to send to the Postgres server during startup auth handshake
        public var authentication: Authentication?
        
        public var tlsConfiguration: TLSConfiguration?
        
        public init(
            host: String,
            port: Int = 5432,
            username: String,
            database: String? = nil,
            password: String? = nil,
            tlsConfiguration: TLSConfiguration? = nil
        ) {
            self.connection = .unresolved(host: host, port: port)
            self.authentication = .init()
            self.authentication!.username = username
            self.authentication!.database = database
            self.authentication!.password = password
            self.tlsConfiguration = tlsConfiguration
        }
        
        init(connection: Connection,
             authentication: Authentication?,
             tlsConfiguration: TLSConfiguration?
        ) {
            self.connection = connection
            self.authentication = authentication
            self.tlsConfiguration = tlsConfiguration
        }
    }
    
    /// The connection's underlying channel
    ///
    /// This should be private, but it is needed for `PostgresConnection` compatibility.
    internal let channel: Channel

    /// The underlying `EventLoop` of both the connection and its channel.
    var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    var closeFuture: EventLoopFuture<Void> {
        return self.channel.closeFuture
    }
    
    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    /// A logger to use in case
    private var logger: Logger
    let connectionID: String

    init(channel: Channel, connectionID: String, logger: Logger) {
        self.channel = channel
        self.connectionID = connectionID
        self.logger = logger
    }
    deinit {
        assert(self.isClosed, "PostgresConnection deinitialized before being closed.")
    }
    
    func close() -> EventLoopFuture<Void> {
        guard !self.isClosed else {
            return self.eventLoop.makeSucceededFuture(())
        }
        
        self.channel.close(mode: .all, promise: nil)
        return self.closeFuture
    }
    
    // MARK: Query
            
    func query(_ query: String, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        self.query(query, [], logger: logger)
    }
    
    func query(_ query: String, _ bind: [PSQLEncodable], logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.connectionID)"
        guard bind.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError(.tooManyParameters))
        }

        var psqlQuery = PostgresQuery(unsafeSQL: query, binds: .init())
        do {
            try bind.forEach {
                try psqlQuery.binds._append($0, context: .default)
            }
        } catch {
            return self.channel.eventLoop.makeFailedFuture(error)
        }

        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: psqlQuery,
            logger: logger,
            promise: promise)
        
        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)
        
        return promise.futureResult
    }
    
    // MARK: Prepared statements
    
    func prepareStatement(_ query: String, with name: String, logger: Logger) -> EventLoopFuture<PSQLPreparedStatement> {
        let promise = self.channel.eventLoop.makePromise(of: RowDescription?.self)
        let context = PrepareStatementContext(
            name: name,
            query: query,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.preparedStatement(context), promise: nil)
        return promise.futureResult.map { rowDescription in
            PSQLPreparedStatement(name: name, query: query, connection: self, rowDescription: rowDescription)
        }
    }
    
    func execute(_ executeStatement: PSQLExecuteStatement, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        guard executeStatement.binds.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError(.tooManyParameters))
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            executeStatement: executeStatement,
            logger: logger,
            promise: promise)
        
        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)
        return promise.futureResult
    }
    
    func close(_ target: CloseTarget, logger: Logger) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let context = CloseCommandContext(target: target, logger: logger, promise: promise)
        
        self.channel.write(PSQLTask.closeCommand(context), promise: nil)
        return promise.futureResult
    }
    
    static func connect(
        configuration: PSQLConnection.Configuration,
        logger: Logger,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PSQLConnection> {
        
        let connectionID = UUID().uuidString
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(connectionID)"
        
        // Here we dispatch to the `eventLoop` first before we setup the EventLoopFuture chain, to
        // ensure all `flatMap`s are executed on the EventLoop (this means the enqueuing of the
        // callbacks).
        //
        // This saves us a number of context switches between the thread the Connection is created
        // on and the EventLoop. In addition, it eliminates all potential races between the creating
        // thread and the EventLoop.
        return eventLoop.flatSubmit {
            eventLoop.submit { () throws -> SocketAddress in
                switch configuration.connection {
                case .resolved(let address, _):
                    return address
                case .unresolved(let host, let port):
                    return try SocketAddress.makeAddressResolvingHost(host, port: port)
                }
            }.flatMap { address -> EventLoopFuture<Channel> in
                let bootstrap = ClientBootstrap(group: eventLoop)
                    .channelInitializer { channel in
                        var configureSSLCallback: ((Channel) throws -> ())? = nil
                        if let tlsConfiguration = configuration.tlsConfiguration {
                            configureSSLCallback = { channel in
                                channel.eventLoop.assertInEventLoop()
                                
                                let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
                                let sslHandler = try NIOSSLClientHandler(
                                    context: sslContext,
                                    serverHostname: configuration.sslServerHostname)
                                try channel.pipeline.syncOperations.addHandler(sslHandler, position: .first)
                            }
                        }
                        
                        return channel.pipeline.addHandlers([
                            PSQLChannelHandler(
                                configuration: configuration,
                                logger: logger,
                                configureSSLCallback: configureSSLCallback),
                            PSQLEventsHandler(logger: logger)
                        ])
                    }
                
                return bootstrap.connect(to: address)
            }.flatMap { channel -> EventLoopFuture<Channel> in
                channel.pipeline.handler(type: PSQLEventsHandler.self).flatMap {
                    eventHandler -> EventLoopFuture<Void> in
                    
                    let startupFuture: EventLoopFuture<Void>
                    if configuration.authentication == nil {
                        startupFuture = eventHandler.readyForStartupFuture
                    } else {
                        startupFuture = eventHandler.authenticateFuture
                    }
                    
                    return startupFuture.flatMapError { error in
                        // in case of an startup error, the connection must be closed and after that
                        // the originating error should be surfaced
                        
                        channel.closeFuture.flatMapThrowing { _ in
                            throw error
                        }
                    }
                }.map { _ in channel }
            }.map { channel in
                PSQLConnection(channel: channel, connectionID: connectionID, logger: logger)
            }.flatMapErrorThrowing { error -> PSQLConnection in
                switch error {
                case is PSQLError:
                    throw error
                default:
                    throw PSQLError(.channel, underlying: error)
                }
            }
        }
    }
}

enum CloseTarget {
    case preparedStatement(String)
    case portal(String)
}

extension PSQLConnection.Configuration {
    var sslServerHostname: String? {
        switch self.connection {
        case .unresolved(let host, _):
            guard !host.isIPAddress() else {
                return nil
            }
            return host
        case .resolved(_, let serverName):
            return serverName
        }
    }
}

// copy and pasted from NIOSSL:
private extension String {
    func isIPAddress() -> Bool {
        // We need some scratch space to let inet_pton write into.
        var ipv4Addr = in_addr()
        var ipv6Addr = in6_addr()

        return self.withCString { ptr in
            return inet_pton(AF_INET, ptr, &ipv4Addr) == 1 ||
                   inet_pton(AF_INET6, ptr, &ipv6Addr) == 1
        }
    }
}
