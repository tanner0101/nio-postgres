import NIO
import NIOFoundationCompat
import NIOSSL
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder
import struct Foundation.UUID
import Logging

@usableFromInline
final class PSQLConnection {
    
    struct Configuration {
        
        struct Coders {
            var jsonEncoder: PSQLJSONEncoder
            var jsonDecoder: PSQLJSONDecoder
            
            init(jsonEncoder: PSQLJSONEncoder, jsonDecoder: PSQLJSONDecoder) {
                self.jsonEncoder = jsonEncoder
                self.jsonDecoder = jsonDecoder
            }
            
            static var foundation: Coders {
                Coders(jsonEncoder: JSONEncoder(), jsonDecoder: JSONDecoder())
            }
        }
        
        struct Authentication {
            var username: String
            var database: String? = nil
            var password: String? = nil
            
            init(username: String, password: String?, database: String?) {
                self.username = username
                self.database = database
                self.password = password
            }
        }
        
        enum Connection {
            case unresolved(host: String, port: Int)
            case resolved(address: SocketAddress, serverName: String?)
        }
        
        var connection: Connection
        
        /// The authentication properties to send to the Postgres server during startup auth handshake
        var authentication: Authentication?
        
        var tlsConfiguration: TLSConfiguration?
        var coders: Coders
        
        init(host: String,
             port: Int = 5432,
             username: String,
             database: String? = nil,
             password: String? = nil,
             tlsConfiguration: TLSConfiguration? = nil,
             coders: Coders = .foundation)
        {
            self.connection = .unresolved(host: host, port: port)
            self.authentication = Authentication(username: username, password: password, database: database)
            self.tlsConfiguration = tlsConfiguration
            self.coders = coders
        }
        
        init(connection: Connection,
             authentication: Authentication?,
             tlsConfiguration: TLSConfiguration?,
             coders: Coders = .foundation)
        {
            self.connection = connection
            self.authentication = authentication
            self.tlsConfiguration = tlsConfiguration
            self.coders = coders
        }
    }
    
    /// The connections underlying channel
    ///
    /// This should be private, but it is needed for `PostgresConnection` compatibility.
    internal let channel: Channel

    /// The connections and its underlying `Channel`'s `EventLoop`.
    var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    var closeFuture: EventLoopFuture<Void> {
        return self.channel.closeFuture
    }
    
    var isClosed: Bool {
        return !self.channel.isActive
    }
    
    /// A logger to use in case
    private var logger: Logger
    let connectionID: String
    let jsonDecoder: PSQLJSONDecoder

    init(channel: Channel, connectionID: String, logger: Logger, jsonDecoder: PSQLJSONDecoder) {
        self.channel = channel
        self.connectionID = connectionID
        self.logger = logger
        self.jsonDecoder = jsonDecoder
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
            
    func query(_ query: String, logger: Logger) -> EventLoopFuture<PSQLRows> {
        self.query(query, [], logger: logger)
    }
    
    func query(_ query: String, _ bind: [PSQLEncodable], logger: Logger) -> EventLoopFuture<PSQLRows> {
        guard bind.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError.tooManyParameters)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRows.self)
        let context = ExecuteExtendedQueryContext(
            query: query,
            bind: bind,
            logger: logger,
            jsonDecoder: self.jsonDecoder,
            promise: promise)
        
        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)
        return promise.futureResult
    }
    
    // MARK: Prepared statements
    
    func prepareStatement(_ query: String, with name: String, logger: Logger) -> EventLoopFuture<PSQLPreparedStatement> {
        let promise = self.channel.eventLoop.makePromise(of: PSQLBackendMessage.RowDescription?.self)
        let context = CreatePreparedStatementContext(
            name: name,
            query: query,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.preparedStatement(context), promise: nil)
        return promise.futureResult.map { rowDescription in
            PSQLPreparedStatement(name: name, query: query, connection: self, rowDescription: rowDescription)
        }
    }
    
    func execute(_ preparedStatement: PSQLPreparedStatement,
                 _ bind: [PSQLEncodable], logger: Logger) -> EventLoopFuture<PSQLRows>
    {
        guard bind.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError.tooManyParameters)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRows.self)
        let context = ExecuteExtendedQueryContext(
            preparedStatement: preparedStatement,
            bind: bind,
            logger: logger,
            jsonDecoder: self.jsonDecoder,
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
        
        let connectionID = "1"
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(connectionID)"
        
        return eventLoop.makeSucceededFuture(Void()).flatMapThrowing { _ -> SocketAddress in
            switch configuration.connection {
            case .resolved(let address, _):
                return address
            case .unresolved(let host, let port):
                return try SocketAddress.makeAddressResolvingHost(host, port: port)
            }
        }.flatMap { address in
            let bootstrap = ClientBootstrap(group: eventLoop)
                .channelInitializer { channel in
                    let decoder = ByteToMessageHandler(PSQLBackendMessage.Decoder())
                    
                    var enableSSLCallback: ((Channel) -> EventLoopFuture<Void>)? = nil
                    if let tlsConfiguration = configuration.tlsConfiguration {
                        enableSSLCallback = { channel in
                            channel.eventLoop.submit {
                                let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
                                return try NIOSSLClientHandler(
                                    context: sslContext,
                                    serverHostname: configuration.sslServerHostname)
                            }.flatMap { sslHandler in
                                channel.pipeline.addHandler(sslHandler, position: .before(decoder))
                            }
                        }
                    }
                    
                    return channel.pipeline.addHandlers([
                        decoder,
                        MessageToByteHandler(PSQLFrontendMessage.Encoder(jsonEncoder: configuration.coders.jsonEncoder)),
                        PSQLChannelHandler(
                            authentification: configuration.authentication,
                            logger: logger,
                            enableSSLCallback: enableSSLCallback),
                        PSQLEventsHandler(logger: logger, eventLoop: channel.eventLoop)
                    ])
                }
            return bootstrap.connect(to: address)
        }.map { channel in
            PSQLConnection(channel: channel, connectionID: connectionID, logger: logger, jsonDecoder: configuration.coders.jsonDecoder)
        }.flatMap { connection -> EventLoopFuture<PSQLConnection> in
            return connection.channel.pipeline.handler(type: PSQLEventsHandler.self).flatMap {
                handler -> EventLoopFuture<PSQLConnection> in
                
                let startupFuture: EventLoopFuture<Void>
                
                if configuration.authentication == nil {
                    startupFuture = handler.readyForStartupFuture
                } else {
                    startupFuture = handler.authenticateFuture
                }
                
                return startupFuture.map { connection }.flatMapError { error in
                    // in case of an startup error, the connection must be closed and after that
                    // the originating error should be surfaced
                    connection.close().map { connection }.flatMapThrowing { _ in
                        throw error
                    }
                }
            }
        }.flatMapErrorThrowing { error -> PSQLConnection in
            switch error {
            case is PSQLError:
                throw error
            default:
                throw PSQLError.connection(underlying: error)
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
