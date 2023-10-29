import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOSSL

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ConnectionFactory: Sendable {

    struct ConfigCache: Sendable {
        var config: PostgresClient.Configuration
    }

    let configBox: NIOLockedValueBox<ConfigCache>

    struct SSLContextCache: Sendable {
        enum State {
            case none
            case producing(TLSConfiguration, [CheckedContinuation<NIOSSLContext, any Error>])
            case cached(TLSConfiguration, NIOSSLContext)
            case failed(TLSConfiguration, any Error)
        }

        var state: State = .none
    }

    let sslContextBox = NIOLockedValueBox(SSLContextCache())

    let eventLoopGroup: any EventLoopGroup

    let logger: Logger

    init(config: PostgresClient.Configuration, eventLoopGroup: any EventLoopGroup, logger: Logger) {
        self.eventLoopGroup = eventLoopGroup
        self.configBox = NIOLockedValueBox(ConfigCache(config: config))
        self.logger = logger
    }

    func makeConnection(_ connectionID: PostgresConnection.ID, pool: PostgresClient.Pool) async throws -> PostgresConnection {
        let config = try await self.makeConnectionConfig()

        var connectionLogger = self.logger
        connectionLogger[postgresMetadataKey: .connectionID] = "\(connectionID)"

        return try await PostgresConnection.connect(
            on: self.eventLoopGroup.any(),
            configuration: config,
            id: connectionID,
            logger: connectionLogger
        ).get()
    }

    func makeConnectionConfig() async throws -> PostgresConnection.Configuration {
        let config = self.configBox.withLockedValue { $0.config }

        let tls: PostgresConnection.Configuration.TLS
        switch config.tls.base {
        case .prefer(let tlsConfiguration):
            let sslContext = try await self.getSSLContext(for: tlsConfiguration)
            tls = .prefer(sslContext)

        case .require(let tlsConfiguration):
            let sslContext = try await self.getSSLContext(for: tlsConfiguration)
            tls = .require(sslContext)
        case .disable:
            tls = .disable
        }

        var connectionConfig: PostgresConnection.Configuration
        switch config.endpointInfo {
        case .bindUnixDomainSocket(let path):
            connectionConfig = PostgresConnection.Configuration(
                unixSocketPath: path,
                username: config.username,
                password: config.password,
                database: config.database
            )

        case .connectTCP(let host, let port):
            connectionConfig = PostgresConnection.Configuration(
                host: host,
                port: port,
                username: config.username,
                password: config.password,
                database: config.database,
                tls: tls
            )
        }

        connectionConfig.options.connectTimeout = TimeAmount(config.options.connectTimeout)
        connectionConfig.options.tlsServerName = config.options.tlsServerName
        connectionConfig.options.requireBackendKeyData = config.options.requireBackendKeyData

        return connectionConfig
    }

    private func getSSLContext(for tlsConfiguration: TLSConfiguration) async throws -> NIOSSLContext {
        enum Action {
            case produce
            case succeed(NIOSSLContext)
            case fail(any Error)
            case wait
        }

        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<NIOSSLContext, any Error>) in
            let action = self.sslContextBox.withLockedValue { cache -> Action in
                switch cache.state {
                case .none:
                    cache.state = .producing(tlsConfiguration, [continuation])
                    return .produce

                case .cached(let cachedTLSConfiguration, let context):
                    if cachedTLSConfiguration.bestEffortEquals(tlsConfiguration) {
                        return .succeed(context)
                    } else {
                        cache.state = .producing(tlsConfiguration, [continuation])
                        return .produce
                    }

                case .failed(let cachedTLSConfiguration, let error):
                    if cachedTLSConfiguration.bestEffortEquals(tlsConfiguration) {
                        return .fail(error)
                    } else {
                        cache.state = .producing(tlsConfiguration, [continuation])
                        return .produce
                    }

                case .producing(let cachedTLSConfiguration, var continuations):
                    continuations.append(continuation)
                    if cachedTLSConfiguration.bestEffortEquals(tlsConfiguration) {
                        cache.state = .producing(cachedTLSConfiguration, continuations)
                        return .wait
                    } else {
                        cache.state = .producing(tlsConfiguration, continuations)
                        return .produce
                    }
                }
            }

            switch action {
            case .wait:
                break

            case .produce:
                self.reportProduceSSLContextResult(
                    Result(catching: {try NIOSSLContext(configuration: tlsConfiguration)}),
                    for: tlsConfiguration
                )

            case .succeed(let context):
                continuation.resume(returning: context)

            case .fail(let error):
                continuation.resume(throwing: error)
            }
        }
    }

    private func reportProduceSSLContextResult(_ result: Result<NIOSSLContext, any Error>, for tlsConfiguration: TLSConfiguration) {
        enum Action {
            case fail(any Error, [CheckedContinuation<NIOSSLContext, any Error>])
            case succeed(NIOSSLContext, [CheckedContinuation<NIOSSLContext, any Error>])
            case none
        }

        let action = self.sslContextBox.withLockedValue { cache -> Action in
            switch cache.state {
            case .none:
                preconditionFailure("Invalid state: \(cache.state)")

            case .cached, .failed:
                return .none

            case .producing(let cachedTLSConfiguration, let continuations):
                if cachedTLSConfiguration.bestEffortEquals(tlsConfiguration) {
                    switch result {
                    case .success(let context):
                        cache.state = .cached(cachedTLSConfiguration, context)
                        return .succeed(context, continuations)

                    case .failure(let failure):
                        cache.state = .failed(cachedTLSConfiguration, failure)
                        return .fail(failure, continuations)
                    }
                } else {
                    return .none
                }
            }
        }

        switch action {
        case .none:
            break

        case .succeed(let context, let continuations):
            for continuation in continuations {
                continuation.resume(returning: context)
            }

        case .fail(let error, let continuations):
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
        }
    }
}
