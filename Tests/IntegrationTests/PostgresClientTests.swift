@_spi(ConnectionPool) import PostgresNIO
import XCTest
import NIOPosix
import NIOSSL
import Logging
import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PostgresClientTests: XCTestCase {

    func testGetConnection() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let iterations = 1000

            for _ in 0..<iterations {
                taskGroup.addTask {
                    try await client.withConnection() { connection in
                        _ = try await connection.query("SELECT 1", logger: logger)
                    }
                }
            }

            for _ in 0..<iterations {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
    }

    func testQueryDirectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for i in 0..<10000 {
                taskGroup.addTask {
                    do {
                        try await client.query("SELECT 1", logger: logger)
                        logger.info("Success", metadata: ["run": "\(i)"])
                    } catch {
                        XCTFail("Unexpected error: \(error)")
                    }
                }
            }

            for _ in 0..<10000 {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
    }

    func testQueryTable() async throws {
        let tableName = "test_client_prepared_statement"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)
        do {
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    await client.run()
                }

                try await client.query(
                    """
                    CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                        id SERIAL PRIMARY KEY,
                        uuid UUID NOT NULL
                    );
                    """,
                    logger: logger
                )

                for _ in 0..<1000 {
                    try await client.query(
                        """
                        INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                        """,
                        logger: logger
                    )
                }

                let rows = try await client.query(#"SELECT id, uuid FROM "\#(unescaped: tableName)";"#, logger: logger).decode((Int, UUID).self)
                for try await (id, uuid) in rows {
                    logger.info("id: \(id), uuid: \(uuid.uuidString)")
                }

                struct Example: PostgresPreparedStatement {
                    static let sql = "SELECT id, uuid FROM test_client_prepared_statement WHERE id < $1"
                    typealias Row = (Int, UUID)
                    var id: Int
                    func makeBindings() -> PostgresBindings {
                        var bindings = PostgresBindings()
                        bindings.append(self.id)
                        return bindings
                    }
                    func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                        try row.decode(Row.self)
                    }
                }

                for try await (id, uuid) in try await client.execute(Example(id: 200), logger: logger) {
                    logger.info("id: \(id), uuid: \(uuid.uuidString)")
                }

                try await client.query(
                    """
                    DROP TABLE "\(unescaped: tableName)";
                    """,
                    logger: logger
                )

                taskGroup.cancelAll()
            }
        } catch {
            XCTFail("Unexpected error: \(String(reflecting: error))")
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresClient.Configuration {
    static func makeTestConfiguration() -> PostgresClient.Configuration {
        var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
        tlsConfiguration.certificateVerification = .none
        var clientConfig = PostgresClient.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap({ Int($0) }) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .prefer(tlsConfiguration)
        )
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 12*4
        clientConfig.options.keepAliveBehavior = .init(frequency: .seconds(5))
        clientConfig.options.connectionIdleTimeout = .seconds(15)

        return clientConfig
    }
}
