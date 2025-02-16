import Logging
import XCTest
import PostgresNIO
#if canImport(Network)
import NIOTransportServices
#endif
import NIOPosix
import NIOCore

final class AsyncPostgresConnectionTests: XCTestCase {
    func test1kRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.query("SELECT version()", logger: .psqlTest)
                var iterator = rows.makeAsyncIterator()
                let firstRow = try await iterator.next()
                XCTAssertEqual(try firstRow?.decode(String.self, context: .default).contains("PostgreSQL"), true)
                let done = try await iterator.next()
                XCTAssertNil(done)
            }
        }
    }

    func test1kRoundTripsSimpleQuery() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.__simpleQuery("SELECT version()", logger: .psqlTest)
                var iterator = rows.makeAsyncIterator()
                let firstRow = try await iterator.next()
                XCTAssertEqual(try firstRow?.decode(String.self, context: .default).contains("PostgreSQL"), true)
                let done = try await iterator.next()
                XCTAssertNil(done)
            }
        }
    }

    func test1kRoundTripsSimpleQueryNoRows() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            for _ in 0..<1_000 {
                let nonExistentOID = 1928394819
                let rows = try await connection.__simpleQuery("SELECT * FROM pg_class WHERE oid = \(nonExistentOID)", logger: .psqlTest)
                var iterator = rows.makeAsyncIterator()
                let first = try await iterator.next()
                XCTAssertEqual(first, nil)
            }
        }
    }

    func testSelect10kRows() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            for try await row in rows {
                let element = try row.decode(Int.self)
                XCTAssertEqual(element, counter + 1)
                counter += 1
            }

            XCTAssertEqual(counter, end)
        }
    }

    func testSelect10kRowsSimpleQuery() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.__simpleQuery("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            for try await row in rows {
                let element = try row.decode(Int.self)
                XCTAssertEqual(element, counter + 1)
                counter += 1
            }

            XCTAssertEqual(counter, end)
        }
    }

    func testSelectActiveConnection() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let query: PostgresQuery = """
            SELECT
                pid
                ,datname
                ,usename
                ,application_name
                ,client_hostname
                ,client_port
                ,backend_start
                ,query_start
                ,query
                ,state
            FROM pg_stat_activity
            WHERE state = 'active';
            """

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.query(query, logger: .psqlTest)
            var counter = 0

            for try await element in rows.decode((Int, String, String, String, String?, Int, Date, Date, String, String).self) {
                XCTAssertEqual(element.1, env("POSTGRES_DB") ?? "test_database")
                XCTAssertEqual(element.2, env("POSTGRES_USER") ?? "test_username")

                XCTAssertEqual(element.8, query.sql)
                XCTAssertEqual(element.9, "active")
                counter += 1
            }

            XCTAssertGreaterThanOrEqual(counter, 1)
        }
    }

    func testAdditionalParametersTakeEffect() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let query: PostgresQuery = """
            SELECT
                current_setting('application_name');
            """

        let applicationName = "postgres-nio-test"
        var options = PostgresConnection.Configuration.Options()
        options.additionalStartupParameters = [
            ("application_name", applicationName)
        ]

        try await withTestConnection(on: eventLoop, options: options) { connection in
            let rows = try await connection.query(query, logger: .psqlTest)
            var counter = 0

            for try await element in rows.decode(String.self) {
                XCTAssertEqual(element, applicationName)
                
                counter += 1
            }

            XCTAssertGreaterThanOrEqual(counter, 1)
        }
    }

    func testSelectTimeoutWhileLongRunningQuery() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000000

        try await withTestConnection(on: eventLoop) { connection -> () in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest)

            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            do {
                for try await element in rows.decode(Int.self, context: .default) {
                    XCTAssertEqual(element, counter + 1)
                    counter += 1
                }
                XCTFail("Expected to get cancelled while reading the query")
            } catch {
                guard let error = error as? PSQLError else { return XCTFail("Unexpected error type") }

                XCTAssertEqual(error.code, .server)
                XCTAssertEqual(error.serverInfo?[.severity], "ERROR")
            }

            XCTAssertFalse(connection.isClosed, "Connection should survive!")

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    XCTAssertEqual(decoded, num)
                }
            }
        }
    }

    func testConnectionSurvives1kQueriesWithATypo() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection -> () in
            for _ in 0..<1000 {
                do {
                    try await connection.query("SELECT generte_series(\(start), \(end));", logger: .psqlTest)
                    XCTFail("Expected to throw from the request")
                } catch {
                    guard let error = error as? PSQLError else { return XCTFail("Unexpected error type: \(error)") }

                    XCTAssertEqual(error.code, .server)
                    XCTAssertEqual(error.serverInfo?[.severity], "ERROR")
                }
            }

            // the connection survived all of this, we can still run normal queries:

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    XCTAssertEqual(decoded, num)
                }
            }
        }
    }

    func testSelect10times10kRows() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            await withThrowingTaskGroup(of: Void.self) { taskGroup in
                for _ in 0..<10 {
                    taskGroup.addTask {
                        try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    }
                }
            }
        }
    }

    func testBindMaximumParameters() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            // Max binds limit is UInt16.max which is 65535 which is 3 * 5 * 17 * 257
            // Max columns limit is 1664, so we will only make 5 * 257 columns which is less
            // Then we will insert 3 * 17 rows
            // In the insertion, there will be a total of 3 * 17 * 5 * 257 == UInt16.max bindings
            // If the test is successful, it means Postgres supports UInt16.max bindings
            let columnsCount = 5 * 257
            let rowsCount = 3 * 17

            let createQuery = PostgresQuery(
                unsafeSQL: """
                CREATE TABLE table1 (
                \((0..<columnsCount).map({ #""int\#($0)" int NOT NULL"# }).joined(separator: ", "))
                );
                """
            )
            try await connection.query(createQuery, logger: .psqlTest)

            var binds = PostgresBindings(capacity: Int(UInt16.max))
            for _ in (0..<rowsCount) {
                for num in (0..<columnsCount) {
                    binds.append(num, context: .default)
                }
            }
            XCTAssertEqual(binds.count, Int(UInt16.max))

            let insertionValues = (0..<rowsCount).map { rowIndex in
                let indices = (0..<columnsCount).map { columnIndex -> String in
                    "$\(rowIndex * columnsCount + columnIndex + 1)"
                }
                return "(\(indices.joined(separator: ", ")))"
            }.joined(separator: ", ")
            let insertionQuery = PostgresQuery(
                unsafeSQL: "INSERT INTO table1 VALUES \(insertionValues)",
                binds: binds
            )
            try await connection.query(insertionQuery, logger: .psqlTest)

            let countQuery = PostgresQuery(unsafeSQL: "SELECT COUNT(*) FROM table1")
            let countRows = try await connection.query(countQuery, logger: .psqlTest)
            var countIterator = countRows.makeAsyncIterator()
            let insertedRowsCount = try await countIterator.next()?.decode(Int.self, context: .default)
            XCTAssertEqual(rowsCount, insertedRowsCount)

            let dropQuery = PostgresQuery(unsafeSQL: "DROP TABLE table1")
            try await connection.query(dropQuery, logger: .psqlTest)
        }
    }

    func testListenAndNotify() async throws {
        let channelNames = [
            "foo",
            "default"
        ]
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        for channelName in channelNames {
            try await self.withTestConnection(on: eventLoop) { connection in
                let stream = try await connection.listen(channelName)
                var iterator = stream.makeAsyncIterator()

                try await self.withTestConnection(on: eventLoop) { other in
                    try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'bar';"#, logger: .psqlTest)

                    try await other.query(#"NOTIFY "\#(unescaped: channelName)", 'foo';"#, logger: .psqlTest)
                }

                let first = try await iterator.next()
                XCTAssertEqual(first?.payload, "bar")

                let second = try await iterator.next()
                XCTAssertEqual(second?.payload, "foo")
            }
        }
    }

    #if canImport(Network)
    func testSelect10kRowsNetworkFramework() async throws {
        let eventLoopGroup = NIOTSEventLoopGroup()
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 1
            for try await row in rows {
                let element = try row.decode(Int.self, context: .default)
                XCTAssertEqual(element, counter)
                counter += 1
            }

            XCTAssertEqual(counter, end + 1)
        }
    }
    #endif

    func testCancelTaskThatIsVeryLongRunningWhichAlsoFailsWhileInStreamingMode() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        // we cancel the query after 400ms.
        // the server times out the query after 1sec.

        try await withTestConnection(on: eventLoop) { connection -> () in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest) // 1000 milliseconds

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    let start = 1
                    let end = 100_000_000

                    let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    var counter = 0
                    do {
                        for try await element in rows.decode(Int.self, context: .default) {
                            XCTAssertEqual(element, counter + 1)
                            counter += 1
                        }
                        XCTFail("Expected to get cancelled while reading the query")
                        XCTAssertEqual(counter, end)
                    } catch let error as CancellationError {
                        XCTAssertGreaterThanOrEqual(counter, 1)
                        // Expected
                        print("\(error)")
                    } catch {
                        XCTFail("Unexpected error: \(error)")
                    }

                    XCTAssertTrue(Task.isCancelled)
                    XCTAssertFalse(connection.isClosed, "Connection should survive!")
                }

                let delay: UInt64 = 400_000_000 // 400 milliseconds
                try await Task.sleep(nanoseconds: delay)

                group.cancelAll()
            }

            try await connection.query("SELECT 1;", logger: .psqlTest)
        }
    }

    func testPreparedStatement() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        struct TestPreparedStatement: PostgresPreparedStatement {
            static let sql = "SELECT pid, datname FROM pg_stat_activity WHERE state = $1"
            typealias Row = (Int, String)

            var state: String

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.state)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode(Row.self)
            }
        }
        let preparedStatement = TestPreparedStatement(state: "active")
        try await withTestConnection(on: eventLoop) { connection in
            var results = try await connection.execute(preparedStatement, logger: .psqlTest)
            var counter = 0

            for try await element in results {
                XCTAssertEqual(element.1, env("POSTGRES_DB") ?? "test_database")
                counter += 1
            }

            XCTAssertGreaterThanOrEqual(counter, 1)

            // Second execution, which reuses the existing prepared statement
            results = try await connection.execute(preparedStatement, logger: .psqlTest)
            for try await element in results {
                XCTAssertEqual(element.1, env("POSTGRES_DB") ?? "test_database")
                counter += 1
            }
        }
    }

    static let preparedStatementTestTable = "AsyncTestPreparedStatementTestTable"
    func testPreparedStatementWithIntegerBinding() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        struct InsertPreparedStatement: PostgresPreparedStatement {
            static let name = "INSERT-AsyncTestPreparedStatementTestTable"

            static let sql = #"INSERT INTO "\#(AsyncPostgresConnectionTests.preparedStatementTestTable)" (uuid) VALUES ($1);"#
            typealias Row = ()

            var uuid: UUID

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.uuid)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                ()
            }
        }

        struct SelectPreparedStatement: PostgresPreparedStatement {
            static let name = "SELECT-AsyncTestPreparedStatementTestTable"

            static let sql = #"SELECT id, uuid FROM "\#(AsyncPostgresConnectionTests.preparedStatementTestTable)" WHERE id <= $1;"#
            typealias Row = (Int, UUID)

            var id: Int

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.id)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode((Int, UUID).self)
            }
        }

        do {
            try await withTestConnection(on: eventLoop) { connection in
                try await connection.query("""
                    CREATE TABLE IF NOT EXISTS "\(unescaped: Self.preparedStatementTestTable)" (
                        id SERIAL PRIMARY KEY,
                        uuid UUID NOT NULL
                    )
                    """,
                    logger: .psqlTest
                )

                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)

                let rows = try await connection.execute(SelectPreparedStatement(id: 3), logger: .psqlTest)
                var counter = 0
                for try await (id, uuid) in rows {
                    Logger.psqlTest.info("Received row", metadata: [
                        "id": "\(id)", "uuid": "\(uuid)"
                    ])
                    counter += 1
                }

                try await connection.query("""
                    DROP TABLE "\(unescaped: Self.preparedStatementTestTable)";
                    """,
                    logger: .psqlTest
                )
            }
        } catch {
            XCTFail("Unexpected error: \(String(describing: error))")
        }
    }

    static let preparedStatementWithOptionalTestTable = "AsyncTestPreparedStatementWithOptionalTestTable"
    func testPreparedStatementWithOptionalBinding() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        struct InsertPreparedStatement: PostgresPreparedStatement {
            static let name = "INSERT-AsyncTestPreparedStatementWithOptionalTestTable"

            static let sql = #"INSERT INTO "\#(AsyncPostgresConnectionTests.preparedStatementWithOptionalTestTable)" (uuid) VALUES ($1);"#
            typealias Row = ()

            var uuid: UUID?

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.uuid)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                ()
            }
        }

        struct SelectPreparedStatement: PostgresPreparedStatement {
            static let name = "SELECT-AsyncTestPreparedStatementWithOptionalTestTable"

            static let sql = #"SELECT id, uuid FROM "\#(AsyncPostgresConnectionTests.preparedStatementWithOptionalTestTable)" WHERE id <= $1;"#
            typealias Row = (Int, UUID?)

            var id: Int

            func makeBindings() -> PostgresBindings {
                var bindings = PostgresBindings()
                bindings.append(self.id)
                return bindings
            }

            func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                try row.decode((Int, UUID?).self)
            }
        }

        do {
            try await withTestConnection(on: eventLoop) { connection in
                try await connection.query("""
                    CREATE TABLE IF NOT EXISTS "\(unescaped: Self.preparedStatementWithOptionalTestTable)" (
                        id SERIAL PRIMARY KEY,
                        uuid UUID
                    )
                    """,
                    logger: .psqlTest
                )

                _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: .init()), logger: .psqlTest)
                _ = try await connection.execute(InsertPreparedStatement(uuid: nil), logger: .psqlTest)

                let rows = try await connection.execute(SelectPreparedStatement(id: 3), logger: .psqlTest)
                var counter = 0
                for try await (id, uuid) in rows {
                    Logger.psqlTest.info("Received row", metadata: [
                        "id": "\(id)", "uuid": "\(String(describing: uuid))"
                    ])
                    counter += 1
                }

                try await connection.query("""
                    DROP TABLE "\(unescaped: Self.preparedStatementWithOptionalTestTable)";
                    """,
                    logger: .psqlTest
                )
            }
        } catch {
            XCTFail("Unexpected error: \(String(describing: error))")
        }
    }
}

extension XCTestCase {

    func withTestConnection<Result>(
        on eventLoop: EventLoop,
        options: PostgresConnection.Configuration.Options? = nil,
        file: StaticString = #filePath,
        line: UInt = #line,
        _ closure: (PostgresConnection) async throws -> Result
    ) async throws -> Result  {
        let connection = try await PostgresConnection.test(on: eventLoop, options: options).get()

        do {
            let result = try await closure(connection)
            try await connection.close()
            return result
        } catch {
            XCTFail("Unexpected error: \(String(reflecting: error))", file: file, line: line)
            try await connection.close()
            throw error
        }
    }
}
