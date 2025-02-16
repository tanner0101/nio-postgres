import NIOCore
import NIOPosix
import NIOEmbedded
import XCTest
import Logging
@testable import PostgresNIO

class PostgresConnectionTests: XCTestCase {

    let logger = Logger(label: "PostgresConnectionTests")

    func testConnectionFailure() {
        // We start a local server and close it immediately to ensure that the port
        // number we try to connect to is not used by any other process.
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        
        var tempChannel: Channel?
        XCTAssertNoThrow(tempChannel = try ServerBootstrap(group: eventLoopGroup)
                            .bind(to: .init(ipAddress: "127.0.0.1", port: 0)).wait())
        let maybePort = tempChannel?.localAddress?.port
        XCTAssertNoThrow(try tempChannel?.close().wait())
        guard let port = maybePort else {
            return XCTFail("Could not get port number from temp started server")
        }
        
        let config = PostgresConnection.Configuration(
            host: "127.0.0.1", port: port,
            username: "postgres", password: "abc123", database: "postgres",
            tls: .disable
        )
        
        var logger = Logger.psqlTest
        logger.logLevel = .trace
        
        XCTAssertThrowsError(try PostgresConnection.connect(on: eventLoopGroup.next(), configuration: config, id: 1, logger: logger).wait()) {
            XCTAssertTrue($0 is PSQLError)
        }
    }

    func testOptionsAreSentOnTheWire() async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = {
            var config = PostgresConnection.Configuration(
                establishedChannel: channel,
                username: "username",
                password: "postgres",
                database: "database"
            )
            config.options.additionalStartupParameters = [
                ("DateStyle", "ISO, MDY"),
                ("application_name", "postgres-nio-test"),
                ("server_encoding", "UTF8"),
                ("integer_datetimes", "on"),
                ("client_encoding", "UTF8"),
                ("TimeZone", "Etc/UTC"),
                ("is_superuser", "on"),
                ("server_version", "13.1 (Debian 13.1-1.pgdg100+1)"),
                ("session_authorization", "postgres"),
                ("IntervalStyle", "postgres"),
                ("standard_conforming_strings", "on")
            ]
            return config
        }()

        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: .psqlTest)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        XCTAssertEqual(message, .startup(.versionThree(parameters: .init(user: "username", database: "database", options: configuration.options.additionalStartupParameters, replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise
        try await connection.close()
    }

    func testSimpleListen() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                for try await event in events {
                    XCTAssertEqual(event.payload, "wooohooo")
                    break
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, #"UNLISTEN "foo";"#)

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testSimpleListenDoesNotUnlistenIfThereIsAnotherSubscriber() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                for try await event in events {
                    XCTAssertEqual(event.payload, "wooohooo")
                    break
                }
            }

            taskGroup.addTask {
                let events = try await connection.listen("foo")
                var counter = 0
                loop: for try await event in events {
                    defer { counter += 1 }
                    switch counter {
                    case 0:
                        XCTAssertEqual(event.payload, "wooohooo")
                    case 1:
                        XCTAssertEqual(event.payload, "wooohooo2")
                        break loop
                    default:
                        XCTFail("Unexpected message: \(event)")
                    }
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo2")))

            let unlistenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(unlistenMessage.parse.query, #"UNLISTEN "foo";"#)

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("UNLISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testSimpleListenConnectionDrops() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup in
            taskGroup.addTask {
                let events = try await connection.listen("foo")
                var iterator = events.makeAsyncIterator()
                let first = try await iterator.next()
                XCTAssertEqual(first?.payload, "wooohooo")
                do {
                    _ = try await iterator.next()
                    XCTFail("Did not expect to not throw")
                } catch {
                    logger.error("error", metadata: ["error": "\(error)"])
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, #"LISTEN "foo";"#)

            try await channel.writeInbound(PostgresBackendMessage.parseComplete)
            try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
            try await channel.writeInbound(PostgresBackendMessage.noData)
            try await channel.writeInbound(PostgresBackendMessage.bindComplete)
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("LISTEN"))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await channel.writeInbound(PostgresBackendMessage.notification(.init(backendPID: 12, channel: "foo", payload: "wooohooo")))
            struct MyWeirdError: Error {}
            channel.pipeline.fireErrorCaught(MyWeirdError())

            switch await taskGroup.nextResult()! {
            case .success:
                break
            case .failure(let failure):
                XCTFail("Unexpected error: \(failure)")
            }
        }
    }

    func testCloseGracefullyClosesWhenInternalQueueIsEmpty() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    let rows = try await connection.query("SELECT 1;", logger: logger)
                    var iterator = rows.decode(Int.self).makeAsyncIterator()
                    let first = try await iterator.next()
                    XCTAssertEqual(first, 1)
                    let second = try await iterator.next()
                    XCTAssertNil(second)
                }
            }

            for i in 0...1 {
                let listenMessage = try await channel.waitForUnpreparedRequest()
                XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

                if i == 0 {
                    taskGroup.addTask {
                        try await connection.closeGracefully()
                    }
                }

                try await channel.writeInbound(PostgresBackendMessage.parseComplete)
                try await channel.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
                let intDescription = RowDescription.Column(
                    name: "",
                    tableOID: 0,
                    columnAttributeNumber: 0,
                    dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary
                )
                try await channel.writeInbound(PostgresBackendMessage.rowDescription(.init(columns: [intDescription])))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.bindComplete)
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 1 1"))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            }

            let terminate = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
            XCTAssertEqual(terminate, .terminate)
            try await channel.closeFuture.get()
            XCTAssertEqual(channel.isActive, false)

            while let taskResult = await taskGroup.nextResult() {
                switch taskResult {
                case .success:
                    break
                case .failure(let failure):
                    XCTFail("Unexpected error: \(failure)")
                }
            }
        }
    }

    func testCloseClosesImmediatly() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    try await connection.query("SELECT 1;", logger: logger)
                }
            }

            let listenMessage = try await channel.waitForUnpreparedRequest()
            XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

            async let close: () = connection.close()

            try await channel.closeFuture.get()
            XCTAssertEqual(channel.isActive, false)

            try await close

            while let taskResult = await taskGroup.nextResult() {
                switch taskResult {
                case .success:
                    XCTFail("Expected queries to fail")
                case .failure(let failure):
                    guard let error = failure as? PSQLError else {
                        return XCTFail("Unexpected error type: \(failure)")
                    }
                    XCTAssertEqual(error.code, .clientClosedConnection)
                }
            }
        }
    }

    func testCloseImmediatelyWithSimpleQuery() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { [logger] taskGroup async throws -> () in
            for _ in 1...2 {
                taskGroup.addTask {
                    try await connection.__simpleQuery("SELECT 1;", logger: logger)
                }
            }

            let query = try await channel.waitForSimpleQueryRequest()
            XCTAssertEqual(query.query, "SELECT 1;")

            async let close: () = connection.close()

            try await channel.closeFuture.get()
            XCTAssertEqual(channel.isActive, false)

            try await close

            while let taskResult = await taskGroup.nextResult() {
                switch taskResult {
                case .success:
                    XCTFail("Expected queries to fail")
                case .failure(let failure):
                    guard let error = failure as? PSQLError else {
                        return XCTFail("Unexpected error type: \(failure)")
                    }
                    XCTAssertEqual(error.code, .clientClosedConnection)
                }
            }
        }
    }

    func testIfServerJustClosesTheErrorReflectsThat() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        let logger = self.logger

        async let response = try await connection.query("SELECT 1;", logger: logger)

        let listenMessage = try await channel.waitForUnpreparedRequest()
        XCTAssertEqual(listenMessage.parse.query, "SELECT 1;")

        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelInactive() }
        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelUnregistered() }

        do {
            _ = try await response
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }

        // retry on same connection

        do {
            _ = try await connection.query("SELECT 1;", logger: self.logger)
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }
    }

    func testIfServerJustClosesTheErrorReflectsThatInSimpleQuery() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()
        let logger = self.logger

        async let response = try await connection.__simpleQuery("SELECT 1;", logger: logger)

        let query = try await channel.waitForSimpleQueryRequest()
        XCTAssertEqual(query.query, "SELECT 1;")

        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelInactive() }
        try await channel.testingEventLoop.executeInContext { channel.pipeline.fireChannelUnregistered() }

        do {
            _ = try await response
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }

        // retry on same connection

        do {
            _ = try await connection.__simpleQuery("SELECT 1;", logger: self.logger)
            XCTFail("Expected to throw")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.code, .serverClosedConnection)
        }
    }

    struct TestPrepareStatement: PostgresPreparedStatement {
        static let sql = "SELECT datname FROM pg_stat_activity WHERE state = $1"
        typealias Row = String

        var state: String

        func makeBindings() -> PostgresBindings {
            var bindings = PostgresBindings()
            bindings.append(.init(string: self.state))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
            try row.decode(Row.self)
        }
    }

    func testPreparedStatement() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            let preparedRequest = try await channel.waitForPreparedRequest()
            XCTAssertEqual(preparedRequest.bind.preparedStatementName, String(reflecting: TestPrepareStatement.self))
            XCTAssertEqual(preparedRequest.bind.parameters.count, 1)
            XCTAssertEqual(preparedRequest.bind.resultColumnFormats, [.binary])

            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )
        }
    }

    func testWeDontCrashOnUnexpectedChannelEvents() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        enum MyEvent {
            case pleaseDontCrash
        }
        channel.pipeline.fireUserInboundEventTriggered(MyEvent.pleaseDontCrash)
        try await connection.close()
    }

    func testSerialExecutionOfSamePreparedStatement() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Send one first and wait to send the other request until preparation is complete
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            let preparedRequest1 = try await channel.waitForPreparedRequest()
            var buffer = preparedRequest1.bind.parameters[0]!
            let parameter1 = buffer.readString(length: buffer.readableBytes)!
            XCTAssertEqual(parameter1, "active")
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )

            // Now that the statement has been prepared and executed, send another request that will only get executed
            // without preparation
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database", database)
                }
                XCTAssertEqual(rows, 1)
            }

            let preparedRequest2 = try await channel.waitForPreparedRequest()
            buffer = preparedRequest2.bind.parameters[0]!
            let parameter2 = buffer.readString(length: buffer.readableBytes)!
            XCTAssertEqual(parameter2, "idle")
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            // Ensure we received and responded to both the requests
            let parameters = [parameter1, parameter2]
            XCTAssert(parameters.contains("active"))
            XCTAssert(parameters.contains("idle"))
        }
    }

    func testStatementPreparationOnlyHappensOnceWithConcurrentRequests() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Let them race to tests that requests and responses aren't mixed up
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database_active", database)
                }
                XCTAssertEqual(rows, 1)
            }
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                let result = try await connection.execute(preparedStatement, logger: .psqlTest)
                var rows = 0
                for try await database in result {
                    rows += 1
                    XCTAssertEqual("test_database_idle", database)
                }
                XCTAssertEqual(rows, 1)
            }

            // The channel deduplicates prepare requests, we're going to see only one of them
            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))

            try await channel.sendPrepareResponse(
                parameterDescription: .init(dataTypes: [
                    PostgresDataType.text
                ]),
                rowDescription: .init(columns: [
                    .init(
                        name: "datname",
                        tableOID: 12222,
                        columnAttributeNumber: 2,
                        dataType: .name,
                        dataTypeSize: 64,
                        dataTypeModifier: -1,
                        format: .text
                    )
                ])
            )

            // Now both the tasks have their statements prepared.
            // We should see both of their execute requests coming in, the order is nondeterministic
            let preparedRequest1 = try await channel.waitForPreparedRequest()
            var buffer = preparedRequest1.bind.parameters[0]!
            let parameter1 = buffer.readString(length: buffer.readableBytes)!
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database_\(parameter1)"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            let preparedRequest2 = try await channel.waitForPreparedRequest()
            buffer = preparedRequest2.bind.parameters[0]!
            let parameter2 = buffer.readString(length: buffer.readableBytes)!
            try await channel.sendPreparedResponse(
                dataRows: [
                    ["test_database_\(parameter2)"]
                ],
                commandTag: TestPrepareStatement.sql
            )
            // Ensure we received and responded to both the requests
            let parameters = [parameter1, parameter2]
            XCTAssert(parameters.contains("active"))
            XCTAssert(parameters.contains("idle"))
        }
    }

    func testStatementPreparationFailure() async throws {
        let (connection, channel) = try await self.makeTestConnectionWithAsyncTestingChannel()

        try await withThrowingTaskGroup(of: Void.self) { taskGroup async throws -> () in
            // Send the same prepared statement twice, but with different parameters.
            // Send one first and wait to send the other request until preparation is complete
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "active")
                do {
                    _ = try await connection.execute(preparedStatement, logger: .psqlTest)
                    XCTFail("Was supposed to fail")
                } catch {
                    XCTAssert(error is PSQLError)
                }
            }

            let prepareRequest = try await channel.waitForPrepareRequest()
            XCTAssertEqual(prepareRequest.parse.query, "SELECT datname FROM pg_stat_activity WHERE state = $1")
            XCTAssertEqual(prepareRequest.parse.parameters.first, .text)
            guard case .preparedStatement(let name) = prepareRequest.describe else {
                fatalError("Describe should contain a prepared statement")
            }
            XCTAssertEqual(name, String(reflecting: TestPrepareStatement.self))
            
            // Respond with an error taking care to return a SQLSTATE that isn't
            // going to get the connection closed.
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .sqlState : "26000" // invalid_sql_statement_name
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            try await channel.testingEventLoop.executeInContext { channel.read() }


            // Send another requests with the same prepared statement, which should fail straight
            // away without any interaction with the server
            taskGroup.addTask {
                let preparedStatement = TestPrepareStatement(state: "idle")
                do {
                    _ = try await connection.execute(preparedStatement, logger: .psqlTest)
                    XCTFail("Was supposed to fail")
                } catch {
                    XCTAssert(error is PSQLError)
                }
            }
        }
    }

    func makeTestConnectionWithAsyncTestingChannel() async throws -> (PostgresConnection, NIOAsyncTestingChannel) {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: "username",
            password: "postgres",
            database: "database"
        )

        let logger = self.logger
        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: logger)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        XCTAssertEqual(message, .startup(.versionThree(parameters: .init(user: "username", database: "database", options: [], replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise

        self.addTeardownBlock {
            try await connection.close()
        }

        return (connection, channel)
    }
}

extension NIOAsyncTestingChannel {

    func waitForUnpreparedRequest() async throws -> UnpreparedRequest {
        let parse = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let describe = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let bind = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let execute = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .parse(let parse) = parse,
              case .describe(let describe) = describe,
              case .bind(let bind) = bind,
              case .execute(let execute) = execute,
              case .sync = sync
        else {
            fatalError()
        }

        return UnpreparedRequest(parse: parse, describe: describe, bind: bind, execute: execute)
    }

    func waitForSimpleQueryRequest() async throws -> PostgresFrontendMessage.Query {
        let query = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        guard case .query(let query) = query else {
            fatalError()
        }
        return query
    }

    func waitForPrepareRequest() async throws -> PrepareRequest {
        let parse = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let describe = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .parse(let parse) = parse,
              case .describe(let describe) = describe,
              case .sync = sync
        else {
            fatalError("Unexpected message")
        }

        return PrepareRequest(parse: parse, describe: describe)
    }

    func sendPrepareResponse(
        parameterDescription: PostgresBackendMessage.ParameterDescription,
        rowDescription: RowDescription
    ) async throws {
        try await self.writeInbound(PostgresBackendMessage.parseComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.parameterDescription(parameterDescription))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.rowDescription(rowDescription))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        try await self.testingEventLoop.executeInContext { self.read() }
    }

    func waitForPreparedRequest() async throws -> PreparedRequest {
        let bind = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let execute = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        let sync = try await self.waitForOutboundWrite(as: PostgresFrontendMessage.self)

        guard case .bind(let bind) = bind,
              case .execute(let execute) = execute,
              case .sync = sync
        else {
            fatalError()
        }

        return PreparedRequest(bind: bind, execute: execute)
    }

    func sendPreparedResponse(
        dataRows: [DataRow],
        commandTag: String
    ) async throws {
        try await self.writeInbound(PostgresBackendMessage.bindComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
        for dataRow in dataRows {
            try await self.writeInbound(PostgresBackendMessage.dataRow(dataRow))
        }
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.commandComplete(commandTag))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        try await self.testingEventLoop.executeInContext { self.read() }
    }
}

struct UnpreparedRequest {
    var parse: PostgresFrontendMessage.Parse
    var describe: PostgresFrontendMessage.Describe
    var bind: PostgresFrontendMessage.Bind
    var execute: PostgresFrontendMessage.Execute
}

struct PrepareRequest {
    var parse: PostgresFrontendMessage.Parse
    var describe: PostgresFrontendMessage.Describe
}

struct PreparedRequest {
    var bind: PostgresFrontendMessage.Bind
    var execute: PostgresFrontendMessage.Execute
}
