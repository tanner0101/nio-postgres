import XCTest
import NIOCore
import NIOEmbedded
import Logging
@testable import PostgresNIO

class SimpleQueryStateMachineTests: XCTestCase {

    func testQueryWithSimpleQueryWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "DELETE FROM table WHERE id=1"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))
        XCTAssertEqual(state.commandCompletedReceived("DELETE 1"), .succeedQuery(promise, with: .init(value: .noRows(.tag("DELETE 1")), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryWithSimpleQueryWithRowDescriptionWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let nonExistentOID = 371280378
        let query = "SELECT * FROM pg_class WHERE oid = \(nonExistentOID)"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.commandCompletedReceived("DELETE 1"), .succeedQuery(promise, with: .init(value: .noRows(.tag("DELETE 1")), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryWithSimpleQueryWithDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        let row1: DataRow = [ByteBuffer(string: "test1")]
        let result = QueryResult(value: .rowDescription(input), logger: queryContext.logger)
        XCTAssertEqual(state.dataRowReceived(row1), .succeedQuery(promise, with: result))
        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row1]))
        XCTAssertEqual(state.readEventCaught(), .wait)
        XCTAssertEqual(state.requestQueryRows(), .read)

        let row2: DataRow = [ByteBuffer(string: "test2")]
        let row3: DataRow = [ByteBuffer(string: "test3")]
        let row4: DataRow = [ByteBuffer(string: "test4")]
        XCTAssertEqual(state.dataRowReceived(row2), .wait)
        XCTAssertEqual(state.dataRowReceived(row3), .wait)
        XCTAssertEqual(state.dataRowReceived(row4), .wait)
        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row2, row3, row4]))
        XCTAssertEqual(state.requestQueryRows(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        let row5: DataRow = [ByteBuffer(string: "test5")]
        let row6: DataRow = [ByteBuffer(string: "test6")]
        XCTAssertEqual(state.dataRowReceived(row5), .wait)
        XCTAssertEqual(state.dataRowReceived(row6), .wait)

        XCTAssertEqual(state.commandCompletedReceived("SELECT 2"), .forwardStreamComplete([row5, row6], commandTag: "SELECT 2"))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryWithSimpleQueryWithNoQuery() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "-- some comments"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))
        XCTAssertEqual(state.emptyQueryResponseReceived(), .succeedQuery(promise, with: .init(value: .noRows(.emptyResponse), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testReceiveTotallyUnexpectedMessageInQuery() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let nonExistentOID = 371280378
        let query = "SELECT * FROM pg_class WHERE oid = \(nonExistentOID)"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)

        let psqlError = PSQLError.unexpectedBackendMessage(.authentication(.ok))
        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .failQuery(promise, with: psqlError, cleanupContext: .init(action: .close, tasks: [], error: psqlError, closePromise: nil)))
    }

    func testQueryIsCancelledImmediately() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        XCTAssertEqual(state.cancelQueryStream(), .failQuery(promise, with: .queryCancelled, cleanupContext: nil))

        // The query was cancelled but it also ended anyways, so we accept that the query has succeeded
        XCTAssertEqual(state.commandCompletedReceived("SELECT 2"), .succeedQuery(promise, with: .init(value: .noRows(.tag("SELECT 2")), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryIsCancelledWithReadPending() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.cancelQueryStream(), .failQuery(promise, with: .queryCancelled, cleanupContext: nil))
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test1")]), .wait)
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test2")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test3")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test4")]), .wait)
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.commandCompletedReceived("SELECT 2"), .wait)
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryIsCancelledWithReadPendingWhileStreaming() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        let row1: DataRow = [ByteBuffer(string: "test1")]
        let result = QueryResult(value: .rowDescription(input), logger: queryContext.logger)
        XCTAssertEqual(state.dataRowReceived(row1), .succeedQuery(promise, with: result))
        XCTAssertEqual(state.cancelQueryStream(), .forwardStreamError(.queryCancelled, read: false, cleanupContext: nil))
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test2")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test3")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test4")]), .wait)
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.commandCompletedReceived("SELECT 2"), .wait)
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }


    func testCancelQueryAfterServerError() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        let result = QueryResult(value: .rowDescription(input), logger: queryContext.logger)
        let row1: DataRow = [ByteBuffer(string: "test1")]
        XCTAssertEqual(state.dataRowReceived(row1), .succeedQuery(promise, with: result))

        let dataRows2: [DataRow] = [
            [ByteBuffer(string: "test2")],
            [ByteBuffer(string: "test3")],
            [ByteBuffer(string: "test4")]
        ]
        for row in dataRows2 {
            XCTAssertEqual(state.dataRowReceived(row), .wait)
        }

        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row1] + dataRows2))
        XCTAssertEqual(state.readEventCaught(), .wait)
        XCTAssertEqual(state.requestQueryRows(), .read)
        let dataRows3: [DataRow] = [
            [ByteBuffer(string: "test5")],
            [ByteBuffer(string: "test6")],
            [ByteBuffer(string: "test7")]
        ]
        for row in dataRows3 {
            XCTAssertEqual(state.dataRowReceived(row), .wait)
        }
        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        XCTAssertEqual(state.errorReceived(serverError), .forwardStreamError(.server(serverError), read: false, cleanupContext: .none))

        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryErrorDoesNotKillConnection() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))

        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        XCTAssertEqual(
            state.errorReceived(serverError), .failQuery(promise, with: .server(serverError), cleanupContext: .none)
        )

        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testQueryErrorAfterCancelDoesNotKillConnection() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = SimpleQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .simpleQuery(queryContext)), .sendQuery(query))
        XCTAssertEqual(state.cancelQueryStream(), .failQuery(promise, with: .queryCancelled, cleanupContext: .none))

        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        XCTAssertEqual(state.errorReceived(serverError), .wait)

        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
}
