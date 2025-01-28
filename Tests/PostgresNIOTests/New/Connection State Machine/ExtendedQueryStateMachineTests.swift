import XCTest
import NIOCore
import NIOEmbedded
import Logging
@testable import PostgresNIO

class ExtendedQueryStateMachineTests: XCTestCase {
    
    func testExtendedQueryWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "DELETE FROM table WHERE id=\(1)"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        XCTAssertEqual(state.noDataReceived(), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .wait)
        XCTAssertEqual(state.commandCompletedReceived("DELETE 1"), .succeedQuery(promise, with: .init(value: .noRows(.tag("DELETE 1")), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testExtendedQueryWithDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }
        
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let row1: DataRow = [ByteBuffer(string: "test1")]
        XCTAssertEqual(state.dataRowReceived(row1), .wait)
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

    func testExtendedQueryWithNoQuery() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "-- some comments"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        XCTAssertEqual(state.noDataReceived(), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .wait)
        XCTAssertEqual(state.emptyQueryResponseReceived(), .succeedQuery(promise, with: .init(value: .noRows(.emptyResponse), logger: logger)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testReceiveTotallyUnexpectedMessageInQuery() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "DELETE FROM table WHERE id=\(1)"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let psqlError = PSQLError.unexpectedBackendMessage(.authentication(.ok))
        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .failQuery(promise, with: psqlError, cleanupContext: .init(action: .close, tasks: [], error: psqlError, closePromise: nil)))
    }

    func testExtendedQueryIsCancelledImmediatly() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        XCTAssertEqual(state.cancelQueryStream(), .forwardStreamError(.queryCancelled, read: false, cleanupContext: nil))
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

    func testExtendedQueryIsCancelledWithReadPending() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let row1: DataRow = [ByteBuffer(string: "test1")]
        XCTAssertEqual(state.dataRowReceived(row1), .wait)
        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row1]))
        XCTAssertEqual(state.readEventCaught(), .wait)
        XCTAssertEqual(state.cancelQueryStream(), .forwardStreamError(.queryCancelled, read: true, cleanupContext: nil))

        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test2")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test3")]), .wait)
        XCTAssertEqual(state.dataRowReceived([ByteBuffer(string: "test4")]), .wait)
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)

        XCTAssertEqual(state.commandCompletedReceived("SELECT 4"), .wait)
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

    func testCancelQueryAfterServerError() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let dataRows1: [DataRow] = [
            [ByteBuffer(string: "test1")],
            [ByteBuffer(string: "test2")],
            [ByteBuffer(string: "test3")]
        ]
        for row in dataRows1 {
            XCTAssertEqual(state.dataRowReceived(row), .wait)
        }
        XCTAssertEqual(state.channelReadComplete(), .forwardRows(dataRows1))
        XCTAssertEqual(state.readEventCaught(), .wait)
        XCTAssertEqual(state.requestQueryRows(), .read)
        let dataRows2: [DataRow] = [
            [ByteBuffer(string: "test4")],
            [ByteBuffer(string: "test5")],
            [ByteBuffer(string: "test6")]
        ]
        for row in dataRows2 {
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
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)

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
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext, writePromise: nil)), .sendParseDescribeBindExecuteSync(query, promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        XCTAssertEqual(state.cancelQueryStream(), .failQuery(promise, with: .queryCancelled, cleanupContext: .none))

        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        XCTAssertEqual(state.errorReceived(serverError), .wait)

        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }

}
