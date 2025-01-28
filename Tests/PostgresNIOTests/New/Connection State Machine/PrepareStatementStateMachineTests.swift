import XCTest
import NIOEmbedded
@testable import PostgresNIO

class PrepareStatementStateMachineTests: XCTestCase {
    func testCreatePreparedStatementReturningRowDescription() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"SELECT id FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        XCTAssertEqual(state.enqueue(task: .extendedQuery(prepareStatementContext, writePromise: nil)),
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: [], promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let columns: [RowDescription.Column] = [
            .init(name: "id", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: -1, format: .binary)
        ]
        
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: columns)),
                       .succeedPreparedStatementCreation(promise, with: .init(columns: columns)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testCreatePreparedStatementReturningNoData() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"DELETE FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        XCTAssertEqual(state.enqueue(task: .extendedQuery(prepareStatementContext, writePromise: nil)),
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: [], promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        XCTAssertEqual(state.noDataReceived(),
                       .succeedPreparedStatementCreation(promise, with: nil))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testErrorReceivedAfter() {
        var state = ConnectionStateMachine.readyForQuery()

        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.

        let name = "haha"
        let query = #"DELETE FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        XCTAssertEqual(state.enqueue(task: .extendedQuery(prepareStatementContext, writePromise: nil)),
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: [], promise: nil))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)

        XCTAssertEqual(state.noDataReceived(),
                       .succeedPreparedStatementCreation(promise, with: nil))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)

        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.authentication(.ok)), closePromise: nil)))
    }
}
