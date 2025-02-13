import XCTest
@testable import PostgresNIO
@testable import NIOCore
import NIOPosix
import NIOSSL

class ConnectionStateMachineTests: XCTestCase {
    
    func testStartup() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        XCTAssertEqual(state.connected(tls: .disable), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.plaintext), .sendPasswordMessage(.cleartext, authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.ok), .wait)
    }
    
    func testSSLStartupSuccess() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        XCTAssertEqual(state.connected(tls: .require), .sendSSLRequest)
        XCTAssertEqual(state.sslSupportedReceived(unprocessedBytes: 0), .establishSSLConnection)
        XCTAssertEqual(state.sslHandlerAdded(), .wait)
        XCTAssertEqual(state.sslEstablished(), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        let salt: UInt32 = 0x00_01_02_03
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
    }

    func testSSLStartupFailureTooManyBytesRemaining() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        XCTAssertEqual(state.connected(tls: .require), .sendSSLRequest)
        let failError = PSQLError.receivedUnencryptedDataAfterSSLRequest
        XCTAssertEqual(state.sslSupportedReceived(unprocessedBytes: 1), .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: failError, closePromise: nil)))
    }

    func testSSLStartupFailHandler() {
        struct SSLHandlerAddError: Error, Equatable {}
        
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        XCTAssertEqual(state.connected(tls: .require), .sendSSLRequest)
        XCTAssertEqual(state.sslSupportedReceived(unprocessedBytes: 0), .establishSSLConnection)
        let failError = PSQLError.failedToAddSSLHandler(underlying: SSLHandlerAddError())
        XCTAssertEqual(state.errorHappened(failError), .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: failError, closePromise: nil)))
    }
    
    func testTLSRequiredStartupSSLUnsupported() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        
        XCTAssertEqual(state.connected(tls: .require), .sendSSLRequest)
        XCTAssertEqual(state.sslUnsupportedReceived(),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: PSQLError.sslUnsupported, closePromise: nil)))
    }

    func testTLSPreferredStartupSSLUnsupported() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)

        XCTAssertEqual(state.connected(tls: .prefer), .sendSSLRequest)
        XCTAssertEqual(state.sslUnsupportedReceived(), .provideAuthenticationContext)
    }
        
    func testParameterStatusReceivedAndBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)), .wait)
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testBackendKeyAndParameterStatusReceivedAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        XCTAssertEqual(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)), .wait)
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testReadyForQueryReceivedWithoutBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]), requireBackendKeyData: true)
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.readyForQueryReceived(.idle),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: PSQLError.unexpectedBackendMessage(.readyForQuery(.idle)), closePromise: nil)))
    }
    
    func testReadyForQueryReceivedWithoutUnneededBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]), requireBackendKeyData: false)
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testErrorIsIgnoredWhenClosingConnection() {
        // test ignore unclean shutdown when closing connection
        var stateIgnoreChannelError = ConnectionStateMachine(.closing(nil))

        XCTAssertEqual(stateIgnoreChannelError.errorHappened(.connectionError(underlying: NIOSSLError.uncleanShutdown)), .wait)
        XCTAssertEqual(stateIgnoreChannelError.closed(), .fireChannelInactive)
        
        // test ignore any other error when closing connection
        
        var stateIgnoreErrorMessage = ConnectionStateMachine(.closing(nil))
        XCTAssertEqual(stateIgnoreErrorMessage.errorReceived(.init(fields: [:])), .wait)
        XCTAssertEqual(stateIgnoreErrorMessage.closed(), .fireChannelInactive)
    }
    
    func testFailQueuedQueriesOnAuthenticationFailure() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        let salt: UInt32 = 0x00_01_02_03

        let queryPromise = eventLoopGroup.next().makePromise(of: PSQLRowStream.self)

        var state = ConnectionStateMachine(requireBackendKeyData: true)
        let extendedQueryContext = ExtendedQueryContext(
            query: "Select version()",
            logger: .psqlTest,
            promise: queryPromise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(extendedQueryContext, writePromise: nil)), .wait)
        XCTAssertEqual(state.connected(tls: .disable), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
        let fields: [PostgresBackendMessage.Field: String] = [
            .message: "password authentication failed for user \"postgres\"",
            .severity: "FATAL",
            .sqlState: "28P01",
            .localizedSeverity: "FATAL",
            .routine: "auth_failed",
            .line: "334",
            .file: "auth.c"
        ]
        XCTAssertEqual(state.errorReceived(.init(fields: fields)),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [.extendedQuery(extendedQueryContext, writePromise: nil)], error: .server(.init(fields: fields)), closePromise: nil)))
        
        XCTAssertNil(queryPromise.futureResult._value)

        // make sure we don't crash
        queryPromise.fail(PSQLError.server(.init(fields: fields)))
    }
}
