import class Foundation.JSONEncoder
import NIOCore
@testable import PostgresNIO

// fully-qualifying all types in the extension has the same effect as adding a `@retroactive` before the protocol
extension PostgresNIO.ConnectionStateMachine.ConnectionAction: Swift.Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.read, read):
            return true
        case (.wait, .wait):
            return true
        case (.provideAuthenticationContext, .provideAuthenticationContext):
            return true
        case (.sendStartupMessage, sendStartupMessage):
            return true
        case (.sendSSLRequest, sendSSLRequest):
            return true
        case (.establishSSLConnection, establishSSLConnection):
            return true
        case (.closeConnectionAndCleanup(let lhs), .closeConnectionAndCleanup(let rhs)):
            return lhs == rhs
        case (.sendPasswordMessage(let lhsMethod, let lhsAuthContext), sendPasswordMessage(let rhsMethod, let rhsAuthContext)):
            return lhsMethod == rhsMethod && lhsAuthContext == rhsAuthContext
        case (.sendParseDescribeBindExecuteSync(let lquery, _), sendParseDescribeBindExecuteSync(let rquery, _)):
            return lquery == rquery
        case (.fireEventReadyForQuery, .fireEventReadyForQuery):
            return true
        case (.succeedQuery(let lhsPromise, let lhsResult), .succeedQuery(let rhsPromise, let rhsResult)):
            return lhsPromise.futureResult === rhsPromise.futureResult && lhsResult.value == rhsResult.value
        case (.failQuery(let lhsPromise, let lhsError, let lhsCleanupContext), .failQuery(let rhsPromise, let rhsError, let rhsCleanupContext)):
            return lhsPromise.futureResult === rhsPromise.futureResult && lhsError == rhsError && lhsCleanupContext == rhsCleanupContext
        case (.forwardRows(let lhsRows), .forwardRows(let rhsRows)):
            return lhsRows == rhsRows
        case (.forwardStreamComplete(let lhsBuffer, let lhsCommandTag), .forwardStreamComplete(let rhsBuffer, let rhsCommandTag)):
            return lhsBuffer == rhsBuffer && lhsCommandTag == rhsCommandTag
        case (.forwardStreamError(let lhsError, let lhsRead, let lhsCleanupContext), .forwardStreamError(let rhsError , let rhsRead, let rhsCleanupContext)):
            return lhsError == rhsError && lhsRead == rhsRead && lhsCleanupContext == rhsCleanupContext
        case (.sendParseDescribeSync(let lhsName, let lhsQuery, let lhsDataTypes, _), .sendParseDescribeSync(let rhsName, let rhsQuery, let rhsDataTypes, _)):
            return lhsName == rhsName && lhsQuery == rhsQuery && lhsDataTypes == rhsDataTypes
        case (.succeedPreparedStatementCreation(let lhsPromise, let lhsRowDescription), .succeedPreparedStatementCreation(let rhsPromise, let rhsRowDescription)):
            return lhsPromise.futureResult === rhsPromise.futureResult && lhsRowDescription == rhsRowDescription
        case (.fireChannelInactive, .fireChannelInactive):
            return true
        default:
            return false
        }
    }
}

// fully-qualifying all types in the extension has the same effect as adding a `@retroactive` before the protocol'
extension PostgresNIO.ConnectionStateMachine.ConnectionAction.CleanUpContext: Swift.Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        guard lhs.closePromise?.futureResult === rhs.closePromise?.futureResult else {
            return false
        }
        
        guard lhs.error == rhs.error else {
            return false
        }
        
        guard lhs.tasks == rhs.tasks else {
            return false
        }
        
        return true
    }
}

extension ConnectionStateMachine {
    static func readyForQuery(transactionState: PostgresBackendMessage.TransactionState = .idle) -> Self {
        let connectionContext = Self.createConnectionContext(transactionState: transactionState)
        return ConnectionStateMachine(.readyForQuery(connectionContext))
    }
    
    static func createConnectionContext(transactionState: PostgresBackendMessage.TransactionState = .idle) -> ConnectionContext {
        let backendKeyData = BackendKeyData(processID: 2730, secretKey: 882037977)
        
        let paramaters = [
            "DateStyle": "ISO, MDY",
            "application_name": "",
            "server_encoding": "UTF8",
            "integer_datetimes": "on",
            "client_encoding": "UTF8",
            "TimeZone": "Etc/UTC",
            "is_superuser": "on",
            "server_version": "13.1 (Debian 13.1-1.pgdg100+1)",
            "session_authorization": "postgres",
            "IntervalStyle": "postgres",
            "standard_conforming_strings": "on"
        ]
        
        return ConnectionContext(
            backendKeyData: backendKeyData,
            parameters: paramaters,
            transactionState: transactionState
        )
    }
}

// fully-qualifying all types in the extension has the same effect as adding a `@retroactive` before the protocol
extension PostgresNIO.PSQLError: Swift.Equatable {
    public static func == (lhs: PSQLError, rhs: PSQLError) -> Bool {
        return true
    }
}

// fully-qualifying all types in the extension has the same effect as adding a `@retroactive` before the protocol
extension PostgresNIO.PSQLTask: Swift.Equatable {
    public static func == (lhs: PSQLTask, rhs: PSQLTask) -> Bool {
        switch (lhs, rhs) {
        case (.extendedQuery(let lhs, _), .extendedQuery(let rhs, _)):
            return lhs === rhs
        case (.closeCommand(let lhs, _), .closeCommand(let rhs, _)):
            return lhs === rhs
        default:
            return false
        }
    }
}
