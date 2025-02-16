import Logging
import NIOCore

enum HandlerTask: Sendable {
    case extendedQuery(ExtendedQueryContext)
    case simpleQuery(SimpleQueryContext)
    case closeCommand(CloseCommandContext)
    case startListening(NotificationListener)
    case cancelListening(String, Int)
    case executePreparedStatement(PreparedStatementContext)
}

enum PSQLTask {
    case extendedQuery(ExtendedQueryContext)
    case simpleQuery(SimpleQueryContext)
    case closeCommand(CloseCommandContext)

    func failWithError(_ error: PSQLError) {
        switch self {
        case .extendedQuery(let extendedQueryContext):
            switch extendedQueryContext.query {
            case .unnamed(_, let eventLoopPromise):
                eventLoopPromise.fail(error)
            case .executeStatement(_, let eventLoopPromise):
                eventLoopPromise.fail(error)
            case .prepareStatement(_, _, _, let eventLoopPromise):
                eventLoopPromise.fail(error)
            }

        case .simpleQuery(let simpleQueryContext):
            simpleQueryContext.promise.fail(error)

        case .closeCommand(let closeCommandContext):
            closeCommandContext.promise.fail(error)
        }
    }
}

final class ExtendedQueryContext: Sendable {
    enum Query {
        case unnamed(PostgresQuery, EventLoopPromise<PSQLRowStream>)
        case executeStatement(PSQLExecuteStatement, EventLoopPromise<PSQLRowStream>)
        case prepareStatement(name: String, query: String, bindingDataTypes: [PostgresDataType], EventLoopPromise<RowDescription?>)
    }
    
    let query: Query
    let logger: Logger

    init(
        query: PostgresQuery,
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.query = .unnamed(query, promise)
        self.logger = logger
    }
    
    init(
        executeStatement: PSQLExecuteStatement,
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.query = .executeStatement(executeStatement, promise)
        self.logger = logger
    }

    init(
        name: String,
        query: String,
        bindingDataTypes: [PostgresDataType],
        logger: Logger,
        promise: EventLoopPromise<RowDescription?>
    ) {
        self.query = .prepareStatement(name: name, query: query, bindingDataTypes: bindingDataTypes, promise)
        self.logger = logger
    }
}

final class PreparedStatementContext: Sendable {
    let name: String
    let sql: String
    let bindingDataTypes: [PostgresDataType]
    let bindings: PostgresBindings
    let logger: Logger
    let promise: EventLoopPromise<PSQLRowStream>

    init(
        name: String,
        sql: String,
        bindings: PostgresBindings,
        bindingDataTypes: [PostgresDataType],
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.name = name
        self.sql = sql
        self.bindings = bindings
        if bindingDataTypes.isEmpty {
            self.bindingDataTypes = bindings.metadata.map(\.dataType)
        } else {
            self.bindingDataTypes = bindingDataTypes
        }
        self.logger = logger
        self.promise = promise
    }
}

final class SimpleQueryContext {
    let query: String
    let logger: Logger
    let promise: EventLoopPromise<PSQLRowStream>

    init(
        query: String,
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.query = query
        self.logger = logger
        self.promise = promise
    }
}

final class CloseCommandContext: Sendable {
    let target: CloseTarget
    let logger: Logger
    let promise: EventLoopPromise<Void>
    
    init(
        target: CloseTarget,
        logger: Logger,
        promise: EventLoopPromise<Void>
    ) {
        self.target = target
        self.logger = logger
        self.promise = promise
    }
}

