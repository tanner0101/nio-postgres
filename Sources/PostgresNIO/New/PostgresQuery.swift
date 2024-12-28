import NIOCore

/// A PostgreSQL statement, that can be executed on a  database. Contains the raw ``PostgresQuery/sql`` statement
/// and ``PostgresQuery/binds``, that are the parameters for the statement.
///
/// ## Creating a Query
///
/// #### Using string interpolation
///
/// Users should create ``PostgresQuery``s in most cases through string interpolation:
///
/// @Snippet(path: "postgres-nio/Snippets/PostgresQuery", slice: "select1")
///
/// While this looks at first glance like a classic case of [SQL injection](https://en.wikipedia.org/wiki/SQL_injection)
/// 😱, PostgresNIO ensures that this usage is safe.
/// The reason for this is, that ``PostgresQuery`` implements Swift's `ExpressibleByStringInterpolation`
/// protocol. ``PostgresQuery`` uses the literal parts of the provided string as the SQL query and replaces each interpolated
/// value with a parameter binding. Only values which implement the ``PostgresEncodable`` protocol may be interpolated
/// in this way.
///
/// ###### Interpolating non parameter values
///
/// Sometimes you need to interpolate parts of your query that can not be send to the server as an SQL binding. An example could
/// be the table name. In those cases add the `\(unescaped:)` keyword in front of your value.
///
/// @Snippet(path: "postgres-nio/Snippets/PostgresQuery-unescaped", slice: "unescaped")
///
/// > Warning:
/// Always make sure, that values passed via `\(unescaped:)` interpolations are trusted. Passing untrusted values can allow
/// [SQL injection](https://en.wikipedia.org/wiki/SQL_injection).
///
/// #### Manually creating a PostgresQuery
///
/// ``PostgresQuery`` can be created manually using the ``PostgresQuery/init(unsafeSQL:binds:)`` initializer.
/// In those cases ``PostgresQuery`` will not perform any validation on the provided SQL statement. Users must make sure
/// that their SQL is safe to execute.
public struct PostgresQuery: Sendable, Hashable {
    /// A raw SQL statement
    ///
    /// >Note:
    /// Since ``PostgresNIO`` only supports the 
    /// [Extended Query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
    /// flow, only a single sql statement is allowed. In other words: SQL statement batches will lead to server
    /// errors at all times.
    public var sql: String
    
    /// The parameters for the ``PostgresQuery/sql`` statement.
    public var binds: PostgresBindings

    /// Create a ``PostgresQuery`` with a SQL statement string and bindings as ``PostgresBindings``
    /// 
    /// > Warning:
    /// If you use string interpolation or generate the SQL statement through concatenating strings, it is your
    /// responsibility to ensure that you are not prone to [SQL injection](https://en.wikipedia.org/wiki/SQL_injection)
    /// attacks.
    ///
    /// - Parameters:
    ///   - sql: The SQL statement to execute.
    ///   - binds: The bindings for the SQL statement.
    public init(unsafeSQL sql: String, binds: PostgresBindings = PostgresBindings()) {
        self.sql = sql
        self.binds = binds
    }
}

extension PostgresQuery: ExpressibleByStringInterpolation {
    public init(stringInterpolation: StringInterpolation) {
        self.sql = stringInterpolation.sql
        self.binds = stringInterpolation.binds
    }

    public init(stringLiteral value: String) {
        self.sql = value
        self.binds = PostgresBindings()
    }
}

extension PostgresQuery {
    public struct StringInterpolation: StringInterpolationProtocol, Sendable {
        public typealias StringLiteralType = String

        @usableFromInline
        var sql: String
        @usableFromInline
        var binds: PostgresBindings

        public init(literalCapacity: Int, interpolationCount: Int) {
            self.sql = ""
            self.binds = PostgresBindings(capacity: interpolationCount)
        }

        public mutating func appendLiteral(_ literal: String) {
            self.sql.append(contentsOf: literal)
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Value) throws {
            try self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Optional<Value>) throws {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                try self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresDynamicTypeEncodable>(_ value: Value) {
            self.binds.append(value, context: .default)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresDynamicTypeEncodable>(_ value: Optional<Value>) {
            switch value {
            case .none:
                self.binds.appendNull()
            case .some(let value):
                self.binds.append(value, context: .default)
            }

            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
            _ value: Value,
            context: PostgresEncodingContext<JSONEncoder>
        ) throws {
            try self.binds.append(value, context: context)
            self.sql.append(contentsOf: "$\(self.binds.count)")
        }

        @inlinable
        public mutating func appendInterpolation(unescaped interpolated: String) {
            self.sql.append(contentsOf: interpolated)
        }
    }
}

extension PostgresQuery: CustomStringConvertible {
    // See `CustomStringConvertible.description`.
    public var description: String {
        "\(self.sql) \(self.binds)"
    }
}

extension PostgresQuery: CustomDebugStringConvertible {
    // See `CustomDebugStringConvertible.debugDescription`.
    public var debugDescription: String {
        "PostgresQuery(sql: \(String(describing: self.sql)), binds: \(String(reflecting: self.binds)))"
    }
}

struct PSQLExecuteStatement {
    /// The statements name
    var name: String
    /// The binds
    var binds: PostgresBindings

    var rowDescription: RowDescription?
}

/// Parameters/bindings for a ``PostgresQuery/sql`` statement.
public struct PostgresBindings: Sendable, Hashable {
    @usableFromInline
    struct Metadata: Sendable, Hashable {
        @usableFromInline
        var dataType: PostgresDataType
        @usableFromInline
        var format: PostgresFormat
        @usableFromInline
        var protected: Bool

        @inlinable
        init(dataType: PostgresDataType, format: PostgresFormat, protected: Bool) {
            self.dataType = dataType
            self.format = format
            self.protected = protected
        }

        @inlinable
        init<Value: PostgresThrowingDynamicTypeEncodable>(value: Value, protected: Bool) {
            self.init(dataType: value.psqlType, format: value.psqlFormat, protected: protected)
        }
    }

    @usableFromInline
    var metadata: [Metadata]
    @usableFromInline
    var bytes: ByteBuffer

    public var count: Int {
        self.metadata.count
    }

    public init() {
        self.metadata = []
        self.bytes = ByteBuffer()
    }

    public init(capacity: Int) {
        self.metadata = []
        self.metadata.reserveCapacity(capacity)
        self.bytes = ByteBuffer()
        self.bytes.reserveCapacity(128 * capacity)
    }

    public mutating func appendNull() {
        self.bytes.writeInteger(-1, as: Int32.self)
        self.metadata.append(.init(dataType: .null, format: .binary, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Value) throws {
        try self.append(value, context: .default)
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable>(_ value: Optional<Value>) throws {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            try self.append(value)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresThrowingDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Optional<Value>,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            try self.append(value, context: context)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable>(_ value: Value) {
        self.append(value, context: .default)
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable>(_ value: Optional<Value>) {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            self.append(value)
        }
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: true))
    }

    @inlinable
    public mutating func append<Value: PostgresDynamicTypeEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Optional<Value>,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        switch value {
        case .none:
            self.appendNull()
        case let .some(value):
            self.append(value, context: context)
        }
    }

    @inlinable
    mutating func appendUnprotected<Value: PostgresEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: false))
    }

    @inlinable
    mutating func appendUnprotected<Value: PostgresNonThrowingEncodable, JSONEncoder: PostgresJSONEncoder>(
        _ value: Value,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        value.encodeRaw(into: &self.bytes, context: context)
        self.metadata.append(.init(value: value, protected: false))
    }

    public mutating func append(_ postgresData: PostgresData) {
        switch postgresData.value {
        case .none:
            self.bytes.writeInteger(-1, as: Int32.self)
        case .some(var input):
            self.bytes.writeInteger(Int32(input.readableBytes))
            self.bytes.writeBuffer(&input)
        }
        self.metadata.append(.init(dataType: postgresData.type, format: .binary, protected: true))
    }
}

extension PostgresBindings: CustomStringConvertible, CustomDebugStringConvertible {
    // See `CustomStringConvertible.description`.
    public var description: String {
        """
        [\(zip(self.metadata, BindingsReader(buffer: self.bytes))
            .lazy.map({ Self.makeBindingPrintable(protected: $0.protected, type: $0.dataType, format: $0.format, buffer: $1) })
            .joined(separator: ", "))]
        """
    }

    // See `CustomDebugStringConvertible.description`.
    public var debugDescription: String {
        """
        [\(zip(self.metadata, BindingsReader(buffer: self.bytes))
            .lazy.map({ Self.makeDebugDescription(protected: $0.protected, type: $0.dataType, format: $0.format, buffer: $1) })
            .joined(separator: ", "))]
        """
    }

    private static func makeDebugDescription(protected: Bool, type: PostgresDataType, format: PostgresFormat, buffer: ByteBuffer?) -> String {
        "(\(Self.makeBindingPrintable(protected: protected, type: type, format: format, buffer: buffer)); \(type); format: \(format))"
    }

    private static func makeBindingPrintable(protected: Bool, type: PostgresDataType, format: PostgresFormat, buffer: ByteBuffer?) -> String {
        if protected {
            return "****"
        }

        guard var buffer = buffer else {
            return "null"
        }

        do {
            switch (type, format) {
            case (.int4, _), (.int2, _), (.int8, _):
                let number = try Int64.init(from: &buffer, type: type, format: format, context: .default)
                return String(describing: number)

            case (.bool, _):
                let bool = try Bool.init(from: &buffer, type: type, format: format, context: .default)
                return String(describing: bool)

            case (.varchar, _), (.bpchar, _), (.text, _), (.name, _):
                let value = try String.init(from: &buffer, type: type, format: format, context: .default)
                return String(reflecting: value) // adds quotes

            default:
                return "\(buffer.readableBytes) bytes"
            }
        } catch {
            return "\(buffer.readableBytes) bytes"
        }
    }
}

/// A small helper to inspect encoded bindings
private struct BindingsReader: Sequence {
    typealias Element = Optional<ByteBuffer>

    var buffer: ByteBuffer

    struct Iterator: IteratorProtocol {
        typealias Element = Optional<ByteBuffer>
        private var buffer: ByteBuffer

        init(buffer: ByteBuffer) {
            self.buffer = buffer
        }

        mutating func next() -> Optional<Optional<ByteBuffer>> {
            guard let length = self.buffer.readInteger(as: Int32.self) else {
                return .none
            }

            if length < 0 {
                return .some(.none)
            }

            return .some(self.buffer.readSlice(length: Int(length))!)
        }
    }

    func makeIterator() -> Iterator {
        Iterator(buffer: self.buffer)
    }
}
