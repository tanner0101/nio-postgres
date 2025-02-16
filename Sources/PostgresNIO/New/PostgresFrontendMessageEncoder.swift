import NIOCore

struct PostgresFrontendMessageEncoder {

    /// The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits,
    /// and 5679 in the least significant 16 bits.
    static let sslRequestCode: Int32 = 80877103

    /// The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits,
    /// and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same
    /// as any protocol version number.)
    static let cancelRequestCode: Int32 = 80877102

    static let startupVersionThree: Int32 = 0x00_03_00_00

    private enum State {
        case flushed
        case writable
    }
    
    private var buffer: ByteBuffer
    private var state: State = .writable
    
    init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    mutating func startup(user: String, database: String?, options: [(String, String)]) {
        self.clearIfNeeded()
        self.buffer.psqlLengthPrefixed { buffer in
            buffer.writeInteger(Self.startupVersionThree)
            buffer.writeNullTerminatedString("user")
            buffer.writeNullTerminatedString(user)

            if let database = database {
                buffer.writeNullTerminatedString("database")
                buffer.writeNullTerminatedString(database)
            }

            // we don't send replication parameters, as the default is false and this is what we
            // need for a client
            for (key, value) in options {
                buffer.writeNullTerminatedString(key)
                buffer.writeNullTerminatedString(value)
            }

            buffer.writeInteger(UInt8(0))
        }
    }

    mutating func bind(portalName: String, preparedStatementName: String, bind: PostgresBindings) {
        self.clearIfNeeded()
        self.buffer.psqlLengthPrefixed(id: .bind) { buffer in
            buffer.writeNullTerminatedString(portalName)
            buffer.writeNullTerminatedString(preparedStatementName)

            // The number of parameter format codes that follow (denoted C below). This can be
            // zero to indicate that there are no parameters or that the parameters all use the
            // default format (text); or one, in which case the specified format code is applied
            // to all parameters; or it can equal the actual number of parameters.
            buffer.writeInteger(UInt16(bind.count))

            // The parameter format codes. Each must presently be zero (text) or one (binary).
            bind.metadata.forEach {
                buffer.writeInteger($0.format.rawValue)
            }

            buffer.writeInteger(UInt16(bind.count))

            var parametersCopy = bind.bytes
            buffer.writeBuffer(&parametersCopy)

            // The number of result-column format codes that follow (denoted R below). This can be
            // zero to indicate that there are no result columns or that the result columns should
            // all use the default format (text); or one, in which case the specified format code
            // is applied to all result columns (if any); or it can equal the actual number of
            // result columns of the query.
            buffer.writeInteger(1, as: Int16.self)
            // The result-column format codes. Each must presently be zero (text) or one (binary).
            buffer.writeInteger(PostgresFormat.binary.rawValue, as: Int16.self)
        }
    }

    mutating func cancel(processID: Int32, secretKey: Int32) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(UInt32(16), Self.cancelRequestCode, processID, secretKey)
    }

    mutating func closePreparedStatement(_ preparedStatement: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .close, length: UInt32(2 + preparedStatement.utf8.count), UInt8(ascii: "S"))
        self.buffer.writeNullTerminatedString(preparedStatement)
    }

    mutating func closePortal(_ portal: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .close, length: UInt32(2 + portal.utf8.count), UInt8(ascii: "P"))
        self.buffer.writeNullTerminatedString(portal)
    }

    mutating func describePreparedStatement(_ preparedStatement: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .describe, length: UInt32(2 + preparedStatement.utf8.count), UInt8(ascii: "S"))
        self.buffer.writeNullTerminatedString(preparedStatement)
    }

    mutating func describePortal(_ portal: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .describe, length: UInt32(2 + portal.utf8.count), UInt8(ascii: "P"))
        self.buffer.writeNullTerminatedString(portal)
    }

    mutating func execute(portalName: String, maxNumberOfRows: Int32 = 0) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .execute, length: UInt32(5 + portalName.utf8.count))
        self.buffer.writeNullTerminatedString(portalName)
        self.buffer.writeInteger(maxNumberOfRows)
    }

    mutating func query(_ query: String) {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .query, length: UInt32(1 + query.utf8.count))
        self.buffer.writeNullTerminatedString(query)
    }

    mutating func parse<Parameters: Collection>(preparedStatementName: String, query: String, parameters: Parameters) where Parameters.Element == PostgresDataType {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(
            id: .parse,
            length: UInt32(preparedStatementName.utf8.count + 1 + query.utf8.count + 1 + 2 + MemoryLayout<PostgresDataType>.size * parameters.count)
        )
        self.buffer.writeNullTerminatedString(preparedStatementName)
        self.buffer.writeNullTerminatedString(query)
        self.buffer.writeInteger(UInt16(parameters.count))

        for dataType in parameters {
            self.buffer.writeInteger(dataType.rawValue)
        }
    }

    mutating func password<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .password, length: UInt32(bytes.count) + 1)
        self.buffer.writeBytes(bytes)
        self.buffer.writeInteger(UInt8(0))
    }

    mutating func flush() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .flush, length: 0)
    }

    mutating func saslResponse<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .password, length: UInt32(bytes.count))
        self.buffer.writeBytes(bytes)
    }

    mutating func saslInitialResponse<Bytes: Collection>(mechanism: String, bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .password, length: UInt32(mechanism.utf8.count + 1 + 4 + bytes.count))
        self.buffer.writeNullTerminatedString(mechanism)
        if bytes.count > 0 {
            self.buffer.writeInteger(Int32(bytes.count))
            self.buffer.writeBytes(bytes)
        } else {
            self.buffer.writeInteger(Int32(-1))
        }
    }

    mutating func ssl() {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(UInt32(8), Self.sslRequestCode)
    }

    mutating func sync() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .sync, length: 0)
    }

    mutating func terminate() {
        self.clearIfNeeded()
        self.buffer.psqlWriteMultipleIntegers(id: .terminate, length: 0)
    }

    mutating func flushBuffer() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }

    private mutating func clearIfNeeded() {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()

        case .writable:
            break
        }
    }
}

private enum FrontendMessageID: UInt8, Hashable, Sendable {
    case bind = 66 // B
    case close = 67 // C
    case describe = 68 // D
    case execute = 69 // E
    case flush = 72 // H
    case parse = 80 // P
    case password = 112 // p - also both sasl values
    case query = 81 // Q
    case sync = 83 // S
    case terminate = 88 // X
}

extension ByteBuffer {
    mutating fileprivate func psqlWriteMultipleIntegers(id: FrontendMessageID, length: UInt32) {
        self.writeMultipleIntegers(id.rawValue, 4 + length)
    }

    mutating fileprivate func psqlWriteMultipleIntegers<T1: FixedWidthInteger>(id: FrontendMessageID, length: UInt32, _ t1: T1) {
        self.writeMultipleIntegers(id.rawValue, 4 + length, t1)
    }

    mutating fileprivate func psqlLengthPrefixed(id: FrontendMessageID, _ encode: (inout ByteBuffer) -> ()) {
        let lengthIndex = self.writerIndex + 1
        self.psqlWriteMultipleIntegers(id: id, length: 0)
        encode(&self)
        let length = UInt32(self.writerIndex - lengthIndex)
        self.setInteger(length, at: lengthIndex)
    }

    mutating fileprivate func psqlLengthPrefixed(_ encode: (inout ByteBuffer) -> ()) {
        let lengthIndex = self.writerIndex
        self.writeInteger(UInt32(0)) // placeholder
        encode(&self)
        let length = UInt32(self.writerIndex - lengthIndex)
        self.setInteger(length, at: lengthIndex)
    }
}
