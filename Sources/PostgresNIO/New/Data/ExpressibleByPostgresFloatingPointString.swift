import NIOCore


/// This protocol allows using various implementations of a Decimal type for Postgres NUMERIC type
public protocol ExpressibleByPostgresFloatingPointString: PostgresEncodable, PostgresDecodable {
    static var psqlType: PostgresDataType { get }
    static var psqlFormat: PostgresFormat { get }
    
    init?(floatingPointString: String)
    var description: String { get }
}

extension ExpressibleByPostgresFloatingPointString {
    public static var psqlType: PostgresDataType {
        .numeric
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    // PostgresEncodable conformance
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let numeric = PostgresNumeric(decimalString: self.description)
        byteBuffer.writeInteger(numeric.ndigits)
        byteBuffer.writeInteger(numeric.weight)
        byteBuffer.writeInteger(numeric.sign)
        byteBuffer.writeInteger(numeric.dscale)
        var value = numeric.value
        byteBuffer.writeBuffer(&value)
    }

    // PostgresDecodable conformance
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &buffer) else {
                throw PostgresDecodingError.Code.failure
            }
            // numeric.string is valid decimal representation
            self = Self(floatingPointString: numeric.string)!
        case (.text, .numeric):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Self(floatingPointString: string) else {
                throw PostgresDecodingError.Code.failure
            }
            self = value
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}
