import NIOCore
import Foundation

/// A type that can encode itself to a postgres wire binary representation.
protocol PostgresEncodable {
    /// identifies the data type that we will encode into `byteBuffer` in `encode`
    var psqlType: PostgresDataType { get }
    
    /// identifies the postgres format that is used to encode the value into `byteBuffer` in `encode`
    var psqlFormat: PostgresFormat { get }
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format, without setting
    /// the byte count. This method is called from the default `encodeRaw` implementation.
    func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) throws
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format including its
    /// leading byte count. This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    func encodeRaw<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) throws
}

/// A type that can decode itself from a postgres wire binary representation.
protocol PostgresDecodable {
    associatedtype DecodableType: PostgresDecodable = Self

    /// Decode an entity from the `byteBuffer` in postgres wire format
    ///
    /// - Parameters:
    ///   - byteBuffer: A `ByteBuffer` to decode. The byteBuffer is sliced in such a way that it is expected
    ///                 that the complete buffer is consumed for decoding
    ///   - type: The postgres data type. Depending on this type the `byteBuffer`'s bytes need to be interpreted
    ///           in different ways.
    ///   - format: The postgres wire format. Can be `.text` or `.binary`
    ///   - context: A `PSQLDecodingContext` providing context for decoding. This includes a `JSONDecoder`
    ///              to use when decoding json and metadata to create better errors.
    /// - Returns: A decoded object
    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self

    /// Decode an entity from the `byteBuffer` in postgres wire format.
    /// This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    static func decodeRaw<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self
}

extension PostgresDecodable {
    @inlinable
    static func decodeRaw<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard var buffer = byteBuffer else {
            throw PostgresCastingError.Code.missingData
        }
        return try self.decode(from: &buffer, type: type, format: format, context: context)
    }
}

/// A type that can be encoded into and decoded from a postgres binary format
protocol PostgresCodable: PostgresEncodable, PostgresDecodable {}

extension PostgresEncodable {
    func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // The length of the parameter value, in bytes (this count does not include
        // itself). Can be zero.
        let lengthIndex = buffer.writerIndex
        buffer.writeInteger(0, as: Int32.self)
        let startIndex = buffer.writerIndex
        // The value of the parameter, in the format indicated by the associated format
        // code. n is the above length.
        try self.encode(into: &buffer, context: context)
        
        // overwrite the empty length, with the real value
        buffer.setInteger(numericCast(buffer.writerIndex - startIndex), at: lengthIndex, as: Int32.self)
    }
}

struct PostgresEncodingContext<JSONEncoder: PostgresJSONEncoder> {
    let jsonEncoder: JSONEncoder

    init(jsonEncoder: JSONEncoder) {
        self.jsonEncoder = jsonEncoder
    }
}

extension PostgresEncodingContext where JSONEncoder == Foundation.JSONEncoder {
    static let `default` = PostgresEncodingContext(jsonEncoder: JSONEncoder())
}

struct PostgresDecodingContext<JSONDecoder: PostgresJSONDecoder> {
    let jsonDecoder: JSONDecoder
    
    init(jsonDecoder: JSONDecoder) {
        self.jsonDecoder = jsonDecoder
    }
}
