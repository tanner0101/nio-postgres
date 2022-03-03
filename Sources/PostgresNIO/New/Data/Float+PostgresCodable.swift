import NIOCore

extension Float: PostgresEncodable {
    var psqlType: PostgresDataType {
        .float4
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.psqlWriteFloat(self)
    }
}

extension Float: PostgresDecodable {
    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PostgresCastingError.Code.failure
            }
            self = float
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PostgresCastingError.Code.failure
            }
            self = Float(double)
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Float(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Float: PostgresCodable {}

extension Double: PostgresEncodable {
    var psqlType: PostgresDataType {
        .float8
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.psqlWriteDouble(self)
    }
}

extension Double: PostgresDecodable {
    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PostgresCastingError.Code.failure
            }
            self = Double(float)
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PostgresCastingError.Code.failure
            }
            self = double
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Double(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Double: PostgresCodable {}
