import NIOCore
import struct Foundation.Decimal

extension Decimal: PSQLCodable {
    var psqlType: PostgresDataType {
        .numeric
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &buffer) else {
                throw PostgresCastingError.Code.failure
            }
            return numeric.decimal
        case (.text, .numeric):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Decimal(string: string) else {
                throw PostgresCastingError.Code.failure
            }
            return value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        let numeric = PostgresNumeric(decimal: self)
        byteBuffer.writeInteger(numeric.ndigits)
        byteBuffer.writeInteger(numeric.weight)
        byteBuffer.writeInteger(numeric.sign)
        byteBuffer.writeInteger(numeric.dscale)
        var value = numeric.value
        byteBuffer.writeBuffer(&value)
    }
}
