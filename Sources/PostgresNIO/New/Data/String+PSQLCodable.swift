import NIOCore
import struct Foundation.UUID

extension String: PSQLCodable {
    var psqlType: PostgresDataType {
        .text
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeString(self)
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (_, .varchar),
             (_, .text),
             (_, .name):
            // we can force unwrap here, since this method only fails if there are not enough
            // bytes available.
            return buffer.readString(length: buffer.readableBytes)!
        case (_, .uuid):
            guard let uuid = try? UUID.decode(from: &buffer, type: .uuid, format: format, context: context) else {
                throw PSQLCastingError.Code.failure
            }
            return uuid.uuidString
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
}
