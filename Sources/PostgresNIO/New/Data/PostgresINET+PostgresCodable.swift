public struct PostgresINET {
    public let ipFamily: UInt8
    public let netmaskLength: UInt8
    public let isCIDR: Bool
    public let addressLength: UInt8
    public let ipAddress: [UInt8]
}

extension PostgresINET: PostgresDecodable {
    public init<JSONDecoder: PostgresJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PostgresDecodingContext<JSONDecoder>) throws {
        // IP family
        guard let ipFamily: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        // netmask length in bits
        guard let netmaskLength: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        // whether it is a CIDR
        let isCIDR: Bool
        switch byteBuffer.readInteger(as: UInt8.self) {
        case .some(0):
            isCIDR = false
        case .some(1):
            isCIDR = true
        default:
            throw PostgresDecodingError.Code.failure
        }

        // address length in bytes
        guard let addressLength: UInt8 = byteBuffer.readInteger(as: UInt8.self),
            addressLength * 8 == netmaskLength,
            let ipAddress: [UInt8] = byteBuffer.readBytes(length: Int(addressLength))
        else {
            throw PostgresDecodingError.Code.failure
        }

        self.init(
            ipFamily: ipFamily,
            netmaskLength: netmaskLength,
            isCIDR: isCIDR,
            addressLength: addressLength,
            ipAddress: ipAddress
        )
    }
}

extension PostgresINET: PostgresEncodable & PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType { return .inet }
    public static var psqlFormat: PostgresFormat { .binary }
    public func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
        byteBuffer.writeInteger(self.ipFamily, as: UInt8.self)
        byteBuffer.writeInteger(self.netmaskLength, as: UInt8.self)
        byteBuffer.writeInteger(self.isCIDR ? 1 : 0, as: UInt8.self)
        byteBuffer.writeInteger(self.addressLength, as: UInt8.self)
        byteBuffer.writeBytes(self.ipAddress)
    }
}

extension PostgresINET: PostgresArrayDecodable {}

extension PostgresINET: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { return .inetArray }
}