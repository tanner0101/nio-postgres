import NIOCore
import struct Foundation.UUID

/// A type, of which arrays can be encoded into and decoded from a postgres binary format
public protocol PSQLArrayElement: PSQLCodable {
    static var psqlArrayType: PSQLDataType { get }
    static var psqlArrayElementType: PSQLDataType { get }
}

extension Bool: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .boolArray }
    public static var psqlArrayElementType: PSQLDataType { .bool }
}

extension ByteBuffer: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .byteaArray }
    public static var psqlArrayElementType: PSQLDataType { .bytea }
}

extension UInt8: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .charArray }
    public static var psqlArrayElementType: PSQLDataType { .char }
}

extension Int16: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .int2Array }
    public static var psqlArrayElementType: PSQLDataType { .int2 }
}

extension Int32: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .int4Array }
    public static var psqlArrayElementType: PSQLDataType { .int4 }
}

extension Int64: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .int8Array }
    public static var psqlArrayElementType: PSQLDataType { .int8 }
}

extension Int: PSQLArrayElement {
    #if (arch(i386) || arch(arm))
    public static var psqlArrayType: PSQLDataType { .int4Array }
    public static var psqlArrayElementType: PSQLDataType { .int4 }
    #else
    public static var psqlArrayType: PSQLDataType { .int8Array }
    public static var psqlArrayElementType: PSQLDataType { .int8 }
    #endif
}

extension Float: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .float4Array }
    public static var psqlArrayElementType: PSQLDataType { .float4 }
}

extension Double: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .float8Array }
    public static var psqlArrayElementType: PSQLDataType { .float8 }
}

extension String: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .textArray }
    public static var psqlArrayElementType: PSQLDataType { .text }
}

extension UUID: PSQLArrayElement {
    public static var psqlArrayType: PSQLDataType { .uuidArray }
    public static var psqlArrayElementType: PSQLDataType { .uuid }
}

extension Array: PSQLEncodable where Element: PSQLArrayElement {
    public var psqlType: PSQLDataType {
        Element.psqlArrayType
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public func encode(into buffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        // 0 if empty, 1 if not
        buffer.writeInteger(self.isEmpty ? 0 : 1, as: UInt32.self)
        // b
        buffer.writeInteger(0, as: Int32.self)
        // array element type
        buffer.writeInteger(Element.psqlArrayElementType.rawValue)

        // continue if the array is not empty
        guard !self.isEmpty else {
            return
        }
        
        // length of array
        buffer.writeInteger(numericCast(self.count), as: Int32.self)
        // dimensions
        buffer.writeInteger(1, as: Int32.self)

        try self.forEach { element in
            try element.encodeRaw(into: &buffer, context: context)
        }
    }
}

extension Array: PSQLDecodable where Element: PSQLArrayElement {
    
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Array<Element> {
        guard case .binary = format else {
            // currently we only support decoding arrays in binary format.
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let isNotEmpty = buffer.readInteger(as: Int32.self), (0...1).contains(isNotEmpty) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let b = buffer.readInteger(as: Int32.self), b == 0 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let elementType = buffer.readRawRepresentableInteger(as: PSQLDataType.self) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard isNotEmpty == 1 else {
            return []
        }
        
        guard let expectedArrayCount = buffer.readInteger(as: Int32.self), expectedArrayCount > 0 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let dimensions = buffer.readInteger(as: Int32.self), dimensions == 1 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        var result = Array<Element>()
        result.reserveCapacity(Int(expectedArrayCount))
        
        for _ in 0 ..< expectedArrayCount {
            guard let elementLength = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            guard var elementBuffer = buffer.readSlice(length: numericCast(elementLength)) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            let element = try Element.decode(from: &elementBuffer, type: elementType, format: format, context: context)
            
            result.append(element)
        }
        
        return result
    }
}

extension Array: PSQLCodable where Element: PSQLArrayElement {

}
