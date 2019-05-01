extension PostgresData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        self.init(type: .text, formatCode: .binary, value: buffer)
    }
    
    public var string: String? {
        guard var value = self.value else {
            return nil
        }
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .varchar, .text:
                guard let string = value.readString(length: value.readableBytes) else {
                    fatalError()
                }
                return string
            case .numeric:
                return self.numeric?.string
            case .uuid:
                return value.readUUID()!.uuidString
            case .timestamp, .timestamptz, .date:
                return self.date?.description
            case .money:
                assert(value.readableBytes == 8)
                guard let int64 = value.getInteger(at: value.readerIndex, as: Int64.self) else {
                    fatalError()
                }
                let description = int64.description
                switch description.count {
                case 0:
                    return "0.00"
                case 1:
                    return "0.0" + description
                case 2:
                    return "0." + description
                default:
                    let decimalIndex = description.index(description.endIndex, offsetBy: -2)
                    return description[description.startIndex..<decimalIndex]
                        + "."
                        + description[decimalIndex..<description.endIndex]
                }
            case .float4, .float8:
                return self.double?.description
            case .int2, .int4, .int8:
                return self.int?.description
            default:
                fatalError("Cannot decode String from \(self.type)")
            }
        case .text:
            guard let string = value.readString(length: value.readableBytes) else {
                fatalError()
            }
            return string
        }
    }
}

extension PostgresData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}
