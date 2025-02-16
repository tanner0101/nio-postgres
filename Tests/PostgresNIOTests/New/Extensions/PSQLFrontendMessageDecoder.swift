@testable import PostgresNIO
import NIOCore

struct PSQLFrontendMessageDecoder: NIOSingleStepByteToMessageDecoder {
    typealias InboundOut = PostgresFrontendMessage
    
    private(set) var isInStartup: Bool
    
    init() {
        self.isInStartup = true
    }
    
    mutating func decode(buffer: inout ByteBuffer) throws -> PostgresFrontendMessage? {
        // make sure we have at least one byte to read
        guard buffer.readableBytes > 0 else {
            return nil
        }
        
        if self.isInStartup {
            guard let length = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) else {
                return nil
            }
            
            guard var messageSlice = buffer.getSlice(at: buffer.readerIndex + 4, length: Int(length) - 4) else {
                return nil
            }
            buffer.moveReaderIndex(to: Int(length))
            let finalIndex = buffer.readerIndex
            
            guard let code = messageSlice.readInteger(as: UInt32.self) else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: UInt32.self)
            }
            
            switch code {
            case 80877103:
                self.isInStartup = true
                return .sslRequest
                
            case 196608:
                var user: String?
                var database: String?
                var options = [(String, String)]()

                while let name = messageSlice.readNullTerminatedString(), messageSlice.readerIndex < finalIndex {
                    let value = messageSlice.readNullTerminatedString()
                    
                    switch name {
                    case "user":
                        user = value
                        
                    case "database":
                        database = value
                        
                    default:
                        if let value = value {
                            options.append((name, value))
                        }
                    }
                }
                
                let parameters = PostgresFrontendMessage.Startup.Parameters(
                    user: user!,
                    database: database,
                    options: options,
                    replication: .false
                )
                
                let startup = PostgresFrontendMessage.Startup(
                    protocolVersion: 0x00_03_00_00,
                    parameters: parameters
                )
                
                precondition(buffer.readerIndex == finalIndex)
                self.isInStartup = false
                
                return .startup(startup)
                
            default:
                throw PostgresMessageDecodingError.unknownStartupCodeReceived(code: code, messageBytes: messageSlice)
            }
        }
        
        // all other packages have an Int32 after the identifier that determines their length.
        // do we have enough bytes for that?
        guard let idByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self),
              let length = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self) else {
            return nil
        }
        
        // At this point we are sure, that we have enough bytes to decode the next message.
        // 1. Create a byteBuffer that represents exactly the next message. This can be force
        //    unwrapped, since it was verified that enough bytes are available.
        guard let completeMessageBuffer = buffer.readSlice(length: 1 + Int(length)) else {
            return nil
        }
        
        // 2. make sure we have a known message identifier
        guard let messageID = PostgresFrontendMessage.ID(rawValue: idByte) else {
            throw PostgresMessageDecodingError.unknownMessageIDReceived(messageID: idByte, messageBytes: completeMessageBuffer)
        }
        
        // 3. decode the message
        do {
            // get a mutable byteBuffer copy
            var slice = completeMessageBuffer
            // move reader index forward by five bytes
            slice.moveReaderIndex(forwardBy: 5)
            
            return try PostgresFrontendMessage.decode(from: &slice, for: messageID)
        } catch let error as PSQLPartialDecodingError {
            throw PostgresMessageDecodingError.withPartialError(error, messageID: messageID.rawValue, messageBytes: completeMessageBuffer)
        } catch {
            preconditionFailure("Expected to only see `PartialDecodingError`s here.")
        }
    }
    
    mutating func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> PostgresFrontendMessage? {
        try self.decode(buffer: &buffer)
    }
}

extension PostgresFrontendMessage {
    
    static func decode(from buffer: inout ByteBuffer, for messageID: ID) throws -> PostgresFrontendMessage {
        switch messageID {
        case .bind:
            guard let portalName = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let preparedStatementName = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let parameterFormatCount = buffer.readInteger(as: UInt16.self) else {
                preconditionFailure("TODO: Unimplemented")
            }

            let parameterFormats = (0..<parameterFormatCount).map({ _ in PostgresFormat(rawValue: buffer.readInteger(as: Int16.self)!)! })

            guard let parameterCount = buffer.readInteger(as: UInt16.self) else {
                preconditionFailure("TODO: Unimplemented")
            }

            let parameters = (0..<parameterCount).map { _ -> ByteBuffer? in
                let length = buffer.readInteger(as: UInt32.self)
                switch length {
                case .some(..<0):
                    return nil
                case .some(0...):
                    return buffer.readSlice(length: Int(length!))
                default:
                    preconditionFailure("TODO: Unimplemented")
                }
            }

            guard let resultColumnFormatCount = buffer.readInteger(as: UInt16.self) else {
                preconditionFailure("TODO: Unimplemented")
            }

            let resultColumnFormats = (0..<resultColumnFormatCount).map({ _ in PostgresFormat(rawValue: buffer.readInteger(as: Int16.self)!)! })

            return .bind(
                Bind(
                    portalName: portalName,
                    preparedStatementName: preparedStatementName,
                    parameterFormats: parameterFormats,
                    parameters: parameters,
                    resultColumnFormats: resultColumnFormats
                )
            )

        case .close:
            preconditionFailure("TODO: Unimplemented")

        case .describe:
            switch buffer.readInteger(as: UInt8.self) {
            case UInt8(ascii: "S"):
                return .describe(.preparedStatement(buffer.readNullTerminatedString()!))
            case UInt8(ascii: "P"):
                return .describe(.portal(buffer.readNullTerminatedString()!))
            default:
                preconditionFailure("TODO: Unimplemented")
            }

        case .execute:
            guard let portalName = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }

            guard let maxNumberOfRows = buffer.readInteger(as: Int32.self) else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: Int.self)
            }

            return .execute(.init(portalName: portalName, maxNumberOfRows: maxNumberOfRows))

        case .flush:
            return .flush

        case .parse:
            guard let preparedStatementName = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let query = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let parameterCount = buffer.readInteger(as: UInt16.self) else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            let parameters = (0..<parameterCount).map({ _ in PostgresDataType(buffer.readInteger(as: UInt32.self)!) })
            return .parse(.init(preparedStatementName: preparedStatementName, query: query, parameters: parameters))

        case .password:
            guard let password = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .password(.init(value: password))
        case .saslInitialResponse:
            preconditionFailure("TODO: Unimplemented")
        case .saslResponse:
            preconditionFailure("TODO: Unimplemented")
        case .query:
            guard let query = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .query(.init(query: query))
        case .sync:
            return .sync
        case .terminate:
            return .terminate
        }
    }
}

extension PostgresMessageDecodingError {
    static func unknownStartupCodeReceived(
        code: UInt32,
        messageBytes: ByteBuffer,
        file: String = #fileID,
        line: Int = #line) -> Self
    {
        var byteBuffer = messageBytes
        let data = byteBuffer.readData(length: byteBuffer.readableBytes)!
        
        return PostgresMessageDecodingError(
            messageID: 0,
            payload: data.base64EncodedString(),
            description: "Received a startup code '\(code)'. There is no message associated with this code.",
            file: file,
            line: line)
    }
}
