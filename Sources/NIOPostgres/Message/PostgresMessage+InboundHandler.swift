import NIO

extension PostgresMessage {
    /// Decodes `PostgresMessage`s from incoming data.
    final class InboundHandler: ByteToMessageDecoder {
        /// See `ByteToMessageDecoder`.
        typealias InboundOut = PostgresMessage
        
        /// See `ByteToMessageDecoder`.
        var cumulationBuffer: ByteBuffer?
        
        /// If `true`, the server has asked for authentication.
        var hasRequestedAuthentication: Bool
        
        /// Creates a new `PostgresMessageDecoder`.
        init() {
            self.hasRequestedAuthentication = false
        }
        
        /// See `ByteToMessageDecoder`.
        func decode(ctx: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            // special check for SSL response
            var sslBuffer = buffer
            if
                !self.hasRequestedAuthentication,
                let sslResponse = sslBuffer.readInteger(as: UInt8.self)
                    .flatMap(PostgresMessage.SSLResponse.init)
            {
                buffer = sslBuffer
                ctx.fireChannelRead(wrapInboundOut(.sslResponse(sslResponse)))
                return .continue
            }
        
            var peekBuffer = buffer
            // peek at the message identifier
            // the message identifier is always the first byte of a message
            guard let messageIdentifier = peekBuffer.readInteger(as: UInt8.self).map(PostgresMessage.Identifier.init) else {
                return .needMoreData
            }
            
            // peek at the message size
            // the message size is always a 4 byte integer appearing immediately after the message identifier
            guard let messageSize = peekBuffer.readInteger(as: Int32.self).flatMap(Int.init) else {
                return .needMoreData
            }
            
            // ensure message is large enough (skipping message type) or reject
            guard peekBuffer.readableBytes >= messageSize - 4 else {
                return .needMoreData
            }
            
            // there is sufficient data, use this buffer
            buffer = peekBuffer
            
            let message: PostgresMessage
            switch messageIdentifier {
            case .authenticationCleartextPassword, .authenticationMD5Password:
                self.hasRequestedAuthentication = true
                message = try .authentication(.parse(from: &buffer))
            case .backendKeyData:
                message = try .backendKeyData(.parse(from: &buffer))
            case .bindComplete:
                message = .bindComplete
            case .commandComplete:
                message = try .commandComplete(.parse(from: &buffer))
            case .dataRow:
                message = try .dataRow(.parse(from: &buffer))
            case .errorResponse:
                message = try .error(.parse(from: &buffer))
            case .noticeResponse:
                message = try .notice(.parse(from: &buffer))
            case .noData:
                message = .noData
            case .parameterDescription:
                message = try .parameterDescription(.parse(from: &buffer))
            case .parameterStatus:
                message = try .parameterStatus(.parse(from: &buffer))
            case .parseComplete:
                message = .parseComplete
            case .readyForQuery:
                message = try .readyForQuery(.parse(from: &buffer))
            case .rowDescription:
                message = try .rowDescription(.parse(from: &buffer))
            default:
                throw PostgresError(.protocol("Unsupported incoming message identifier: \(messageIdentifier)"))
            }
            
            ctx.fireChannelRead(wrapInboundOut(message))
            return .continue
        }
        
        func decodeLast(ctx: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            // ignore
            return .needMoreData
        }
    }
}
