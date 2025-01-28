import class Foundation.JSONDecoder
import struct Foundation.Data
import NIOFoundationCompat
import NIOCore
import NIOConcurrencyHelpers

/// A protocol that mimicks the Foundation `JSONDecoder.decode(_:from:)` function.
/// Conform a non-Foundation JSON decoder to this protocol if you want PostgresNIO to be
/// able to use it when decoding JSON & JSONB values (see `PostgresNIO._defaultJSONDecoder`)
@preconcurrency
public protocol PostgresJSONDecoder: Sendable {
    func decode<T>(_ type: T.Type, from data: Data) throws -> T where T : Decodable

    func decode<T: Decodable>(_ type: T.Type, from buffer: ByteBuffer) throws -> T
}

extension PostgresJSONDecoder {
    public func decode<T: Decodable>(_ type: T.Type, from buffer: ByteBuffer) throws -> T {
        var copy = buffer
        let data = copy.readData(length: buffer.readableBytes)!
        return try self.decode(type, from: data)
    }
}

extension JSONDecoder: PostgresJSONDecoder {}

private let jsonDecoderLocked: NIOLockedValueBox<PostgresJSONDecoder> = NIOLockedValueBox(JSONDecoder())

/// The default JSON decoder used by PostgresNIO when decoding JSON & JSONB values.
/// As `_defaultJSONDecoder` will be reused for decoding all JSON & JSONB values
/// from potentially multiple threads at once, you must ensure your custom JSON decoder is
/// thread safe internally like `Foundation.JSONDecoder`.
public var _defaultJSONDecoder: PostgresJSONDecoder {
    set {
        jsonDecoderLocked.withLockedValue { $0 = newValue }
    }
    get {
        jsonDecoderLocked.withLockedValue { $0 }
    }
}
