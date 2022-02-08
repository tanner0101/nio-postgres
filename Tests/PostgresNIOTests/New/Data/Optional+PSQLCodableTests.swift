import XCTest
import NIOCore
@testable import PostgresNIO

class Optional_PSQLCodableTests: XCTestCase {

    func testRoundTripSomeString() {
        let value: String? = "Hello World"

        var buffer = ByteBuffer()
        XCTAssertNoThrow(try value.encodeRaw(into: &buffer, context: .default))
        XCTAssertEqual(value.psqlType, .text)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 11)

        var result: String?
        var optBuffer: ByteBuffer? = buffer
        #if swift(<5.4)
        XCTAssertNoThrow(result = try Optional<String>.decodeRaw(from: &optBuffer, type: .text, format: .binary, context: .default))
        #else
        XCTAssertNoThrow(result = try String?.decodeRaw(from: &optBuffer, type: .text, format: .binary, context: .default))
        #endif
        XCTAssertEqual(result, value)
    }

    func testRoundTripNoneString() {
        let value: Optional<String> = .none

        var buffer = ByteBuffer()
        XCTAssertNoThrow(try value.encodeRaw(into: &buffer, context: .default))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.getInteger(at: 0, as: Int32.self), -1)
        XCTAssertEqual(value.psqlType, .null)

        var result: String?
        var inBuffer: ByteBuffer? = nil
        #if swift(<5.4)
        XCTAssertNoThrow(result = try Optional<String>.decodeRaw(from: &inBuffer, type: .text, format: .binary, context: .default))
        #else
        XCTAssertNoThrow(result = try String?.decodeRaw(from: &inBuffer, type: .text, format: .binary, context: .default))
        #endif
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripSomeUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = UUID()
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .uuid)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .default))
        XCTAssertEqual(buffer.readableBytes, 20)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 16)

        var result: UUID?
        var optBuffer: ByteBuffer? = buffer
        #if swift(<5.4)
        XCTAssertNoThrow(result = try Optional<UUID>.decodeRaw(from: &optBuffer, type: .uuid, format: .binary, context: .default))
        #else
        XCTAssertNoThrow(result = try UUID?.decodeRaw(from: &optBuffer, type: .uuid, format: .binary, context: .default))
        #endif
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = .none
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .null)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .default))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), -1)

        var result: UUID?
        var inBuffer: ByteBuffer? = nil
        #if swift(<5.4)
        XCTAssertNoThrow(result = try Optional<UUID>.decodeRaw(from: &inBuffer, type: .uuid, format: .binary, context: .default))
        #else
        XCTAssertNoThrow(result = try UUID?.decodeRaw(from: &inBuffer, type: .uuid, format: .binary, context: .default))
        #endif
        XCTAssertEqual(result, value)
    }
}
