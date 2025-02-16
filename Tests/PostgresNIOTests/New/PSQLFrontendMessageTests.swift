import XCTest
import NIOCore
@testable import PostgresNIO

class PSQLFrontendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testMessageIDs() {
        XCTAssertEqual(PostgresFrontendMessage.ID.bind.rawValue, UInt8(ascii: "B"))
        XCTAssertEqual(PostgresFrontendMessage.ID.close.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PostgresFrontendMessage.ID.describe.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PostgresFrontendMessage.ID.execute.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PostgresFrontendMessage.ID.flush.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PostgresFrontendMessage.ID.parse.rawValue, UInt8(ascii: "P"))
        XCTAssertEqual(PostgresFrontendMessage.ID.password.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.saslInitialResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.saslResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.sync.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PostgresFrontendMessage.ID.terminate.rawValue, UInt8(ascii: "X"))
    }
    
    // MARK: Encoder
    
    func testEncodeFlush() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.flush()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.flush.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }

    func testEncodeQuery() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let query = "SELECT * FROM foo"
        encoder.query(query)
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 23)
        XCTAssertEqual(PostgresFrontendMessage.ID.query.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(22, byteBuffer.readInteger(as: Int32.self)) // payload length
        XCTAssertEqual([UInt8](query.utf8), byteBuffer.readBytes(length: 17))
    }

    func testEncodeSync() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.sync()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.sync.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeTerminate() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.terminate()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.terminate.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }

}
