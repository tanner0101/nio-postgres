import XCTest
import NIOCore
@testable import PostgresNIO

class ExecuteTests: XCTestCase {
    
    func testEncodeExecute() {
        let encoder = PSQLFrontendMessageEncoder.forTests
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.execute(.init(portalName: "", maxNumberOfRows: 0))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        XCTAssertEqual(byteBuffer.readableBytes, 10) // 1 (id) + 4 (length) + 1 (empty null terminated string) + 4 (count)
        XCTAssertEqual(PSQLFrontendMessage.ID.execute.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(9, byteBuffer.readInteger(as: Int32.self)) // length
        XCTAssertEqual("", byteBuffer.psqlReadNullTerminatedString())
        XCTAssertEqual(0, byteBuffer.readInteger(as: Int32.self))
    }
}
