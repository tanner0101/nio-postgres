import XCTest
import NIOCore
@testable import PostgresNIO

class Decimal_PSQLCodableTests: XCTestCase {
    
    func testRoundTrip() {
        let values: [Decimal] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .numeric)

            var result: Decimal?
            XCTAssertNoThrow(result = try Decimal.decode(from: &buffer, type: .numeric, format: .binary, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testDecodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))
        
        XCTAssertThrowsError(try Decimal.decode(from: &buffer, type: .int8, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PostgresCastingError.Code, .typeMismatch)
        }
    }
    
}
