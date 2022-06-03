@testable import PostgresNIO
import XCTest

final class PostgresDecodingErrorTests: XCTestCase {
    func testPostgresDecodingErrorEquality() {
        let error1 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        let error2 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: Int.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        XCTAssertNotEqual(error1, error2)
        let error3 = error1
        XCTAssertEqual(error1, error3)
    }

    func testPostgresDecodingErrorDescription() {
        let error = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        XCTAssertEqual("\(error)", "Database error")
    }
}
