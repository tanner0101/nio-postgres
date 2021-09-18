@testable import PostgresNIO
import Foundation

extension PSQLFrontendMessageEncoder {
    static var forTests: Self {
        Self(jsonEncoder: JSONEncoder())
    }
}

extension PSQLDecodingContext {
    static func forTests(columnName: String = "unknown", columnIndex: Int = 0, jsonDecoder: PSQLJSONDecoder = JSONDecoder(), file: String = #file, line: Int = #line) -> Self {
        Self(jsonDecoder: JSONDecoder(), columnName: columnName, columnIndex: columnIndex, file: file, line: line)
    }
}

extension PSQLEncodingContext {
    static func forTests(jsonEncoder: PSQLJSONEncoder = JSONEncoder()) -> Self {
        Self(jsonEncoder: jsonEncoder)
    }
}
