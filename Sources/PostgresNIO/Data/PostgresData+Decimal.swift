import Foundation

extension PostgresData {
    public var decimal: Decimal? {
        guard let string = self.string else {
            return nil
        }
        guard let decimal = Decimal(string: string) else {
            return nil
        }
        return decimal
    }

    public init(decimal: Decimal) {
        self.init(string: decimal.description)
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresDataConvertible`, since it is deprecated.")
extension Decimal: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .numeric
    }

    public init?(postgresData: PostgresData) {
        guard let decimal = postgresData.decimal else {
            return nil
        }
        self = decimal
    }

    public var postgresData: PostgresData? {
        return .init(numeric: PostgresNumeric(decimalString: self.description))
    }
}
