import struct Foundation.Decimal

extension Decimal: ExpressibleByPostgresFloatingPointString {
    public init?(floatingPointString: String) {
        self.init(string: floatingPointString)
    }
}
