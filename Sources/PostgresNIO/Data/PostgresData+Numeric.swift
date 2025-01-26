import NIOCore
import struct Foundation.Decimal

public struct PostgresNumeric: CustomStringConvertible, CustomDebugStringConvertible, ExpressibleByStringLiteral {
    /// The number of digits after this metadata
    internal var ndigits: Int16
    /// How many of the digits are before the decimal point (always add 1)
    internal var weight: Int16
    /// If 0x4000, this number is negative. See NUMERIC_NEG in
    /// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c
    internal var sign: Int16
    /// The number of sig digits after the decimal place (get rid of trailing 0s)
    internal var dscale: Int16
    /// Array of Int16, each representing 4 chars of the number
    internal var value: ByteBuffer

    public var description: String {
        return self.string
    }

    public var debugDescription: String {
        var copy = self.value
        var values: [Int16] = []
        while let value = copy.readInteger(endianness: .big, as: Int16.self) {
            values.append(value)
        }
        return """
        ndigits: \(self.ndigits)
        weight: \(self.weight)
        sign: \(self.sign)
        dscale: \(self.dscale)
        value: \(values)
        """
    }

    public var double: Double? {
        return Double(self.string)
    }
    
    public init(decimal: Decimal) {
        self.init(decimalString: decimal.description)
    }

    public init?(string: String) {
        // validate string contents are decimal
        // TODO: this won't work for all Big decimals
        // TODO: how does this handle Nan and Infinity
        guard Double(string) != nil else {
            return nil
        }
        self.init(decimalString: string)
    }
    
    public init(stringLiteral value: String) {
        self.init(decimalString: value)
    }

    internal init(decimalString: String) {
        // split on period, get integer and fractional
        let parts = decimalString.split(separator: ".")
        var integer: Substring
        let fractional: Substring?
        switch parts.count {
        case 1:
            integer = parts[0]
            fractional = nil
        case 2:
            integer = parts[0]
            fractional = parts[1]
        default:
            fatalError("Unexpected decimal string: \(decimalString)")
        }
        
        // check if negative
        let isNegative: Bool
        if integer.hasPrefix("-") {
            integer = integer.dropFirst()
            isNegative = true
        } else {
            isNegative = false
        }
        
        // buffer will store 1+ Int16 values representing
        // 4 digit chunks of the number
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        
        // weight always has 1 added to it, so start at -1
        var weight = -1
        
        // iterate over each chunk in the integer part of the numeric
        // we use reverse chunked since the first chunk should be the
        // shortest if the integer length is not evenly divisible by 4
        for chunk in integer.reverseChunked(by: 4) {
            weight += 1
            // convert the 4 digits to an Int16
            buffer.writeInteger(Int16(chunk)!, endianness: .big)
        }
        
        // dscale will measure how many sig digits are in the fraction
        var dscale = 0
        
        if let fractional = fractional {
            // iterate over each chunk in the fractional part of the numeric
            // we use normal chunking size the end chunk should be the shortest
            // (potentially having extra zeroes)
            for chunk in fractional.chunked(by: 4) {
                // for each _significant_ digit, increment dscale by count
                dscale += chunk.count
                // add trailing zeroes if the number is not 4 long
                let string = chunk + String(repeating: "0", count: 4 - chunk.count)
                // convert the 4 digits to an Int16
                buffer.writeInteger(Int16(string)!, endianness: .big)
            }
        }
        // ndigits is the number of int16's in the buffer
        self.ndigits = numericCast(buffer.readableBytes / 2)
        self.weight = numericCast(weight)
        self.sign = isNegative ? 0x4000 : 0
        self.dscale = numericCast(dscale)
        self.value = buffer
    }

    
    public var decimal: Decimal {
        // force cast should always succeed since we know
        // string returns a valid decimal
        return Decimal(string: self.string)!
    }

    public var string: String {
        guard self.ndigits > 0 else {
            return "0"
        }
        // print(self.debugDescription)

        // Digits before the decimal point.
        var integer = ""

        // Digits after the decimal point.
        var fractional = ""

        // Consume digits from the value buffer.
        var value = self.value
        for offset in 0..<self.ndigits {
            let char = value.readInteger(endianness: .big, as: Int16.self) ?? 0

            // Depending on offset, append value before or after the decimal point.
            if self.weight - offset >= 0 {
                if offset == 0 {
                    // First integer offset doesn't have trailing zeroes.
                    integer += char.description
                } else {
                    integer += String(repeating: "0", count: 4 - char.description.count) + char.description
                }
            } else {
                fractional += String(repeating: "0", count: 4 - char.description.count)
                    + char.description
            }
        }

        // Check for any remaining zeroes required before or after decimal point.
        let offset: Int16
        if self.weight > 0 {
            offset = (self.weight + 1) - self.ndigits
        } else {
            offset = abs(self.weight) - self.ndigits
        }
        if offset > 0 {
            for _ in 0..<offset {
                if self.weight > 0 {
                    integer = integer + "0000"
                } else {
                    fractional = "0000" + fractional
                }
            }
        }

        // Prevent fraction without leading "0"
        if integer.count == 0 {
            integer = "0"
        }

        // Remove extraneous zeroes at the end of the fraction.
        if fractional.count > self.dscale {
            let lastSignificant = fractional.index(
                fractional.startIndex,
                offsetBy: Int(self.dscale)
            )
            fractional = String(fractional[..<lastSignificant])
        }

        // Determine whether fraction is empty to add decimal point.
        let numeric: String
        if fractional != "" {
            numeric = integer + "." + fractional
        } else {
            numeric = integer
        }

        // Indicate whether or not the value is negative.
        if (self.sign & 0x4000) != 0 {
            return "-" + numeric
        } else {
            return numeric
        }
    }

    init?(buffer: inout ByteBuffer) {
        guard let ndigits = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.ndigits = ndigits
        guard let weight = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.weight = weight
        guard let sign = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.sign = sign
        guard let dscale = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.dscale = dscale
        self.value = buffer
    }
}

extension PostgresData {
    public init(numeric: PostgresNumeric) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeInteger(numeric.ndigits, endianness: .big)
        buffer.writeInteger(numeric.weight, endianness: .big)
        buffer.writeInteger(numeric.sign, endianness: .big)
        buffer.writeInteger(numeric.dscale, endianness: .big)
        var value = numeric.value
        buffer.writeBuffer(&value)
        self.init(type: .numeric, value: buffer)
    }
    
    public var numeric: PostgresNumeric? {
        /// create mutable value since we will be using `.extract` which advances the buffer's view
        guard var value = self.value else {
            return nil
        }

        /// grab the numeric metadata from the beginning of the array
        guard let metadata = PostgresNumeric(buffer: &value) else {
            return nil
        }

        return metadata
    }
}

private extension Collection {
    // splits the collection into chunks of the supplied size
    // if the collection is not evenly divisible, the last chunk will be smaller
    func chunked(by maxSize: Int) -> [SubSequence] {
        return stride(from: 0, to: self.count, by: maxSize).map { current in
            let chunkStartIndex = self.index(self.startIndex, offsetBy: current)
            let chunkEndOffset = Swift.min(
                self.distance(from: chunkStartIndex, to: self.endIndex),
                maxSize
            )
            let chunkEndIndex = self.index(chunkStartIndex, offsetBy: chunkEndOffset)
            return self[chunkStartIndex..<chunkEndIndex]
        }
    }
    
    // splits the collection into chunks of the supplied size
    // if the collection is not evenly divisible, the first chunk will be smaller
    func reverseChunked(by maxSize: Int) -> [SubSequence] {
        var chunkStartIndex = self.startIndex
        return stride(from: 0, to: self.count, by: maxSize).reversed().map { current in
            let distance = self.count - current
            let chunkEndIndex = self.index(self.startIndex, offsetBy: distance)
            defer { chunkStartIndex = chunkEndIndex }
            return self[chunkStartIndex..<chunkEndIndex]
        }
    }
}

