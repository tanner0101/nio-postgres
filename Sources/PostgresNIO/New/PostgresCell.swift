import NIOCore

struct PostgresCell: Equatable {
    var bytes: ByteBuffer?
    var dataType: PostgresDataType
    var format: PostgresFormat

    var columnName: String
    var columnIndex: Int

    init(bytes: ByteBuffer?, dataType: PostgresDataType, format: PostgresFormat, columnName: String, columnIndex: Int) {
        self.bytes = bytes
        self.dataType = dataType
        self.format = format

        self.columnName = columnName
        self.columnIndex = columnIndex
    }
}

extension PostgresCell {

    func decode<T: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(
        _: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #file,
        line: Int = #line
    ) throws -> T {
        var copy = self.bytes
        do {
            return try T.decodeRaw(
                from: &copy,
                type: self.dataType,
                format: self.format,
                context: context
            )
        } catch let code as PostgresCastingError.Code {
            throw PostgresCastingError(
                code: code,
                columnName: self.columnName,
                columnIndex: self.columnIndex,
                targetType: T.self,
                postgresType: self.dataType,
                postgresFormat: self.format,
                postgresData: copy,
                file: file,
                line: line
            )
        }
    }
}
