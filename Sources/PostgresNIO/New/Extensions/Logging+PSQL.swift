import Logging

@usableFromInline
enum PSQLConnection {}

extension PSQLConnection {
    @usableFromInline
    enum LoggerMetaDataKey: String {
        case connectionID = "psql_connection_id"
        case query = "psql_query"
        case name = "psql_name"
        case error = "psql_error"
        case notice = "psql_notice"
        case binds = "psql_binds"
        case commandTag = "psql_command_tag"
        
        case connectionState = "psql_connection_state"
        case connectionAction = "psql_connection_action"
        case message = "psql_message"
        case messageID = "psql_message_id"
        case messagePayload = "psql_message_payload"
        case rowCount = "psql_row_count"

        case database = "psql_database"
        case username = "psql_username"
        
        case userEvent = "psql_user_event"
    }
}

@usableFromInline
struct PSQLLoggingMetadata: ExpressibleByDictionaryLiteral {
    @usableFromInline
    typealias Key = PSQLConnection.LoggerMetaDataKey
    @usableFromInline
    typealias Value = Logger.MetadataValue
    
    @usableFromInline var _baseRepresentation: Logger.Metadata
    
    @usableFromInline
    init(dictionaryLiteral elements: (PSQLConnection.LoggerMetaDataKey, Logger.MetadataValue)...) {
        let values = elements.lazy.map { (key, value) -> (String, Self.Value) in
            (key.rawValue, value)
        }
        
        self._baseRepresentation = Logger.Metadata(uniqueKeysWithValues: values)
    }
    
    @usableFromInline
    subscript(postgresLoggingKey loggingKey: PSQLConnection.LoggerMetaDataKey) -> Logger.Metadata.Value? {
        get {
            return self._baseRepresentation[loggingKey.rawValue]
        }
        set {
            self._baseRepresentation[loggingKey.rawValue] = newValue
        }
    }
    
    @inlinable
    var representation: Logger.Metadata {
        self._baseRepresentation
    }
}


extension Logger {
    
    static let psqlNoOpLogger = Logger(label: "psql_do_not_log", factory: { _ in SwiftLogNoOpLogHandler() })
    
    @usableFromInline
    subscript(postgresMetadataKey metadataKey: PSQLConnection.LoggerMetaDataKey) -> Logger.Metadata.Value? {
        get {
            return self[metadataKey: metadataKey.rawValue]
        }
        set {
            self[metadataKey: metadataKey.rawValue] = newValue
        }
    }
    
}

extension Logger {
    
    /// See `Logger.trace(_:metadata:source:file:function:line:)`
    @usableFromInline
    func trace(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PSQLLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .trace, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.debug(_:metadata:source:file:function:line:)`
    @usableFromInline
    func debug(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PSQLLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .debug, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.info(_:metadata:source:file:function:line:)`
    @usableFromInline
    func info(_ message: @autoclosure () -> Logger.Message,
              metadata: @autoclosure () -> PSQLLoggingMetadata,
              source: @autoclosure () -> String? = nil,
              file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .info, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.notice(_:metadata:source:file:function:line:)`
    @usableFromInline
    func notice(_ message: @autoclosure () -> Logger.Message,
                metadata: @autoclosure () -> PSQLLoggingMetadata,
                source: @autoclosure () -> String? = nil,
                file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .notice, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.warning(_:metadata:source:file:function:line:)`
    @usableFromInline
    func warning(_ message: @autoclosure () -> Logger.Message,
                 metadata: @autoclosure () -> PSQLLoggingMetadata,
                 source: @autoclosure () -> String? = nil,
                 file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .warning, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.error(_:metadata:source:file:function:line:)`
    @usableFromInline
    func error(_ message: @autoclosure () -> Logger.Message,
               metadata: @autoclosure () -> PSQLLoggingMetadata,
               source: @autoclosure () -> String? = nil,
               file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .error, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }

    /// See `Logger.critical(_:metadata:source:file:function:line:)`
    @usableFromInline
    func critical(_ message: @autoclosure () -> Logger.Message,
                  metadata: @autoclosure () -> PSQLLoggingMetadata,
                  source: @autoclosure () -> String? = nil,
                  file: String = #fileID, function: String = #function, line: UInt = #line) {
        self.log(level: .critical, message(), metadata: metadata().representation, source: source(), file: file, function: function, line: line)
    }
}

