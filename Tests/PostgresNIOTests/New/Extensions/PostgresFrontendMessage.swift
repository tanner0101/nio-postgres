import NIOCore
import PostgresNIO

/// A wire message that is created by a Postgres client to be consumed by Postgres server.
///
/// All messages are defined in the official Postgres Documentation in the section
/// [Frontend/Backend Protocol – Message Formats](https://www.postgresql.org/docs/13/protocol-message-formats.html)
enum PostgresFrontendMessage: Equatable {

    struct Bind: Hashable {
        /// The name of the destination portal (an empty string selects the unnamed portal).
        var portalName: String

        /// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        var preparedStatementName: String

        /// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
        var parameterFormats: [PostgresFormat]

        /// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
        var parameters: [ByteBuffer?]

        var resultColumnFormats: [PostgresFormat]
    }

    struct Cancel: Equatable {
        /// The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same
        /// as any protocol version number.)
        static let requestCode: Int32 = 80877102

        /// The process ID of the target backend.
        let processID: Int32

        /// The secret key for the target backend.
        let secretKey: Int32
    }

    enum Close: Hashable {
        case preparedStatement(String)
        case portal(String)
    }

    enum Describe: Hashable {
        case preparedStatement(String)
        case portal(String)
    }

    struct Execute: Hashable {
        /// The name of the portal to execute (an empty string selects the unnamed portal).
        let portalName: String

        /// Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
        let maxNumberOfRows: Int32

        init(portalName: String, maxNumberOfRows: Int32 = 0) {
            self.portalName = portalName
            self.maxNumberOfRows = maxNumberOfRows
        }
    }

    struct Query: Hashable {
        /// The query string.
        let query: String
    }

    struct Parse: Hashable {
        /// The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
        let preparedStatementName: String

        /// The query string to be parsed.
        let query: String

        /// The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
        let parameters: [PostgresDataType]
    }

    struct Password: Hashable {
        let value: String
    }

    struct SASLInitialResponse: Hashable {

        let saslMechanism: String
        let initialData: [UInt8]

        /// Creates a new `SSLRequest`.
        init(saslMechanism: String, initialData: [UInt8]) {
            self.saslMechanism = saslMechanism
            self.initialData = initialData
        }
    }

    struct SASLResponse: Hashable {
        var data: [UInt8]

        /// Creates a new `SSLRequest`.
        init(data: [UInt8]) {
            self.data = data
        }
    }

    /// A message asking the PostgreSQL server if TLS is supported
    /// For more info, see https://www.postgresql.org/docs/10/static/protocol-flow.html#id-1.10.5.7.11
    struct SSLRequest: Hashable {
        /// The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5679 in the least significant 16 bits.
        static let requestCode: Int32 = 80877103
    }

    struct Startup: Equatable {
        static let versionThree: Int32 = 0x00_03_00_00

        /// Creates a `Startup` with "3.0" as the protocol version.
        static func versionThree(parameters: Parameters) -> Startup {
            return .init(protocolVersion: Self.versionThree, parameters: parameters)
        }

        /// The protocol version number. The most significant 16 bits are the major
        /// version number (3 for the protocol described here). The least significant
        /// 16 bits are the minor version number (0 for the protocol described here).
        var protocolVersion: Int32

        /// The protocol version number is followed by one or more pairs of parameter
        /// name and value strings. A zero byte is required as a terminator after
        /// the last name/value pair. `user` is required, others are optional.
        struct Parameters: Equatable {
            enum Replication {
                case `true`
                case `false`
                case database
            }

            /// The database user name to connect as. Required; there is no default.
            var user: String

            /// The database to connect to. Defaults to the user name.
            var database: String?

            /// Command-line arguments for the backend. (This is deprecated in favor
            /// of setting individual run-time parameters.) Spaces within this string are
            /// considered to separate arguments, unless escaped with a
            /// backslash (\); write \\ to represent a literal backslash.
            var options: [(String, String)]

            /// Used to connect in streaming replication mode, where a small set of
            /// replication commands can be issued instead of SQL statements. Value
            /// can be true, false, or database, and the default is false.
            var replication: Replication

            static func ==(lhs: Self, rhs: Self) -> Bool {
                guard lhs.user == rhs.user
                        && lhs.database == rhs.database
                        && lhs.replication == rhs.replication
                        && lhs.options.count == rhs.options.count
                else {
                    return false
                }

                var lhsIterator = lhs.options.makeIterator()
                var rhsIterator = rhs.options.makeIterator()

                while let lhsNext = lhsIterator.next(), let rhsNext = rhsIterator.next() {
                    guard lhsNext.0 == rhsNext.0 && lhsNext.1 == rhsNext.1 else {
                        return false
                    }
                }
                return true
            }

        }

        var parameters: Parameters
    }

    case bind(Bind)
    case cancel(Cancel)
    case close(Close)
    case describe(Describe)
    case execute(Execute)
    case flush
    case parse(Parse)
    case password(Password)
    case saslInitialResponse(SASLInitialResponse)
    case saslResponse(SASLResponse)
    case sslRequest
    case query(Query)
    case sync
    case startup(Startup)
    case terminate
    
    enum ID: UInt8, Equatable {
        
        case bind
        case close
        case describe
        case execute
        case flush
        case parse
        case password
        case saslInitialResponse
        case saslResponse
        case query
        case sync
        case terminate
        
        init?(rawValue: UInt8) {
            switch rawValue {
            case UInt8(ascii: "B"):
                self = .bind
            case UInt8(ascii: "C"):
                self = .close
            case UInt8(ascii: "D"):
                self = .describe
            case UInt8(ascii: "E"):
                self = .execute
            case UInt8(ascii: "H"):
                self = .flush
            case UInt8(ascii: "P"):
                self = .parse
            case UInt8(ascii: "p"):
                self = .password
            case UInt8(ascii: "p"):
                self = .saslInitialResponse
            case UInt8(ascii: "p"):
                self = .saslResponse
            case UInt8(ascii: "Q"):
                self = .query
            case UInt8(ascii: "S"):
                self = .sync
            case UInt8(ascii: "X"):
                self = .terminate
            default:
                return nil
            }
        }

        var rawValue: UInt8 {
            switch self {
            case .bind:
                return UInt8(ascii: "B")
            case .close:
                return UInt8(ascii: "C")
            case .describe:
                return UInt8(ascii: "D")
            case .execute:
                return UInt8(ascii: "E")
            case .flush:
                return UInt8(ascii: "H")
            case .parse:
                return UInt8(ascii: "P")
            case .password:
                return UInt8(ascii: "p")
            case .saslInitialResponse:
                return UInt8(ascii: "p")
            case .saslResponse:
                return UInt8(ascii: "p")
            case .query:
                return UInt8(ascii: "Q")
            case .sync:
                return UInt8(ascii: "S")
            case .terminate:
                return UInt8(ascii: "X")
            }
        }
    }
}

extension PostgresFrontendMessage {
    
    var id: ID {
        switch self {
        case .bind:
            return .bind
        case .cancel:
            preconditionFailure("Cancel messages don't have an identifier")
        case .close:
            return .close
        case .describe:
            return .describe
        case .execute:
            return .execute
        case .flush:
            return .flush
        case .parse:
            return .parse
        case .password:
            return .password
        case .saslInitialResponse:
            return .saslInitialResponse
        case .saslResponse:
            return .saslResponse
        case .sslRequest:
            preconditionFailure("SSL requests don't have an identifier")
        case .startup:
            preconditionFailure("Startup messages don't have an identifier")
        case .query:
            return .query
        case .sync:
            return .sync
        case .terminate:
            return .terminate

        }
    }
}
