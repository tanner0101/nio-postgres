<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/1130717/259709891-64d4c78b-1cd1-4446-8746-d3a009992811.png">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/1130717/259710040-d79ee9eb-b5d9-4a82-a894-3eb5ef366c1f.png">
  <img src="https://user-images.githubusercontent.com/1130717/259710040-d79ee9eb-b5d9-4a82-a894-3eb5ef366c1f.png" height="96" alt="PostgresNIO">
</picture>
<br>
<br>
<a href="https://api.vapor.codes/postgresnio/documentation/postgresnio/">
    <img src="https://design.vapor.codes/images/readthedocs.svg" alt="Documentation">
</a>
<a href="LICENSE">
    <img src="https://design.vapor.codes/images/mitlicense.svg" alt="MIT License">
</a>
<a href="https://github.com/vapor/postgres-nio/actions/workflows/test.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/vapor/postgres-nio/test.yml?event=push&style=plastic&logo=github&label=tests&logoColor=%23ccc" alt="Continuous Integration">
</a>
<a href="https://swift.org">
    <img src="https://design.vapor.codes/images/swift58up.svg" alt="Swift 5.8+">
</a>
<a href="https://www.swift.org/sswg/incubation-process.html">
    <img src="https://design.vapor.codes/images/sswg-graduated.svg" alt="SSWG Incubation Level: Graduated">
</a>
</p>

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on [SwiftNIO].

Features:

- A [`PostgresConnection`] which allows you to connect to, authorize with, query, and retrieve results from a PostgreSQL server
- A [`PostgresClient`] which pools and manages connections 
- An async/await interface that supports backpressure 
- Automatic conversions between Swift primitive types and the Postgres wire format
- Integrated with the Swift server ecosystem, including use of [SwiftLog] and [ServiceLifecycle].
- Designed to run efficiently on all supported platforms (tested extensively on Linux and Darwin systems)
- Support for `Network.framework` when available (e.g. on Apple platforms)
- Supports running on Unix Domain Sockets

## API Docs

Check out the [PostgresNIO API docs][Documentation] for a 
detailed look at all of the classes, structs, protocols, and more.

## Getting started

Interested in an example? We prepared a simple [Birthday example](https://github.com/vapor/postgres-nio/blob/main/Snippets/Birthdays.swift)
in the Snippets folder.

#### Adding the dependency

Add `PostgresNIO` as dependency to your `Package.swift`:

```swift
  dependencies: [
    .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
    ...
  ]
```

Add `PostgresNIO` to the target you want to use it in:
```swift
  targets: [
    .target(name: "MyFancyTarget", dependencies: [
      .product(name: "PostgresNIO", package: "postgres-nio"),
    ])
  ]
```

#### Creating a client

To create a [`PostgresClient`], which pools connections for you, first create a configuration object:

```swift
import PostgresNIO

let config = PostgresClient.Configuration(
  host: "localhost",
  port: 5432,
  username: "my_username",
  password: "my_password",
  database: "my_database",
  tls: .disable
)
```

Next you can create you client with it:
```swift
let client = PostgresClient(configuration: config)
```

Once you have create your client, you must [`run()`] it:
```swift
await withTaskGroup(of: Void.self) { taskGroup in
  taskGroup.addTask {
    await client.run() // !important
  }

  // You can use the client while the `client.run()` method is not cancelled.

  // To shutdown the client, cancel its run method, by cancelling the taskGroup.
  taskGroup.cancelAll()
}
```

#### Querying

Once a client is running, queries can be sent to the server. This is straightforward:

```swift
let rows = try await client.query("SELECT id, username, birthday FROM users")
```

The query will return a [`PostgresRowSequence`], which is an AsyncSequence of [`PostgresRow`]s. 
The rows can be iterated one-by-one: 

```swift
for try await row in rows {
  // do something with the row
}
```

#### Decoding from PostgresRow

However, in most cases it is much easier to request a row's fields as a set of Swift types:

```swift
for try await (id, username, birthday) in rows.decode((Int, String, Date).self) {
  // do something with the datatypes.
}
```

A type must implement the [`PostgresDecodable`] protocol in order to be decoded from a row. PostgresNIO provides default implementations for most of Swift's builtin types, as well as some types provided by Foundation:

- `Bool`
- `Bytes`, `Data`, `ByteBuffer`
- `Date`
- `UInt8`, `Int16`, `Int32`, `Int64`, `Int`
- `Float`, `Double`
- `String`
- `UUID`

#### Querying with parameters

Sending parameterized queries to the database is also supported (in the coolest way possible):

```swift
let id = 1
let username = "fancyuser"
let birthday = Date()
try await client.query("""
  INSERT INTO users (id, username, birthday) VALUES (\(id), \(username), \(birthday))
  """, 
  logger: logger
)
```

While this looks at first glance like a classic case of [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) 😱, PostgresNIO's API ensures that this usage is safe. The first parameter of the [`query(_:logger:)`] method is not a plain `String`, but a [`PostgresQuery`], which implements Swift's `ExpressibleByStringInterpolation` protocol. PostgresNIO uses the literal parts of the provided string as the SQL query and replaces each interpolated value with a parameter binding. Only values which implement the [`PostgresEncodable`] protocol may be interpolated in this way. As with [`PostgresDecodable`], PostgresNIO provides default implementations for most common types.

Some queries do not receive any rows from the server (most often `INSERT`, `UPDATE`, and `DELETE` queries with no `RETURNING` clause, not to mention most DDL queries). To support this, the [`query(_:logger:)`] method is marked `@discardableResult`, so that the compiler does not issue a warning if the return value is not used. 

## Security

Please see [SECURITY.md] for details on the security process.

[SSWG Incubation]: https://github.com/swift-server/sswg/blob/main/process/incubation.md#graduated-level
[Documentation]: https://api.vapor.codes/postgresnio/documentation/postgresnio
[Team Chat]: https://discord.gg/vapor
[MIT License]: LICENSE
[Continuous Integration]: https://github.com/vapor/postgres-nio/actions
[Swift 5.8]: https://swift.org
[Security.md]: https://github.com/vapor/.github/blob/main/SECURITY.md

[`PostgresConnection`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresconnection
[`PostgresClient`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresclient
[`run()`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresclient/run()
[`query(_:logger:)`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresconnection/query(_:logger:file:line:)-9mkfn
[`PostgresQuery`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresquery
[`PostgresRow`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresrow
[`PostgresRowSequence`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresrowsequence
[`PostgresDecodable`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresdecodable
[`PostgresEncodable`]: https://api.vapor.codes/postgresnio/documentation/postgresnio/postgresencodable
[SwiftNIO]: https://github.com/apple/swift-nio
[PostgresKit]: https://github.com/vapor/postgres-kit
[SwiftLog]: https://github.com/apple/swift-log
[ServiceLifecycle]: https://github.com/swift-server/swift-service-lifecycle
[`Logger`]: https://apple.github.io/swift-log/docs/current/Logging/Structs/Logger.html
