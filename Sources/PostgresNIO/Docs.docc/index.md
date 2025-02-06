# ``PostgresNIO``

@Metadata {
    @TitleHeading(Package)
}

🐘 Non-blocking, event-driven Swift client for PostgreSQL built on SwiftNIO.

## Overview

``PostgresNIO`` allows you to connect to, authorize with, query, and retrieve results from a 
PostgreSQL server. PostgreSQL is an open source relational database.

Use a ``PostgresConnection`` to create a connection to the PostgreSQL server. You can then use it to
run queries and prepared statements against the server. ``PostgresConnection`` also supports 
PostgreSQL's Listen & Notify API.

Developers, who don't want to manage connections themselves, can use the ``PostgresClient``, which 
offers the same functionality as ``PostgresConnection``. ``PostgresClient``
pools connections for rapid connection reuse and hides the complexities of connection 
management from the user, allowing developers to focus on their SQL queries. ``PostgresClient``
implements the `Service` protocol from Service Lifecycle allowing an easy adoption in Swift server
applications.

``PostgresNIO`` embraces Swift structured concurrency, offering async/await APIs which handle
task cancellation. The query interface makes use of backpressure to ensure that memory can not grow 
unbounded for queries that return thousands of rows.

``PostgresNIO`` runs efficiently on Linux and Apple platforms. On Apple platforms developers can 
configure ``PostgresConnection`` to use `Network.framework` as the underlying transport framework. 
 
## Topics

### Essentials

- ``PostgresClient``
- ``PostgresConnection``
- <doc:running-queries>

### Advanced

- <doc:coding>
- <doc:prepared-statement>
- <doc:listen>

### Errors

- ``PostgresError``
- ``PostgresDecodingError``
- ``PSQLError``

### Deprecations

- <doc:deprecated>

[SwiftNIO]: https://github.com/apple/swift-nio
[SwiftLog]: https://github.com/apple/swift-log
