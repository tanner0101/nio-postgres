# Running Statements

Interact with the PostgreSQL database by running Queries.

## Overview

You run statements on a PostgreSQL database, by:

1. Creating a connection to the Postgres database by creating a ``PostgresClient`` or 
    ``PostgresConnection``

You interact with the Postgres database by running SQL [Queries]. 

A ``PostgresQuery`` consists out of the ``PostgresQuery/sql`` statement and the 
``PostgresQuery/binds``, which are the parameters for the sql statement.

Users should create ``PostgresQuery``s in most cases through String interpolation:

@Snippet(path: "postgres-nio/Snippets/PostgresQuery", slice: "select1")




## Topics

- ``PostgresQuery``
- ``PostgresBindings``
- ``PostgresRow``
- ``PostgresRowSequence``
- ``PostgresRandomAccessRow``
- ``PostgresCell``
- ``PostgresQueryMetadata``

[Queries]: doc:PostgresQuery
[`ExpressibleByStringInterpolation`]: https://developer.apple.com/documentation/swift/expressiblebystringinterpolation
