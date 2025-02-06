# Deprecations

`PostgresNIO` follows SemVer 2.0.0. Learn which APIs are considered deprecated and how to migrate to
their replacements.

``PostgresNIO`` reached 1.0 in April 2020. Since then the maintainers have been hard at work to
guarantee API stability. However as the Swift and Swift on server ecosystem have matured approaches
have changed. The introduction of structured concurrency changed what developers expect from a
modern Swift library. Because of this ``PostgresNIO`` added various APIs that embrace the new Swift
patterns. This means however, that PostgresNIO still offers APIs that have fallen out of favor.
Those are documented here. All those APIs will be removed once the maintainers release the next
major version. The maintainers recommend all adopters to move off those APIs sooner rather than
later.

## Topics

### Migrate of deprecated APIs

- <doc:migrations>

### Deprecated APIs

These types are already deprecated or will be deprecated in the near future. All of them will be
removed from the public API with the next major release.

- ``PostgresDatabase``
- ``PostgresData``
- ``PostgresDataConvertible``
- ``PostgresQueryResult``
- ``PostgresJSONCodable``
- ``PostgresJSONBCodable``
- ``PostgresMessageEncoder``
- ``PostgresMessageDecoder``
- ``PostgresRequest``
- ``PostgresMessage``
- ``PostgresMessageType``
- ``PostgresFormatCode``
- ``PostgresListenContext``
- ``PreparedQuery``
- ``SASLAuthenticationManager``
- ``SASLAuthenticationMechanism``
- ``SASLAuthenticationError``
- ``SASLAuthenticationStepResult``
