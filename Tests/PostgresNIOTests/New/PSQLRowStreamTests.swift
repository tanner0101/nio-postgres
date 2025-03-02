import Atomics
import NIOCore
import Logging
import XCTest
@testable import PostgresNIO
import NIOCore
import NIOEmbedded

final class PSQLRowStreamTests: XCTestCase {
    let logger = Logger(label: "PSQLRowStreamTests")
    let eventLoop = EmbeddedEventLoop()

    func testEmptyStreamAndDrainDoesNotThrowErrorAfterConsumption() {
        let stream = PSQLRowStream(
            source: .noRows(.success(.tag("INSERT 0 1"))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        
        XCTAssertEqual(try stream.all().wait(), [])
        XCTAssertEqual(stream.commandTag, "INSERT 0 1")

        XCTAssertNoThrow(try stream.drain().wait())
    }

    func testFailedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let expectedError = PSQLError.serverClosedConnection(underlying: nil)

        XCTAssertThrowsError(try stream.all().wait()) {
            XCTAssertEqual($0 as? PSQLError, expectedError)
        }

        // Drain should work
        XCTAssertThrowsError(try stream.drain().wait()) {
            XCTAssertEqual($0 as? PSQLError, expectedError)
        }
    }

    func testGetArrayAfterStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        stream.receive(completion: .success("SELECT 2"))
        
        // attach consumer
        let future = stream.all()
        XCTAssertEqual(dataSource.hitDemand, 0) // TODO: Is this right?
        
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 2)
    }

    func testGetArrayBeforeStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")

        // attach consumer
        let future = stream.all()
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 2)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 3)

        stream.receive(completion: .success("SELECT 2"))

        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 6)
    }

    func testOnRowAfterStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2)
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 2")
    }

    func testOnRowThrowsErrorOnInitialBatch() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")],
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")],
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
            if expected == 1 {
                throw OnRowError(row: expected)
            }
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2) // one more than where we excited, because we already incremented
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? OnRowError, OnRowError(row: 1))
        }
    }

    func testOnRowBeforeStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2)
        XCTAssertEqual(dataSource.hitDemand, 1)
        
        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 4)
        XCTAssertEqual(dataSource.hitDemand, 2)
        
        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 6)
        XCTAssertEqual(dataSource.hitDemand, 3)
        
        stream.receive(completion: .success("SELECT 6"))
        
        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 6")
    }

    func testEmptyStreamDrainsSuccessfully() {
        let stream = PSQLRowStream(
            source: .noRows(.success(.tag("INSERT 0 1"))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        XCTAssertNoThrow(try stream.drain().wait())
        XCTAssertEqual(stream.commandTag, "INSERT 0 1")
    }

    func testDrainFailedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let expectedError = PSQLError.serverClosedConnection(underlying: nil)

        XCTAssertThrowsError(try stream.drain().wait()) {
            XCTAssertEqual($0 as? PSQLError, expectedError)
        }
    }

    func testDrainAfterStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        stream.receive(completion: .success("SELECT 2"))

        // attach consumer
        XCTAssertNoThrow(try stream.drain().wait())
        XCTAssertEqual(dataSource.hitDemand, 0) // TODO: Is this right?
    }

    func testDrainBeforeStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")

        // attach consumer
        let future = stream.drain()
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 2)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 3)

        stream.receive(completion: .success("SELECT 2"))

        XCTAssertNoThrow(try future.wait())
    }

    func testDrainBeforeStreamHasFinishedWhenThereIsAlreadyAConsumer() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")

        // attach consumers
        let allFuture = stream.all()
        XCTAssertEqual(dataSource.hitDemand, 1)
        let drainFuture = stream.drain()
        XCTAssertEqual(dataSource.hitDemand, 2)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 3)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 4)

        stream.receive(completion: .success("SELECT 2"))

        XCTAssertNoThrow(try drainFuture.wait())

        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try allFuture.wait())
        XCTAssertEqual(rows?.count, 6)
    }

    func testDrainBeforeStreamHasFinishedWhenThereIsAlreadyAnAsyncConsumer() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")

        // attach consumers
        let rowSequence = stream.asyncSequence()
        XCTAssertEqual(dataSource.hitDemand, 0)
        let drainFuture = stream.drain()
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 2)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 3)

        stream.receive(completion: .success("SELECT 2"))

        XCTAssertNoThrow(try drainFuture.wait())

        XCTAssertNoThrow {
            let rows = try stream.eventLoop.makeFutureWithTask {
                try? await rowSequence.collect()
            }.wait()
            XCTAssertEqual(dataSource.hitDemand, 4)
            XCTAssertEqual(rows?.count, 6)
        }
    }

    func makeColumnDescription(name: String, dataType: PostgresDataType, format: PostgresFormat) -> RowDescription.Column {
        RowDescription.Column(
            name: "test",
            tableOID: 123,
            columnAttributeNumber: 1,
            dataType: .text,
            dataTypeSize: -1,
            dataTypeModifier: 0,
            format: .binary
        )
    }
}

private struct OnRowError: Error, Equatable {
    var row: Int
}

class CountingDataSource: PSQLRowsDataSource {
    
    var hitDemand: Int = 0
    var hitCancel: Int = 0
    
    init() {}
    
    func cancel(for stream: PSQLRowStream) {
        self.hitCancel += 1
    }
    
    func request(for stream: PSQLRowStream) {
        self.hitDemand += 1
    }
}
