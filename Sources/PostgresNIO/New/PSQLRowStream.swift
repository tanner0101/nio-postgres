import NIOCore
import Logging

struct QueryResult {
    enum Value: Equatable {
        case noRows(PSQLRowStream.StatementSummary)
        case rowDescription([RowDescription.Column])
    }

    var value: Value

    var logger: Logger
}

// Thread safety is guaranteed in the RowStream through dispatching onto the NIO EventLoop.
final class PSQLRowStream: @unchecked Sendable {
    private typealias AsyncSequenceSource = NIOThrowingAsyncSequenceProducer<DataRow, Error, AdaptiveRowBuffer, PSQLRowStream>.Source

    enum StatementSummary: Equatable {
        case tag(String)
        case emptyResponse
    }
    
    enum Source {
        case stream([RowDescription.Column], PSQLRowsDataSource)
        case noRows(Result<StatementSummary, Error>)
    }
    
    let eventLoop: EventLoop
    let logger: Logger

    private enum BufferState {
        case streaming(buffer: CircularBuffer<DataRow>, dataSource: PSQLRowsDataSource)
        case finished(buffer: CircularBuffer<DataRow>, summary: StatementSummary)
        case failure(Error)
    }

    private enum DownstreamState {
        case waitingForConsumer(BufferState)
        case iteratingRows(onRow: (PostgresRow) throws -> (), EventLoopPromise<Void>, PSQLRowsDataSource)
        case waitingForAll([PostgresRow], EventLoopPromise<[PostgresRow]>, PSQLRowsDataSource)
        case consumed(Result<StatementSummary, Error>)
        case asyncSequence(AsyncSequenceSource, PSQLRowsDataSource, onFinish: @Sendable () -> ())
    }
    
    internal let rowDescription: [RowDescription.Column]
    private let lookupTable: [String: Int]
    private var downstreamState: DownstreamState
    
    init(
        source: Source,
        eventLoop: EventLoop,
        logger: Logger
    ) {
        let bufferState: BufferState
        switch source {
        case .stream(let rowDescription, let dataSource):
            self.rowDescription = rowDescription
            bufferState = .streaming(buffer: .init(), dataSource: dataSource)
        case .noRows(.success(let summary)):
            self.rowDescription = []
            bufferState = .finished(buffer: .init(), summary: summary)
        case .noRows(.failure(let error)):
            self.rowDescription = []
            bufferState = .failure(error)
        }
        
        self.downstreamState = .waitingForConsumer(bufferState)
        
        self.eventLoop = eventLoop
        self.logger = logger

        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }
    
    // MARK: Async Sequence

    func asyncSequence(onFinish: @escaping @Sendable () -> () = {}) -> PostgresRowSequence {
        self.eventLoop.preconditionInEventLoop()

        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        let producer = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: DataRow.self,
            failureType: Error.self,
            backPressureStrategy: AdaptiveRowBuffer(),
            finishOnDeinit: false,
            delegate: self
        )

        let source = producer.source
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            let yieldResult = source.yield(contentsOf: bufferedRows)
            self.downstreamState = .asyncSequence(source, dataSource, onFinish: onFinish)
            self.executeActionBasedOnYieldResult(yieldResult, source: dataSource)

        case .finished(let buffer, let summary):
            _ = source.yield(contentsOf: buffer)
            source.finish()
            onFinish()
            self.downstreamState = .consumed(.success(summary))

        case .failure(let error):
            source.finish(error)
            self.downstreamState = .consumed(.failure(error))
        }
        
        return PostgresRowSequence(producer.sequence, lookupTable: self.lookupTable, columns: self.rowDescription)
    }
    
    func demand() {
        if self.eventLoop.inEventLoop {
            self.demand0()
        } else {
            self.eventLoop.execute {
                self.demand0()
            }
        }
    }
    
    private func demand0() {
        switch self.downstreamState {
        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
            
        case .consumed:
            break
            
        case .asyncSequence(_, let dataSource, _):
            dataSource.request(for: self)
        }
    }
    
    func cancel() {
        if self.eventLoop.inEventLoop {
            self.cancel0()
        } else {
            self.eventLoop.execute {
                self.cancel0()
            }
        }
    }

    private func cancel0() {
        switch self.downstreamState {
        case .asyncSequence(_, let dataSource, let onFinish):
            self.downstreamState = .consumed(.failure(CancellationError()))
            dataSource.cancel(for: self)
            onFinish()

        case .consumed:
            return

        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
    }
    
    // MARK: Consume in array
        
    func all() -> EventLoopFuture<[PostgresRow]> {
        if self.eventLoop.inEventLoop {
            return self.all0()
        } else {
            return self.eventLoop.flatSubmit {
                self.all0()
            }
        }
    }
    
    private func all0() -> EventLoopFuture<[PostgresRow]> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            let promise = self.eventLoop.makePromise(of: [PostgresRow].self)
            let rows = bufferedRows.map { data in
                PostgresRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)
            return promise.futureResult
            
        case .finished(let buffer, let summary):
            let rows = buffer.map {
                PostgresRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            
            self.downstreamState = .consumed(.success(summary))
            return self.eventLoop.makeSucceededFuture(rows)
            
        case .failure(let error):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    // MARK: Consume on EventLoop
    
    func onRow(_ onRow: @Sendable @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.onRow0(onRow)
        } else {
            return self.eventLoop.flatSubmit {
                self.onRow0(onRow)
            }
        }
    }
    
    private func onRow0(_ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(var buffer, let dataSource):
            let promise = self.eventLoop.makePromise(of: Void.self)
            do {
                for data in buffer {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
                }
                
                buffer.removeAll()
                self.downstreamState = .iteratingRows(onRow: onRow, promise, dataSource)
                // immediately request more
                dataSource.request(for: self)
            } catch {
                self.downstreamState = .consumed(.failure(error))
                dataSource.cancel(for: self)
                promise.fail(error)
            }
            
            return promise.futureResult

        case .finished(let buffer, let summary):
            do {
                for data in buffer {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
                }
                
                self.downstreamState = .consumed(.success(summary))
                return self.eventLoop.makeSucceededVoidFuture()
            } catch {
                self.downstreamState = .consumed(.failure(error))
                return self.eventLoop.makeFailedFuture(error)
            }
            
        case .failure(let error):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    internal func noticeReceived(_ notice: PostgresBackendMessage.NoticeResponse) {
        self.logger.debug("Notice Received", metadata: [
            .notice: "\(notice)"
        ])
    }
    
    internal func receive(_ newRows: [DataRow]) {
        precondition(!newRows.isEmpty, "Expected to get rows!")
        self.eventLoop.preconditionInEventLoop()
        self.logger.trace("Row stream received rows", metadata: [
            .rowCount: "\(newRows.count)"
        ])
        
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(buffer: var buffer, dataSource: let dataSource)):
            buffer.append(contentsOf: newRows)
            self.downstreamState = .waitingForConsumer(.streaming(buffer: buffer, dataSource: dataSource))
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("How can new rows be received, if an end was already signalled?")

        case .iteratingRows(let onRow, let promise, let dataSource):
            do {
                for data in newRows {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
                }
                // immediately request more
                dataSource.request(for: self)
            } catch {
                dataSource.cancel(for: self)
                self.downstreamState = .consumed(.failure(error))
                promise.fail(error)
                return
            }

        case .waitingForAll(var rows, let promise, let dataSource):
            newRows.forEach { data in
                let row = PostgresRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
                rows.append(row)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)

        case .asyncSequence(let consumer, let source, _):
            let yieldResult = consumer.yield(contentsOf: newRows)
            self.executeActionBasedOnYieldResult(yieldResult, source: source)
            
        case .consumed(.success):
            preconditionFailure("How can we receive further rows, if we are supposed to be done")
            
        case .consumed(.failure):
            break
        }
    }
    
    internal func receive(completion result: Result<String, Error>) {
        self.eventLoop.preconditionInEventLoop()
        
        switch result {
        case .success(let commandTag):
            self.receiveEnd(commandTag)
        case .failure(let error):
            self.receiveError(error)
        }
    }
        
    private func receiveEnd(_ commandTag: String) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(buffer: let buffer, _)):
            self.downstreamState = .waitingForConsumer(.finished(buffer: buffer, summary: .tag(commandTag)))

        case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            promise.succeed(())
            
        case .waitingForAll(let rows, let promise, _):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            promise.succeed(rows)

        case .asyncSequence(let source, _, let onFinish):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            source.finish()
            onFinish()

        case .consumed(.success(.tag)), .consumed(.failure):
            break
        }
    }
        
    private func receiveError(_ error: Error) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming):
            self.downstreamState = .waitingForConsumer(.failure(error))
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            promise.fail(error)
            
        case .waitingForAll(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            promise.fail(error)

        case .asyncSequence(let consumer, _, let onFinish):
            self.downstreamState = .consumed(.failure(error))
            consumer.finish(error)
            onFinish()

        case .consumed(.success(.tag)), .consumed(.failure):
            break
        }
    }

    private func executeActionBasedOnYieldResult(_ yieldResult: AsyncSequenceSource.YieldResult, source: PSQLRowsDataSource) {
        self.eventLoop.preconditionInEventLoop()
        switch yieldResult {
        case .dropped:
            // ignore
            break

        case .produceMore:
            source.request(for: self)

        case .stopProducing:
            // ignore
            break
        }
    }
    
    var commandTag: String {
        guard case .consumed(.success(let consumed)) = self.downstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        switch consumed {
        case .tag(let tag):
            return tag
        case .emptyResponse:
            return ""
        }
    }
}

extension PSQLRowStream: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        self.demand()
    }

    func didTerminate() {
        self.cancel()
    }
}

protocol PSQLRowsDataSource {
    
    func request(for stream: PSQLRowStream)
    func cancel(for stream: PSQLRowStream)
    
}
