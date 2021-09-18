import NIOCore
import Logging

final class PSQLRowStream {
    
    let eventLoop: EventLoop
    let logger: Logger
    
    private enum UpstreamState {
        case streaming(next: () -> EventLoopFuture<StateMachineStreamNextResult>, cancel: () -> ())
        case finished(remaining: CircularBuffer<PSQLBackendMessage.DataRow>, commandTag: String)
        case failure(Error)
        case consumed(Result<String, Error>)
    }
    
    private enum DownstreamState {
        case waitingForNext
        case consuming
    }
    
    internal let rowDescription: [PSQLBackendMessage.RowDescription.Column]
    private let lookupTable: [String: Int]
    private var upstreamState: UpstreamState
    private var downstreamState: DownstreamState
    private let jsonDecoder: PSQLJSONDecoder
    
    init(rowDescription: [PSQLBackendMessage.RowDescription.Column],
         queryContext: ExtendedQueryContext,
         eventLoop: EventLoop,
         cancel: @escaping () -> (),
         next: @escaping () -> EventLoopFuture<StateMachineStreamNextResult>)
    {
        self.upstreamState = .streaming(next: next, cancel: cancel)
        self.downstreamState = .consuming
        
        self.eventLoop = eventLoop
        self.logger = queryContext.logger
        self.jsonDecoder = queryContext.jsonDecoder
        
        self.rowDescription = rowDescription
        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }
    
    func next() -> EventLoopFuture<PSQLRow?> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.next()
            }
        }
        
        assert(self.downstreamState == .consuming)
        
        switch self.upstreamState {
        case .streaming(let upstreamNext, _):
            return upstreamNext().map { payload -> PSQLRow? in
                self.downstreamState = .consuming
                switch payload {
                case .row(let data):
                    return PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                case .complete(var buffer, let commandTag):
                    if let data = buffer.popFirst() {
                        self.upstreamState = .finished(remaining: buffer, commandTag: commandTag)
                        return PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                    }
                    
                    self.upstreamState = .consumed(.success(commandTag))
                    return nil
                }
            }.flatMapErrorThrowing { error in
                // if we have an error upstream that, we pass through here, we need to set
                // our internal state
                self.upstreamState = .consumed(.failure(error))
                throw error
            }
            
        case .finished(remaining: var buffer, commandTag: let commandTag):
            self.downstreamState = .consuming
            if let data = buffer.popFirst() {
                self.upstreamState = .finished(remaining: buffer, commandTag: commandTag)
                let row = PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                return self.eventLoop.makeSucceededFuture(row)
            }
            
            self.upstreamState = .consumed(.success(commandTag))
            return self.eventLoop.makeSucceededFuture(nil)
            
        case .failure(let error):
            self.upstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
            
        case .consumed:
            preconditionFailure("We already signaled, that the stream has completed, why are we asked again?")
        }
    }
    
    internal func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) {
        self.logger.debug("Notice Received", metadata: [
            .notice: "\(notice)"
        ])
    }
    
    internal func finalForward(_ finalForward: Result<(CircularBuffer<PSQLBackendMessage.DataRow>, commandTag: String), PSQLError>?) {
        switch finalForward {
        case .some(.success((let buffer, commandTag: let commandTag))):
            guard case .streaming = self.upstreamState else {
                preconditionFailure("Expected to be streaming up until now")
            }
            self.upstreamState = .finished(remaining: buffer, commandTag: commandTag)
        case .some(.failure(let error)):
            guard case .streaming = self.upstreamState else {
                preconditionFailure("Expected to be streaming up until now")
            }
            self.upstreamState = .failure(error)
        case .none:
            switch self.upstreamState {
            case .consumed:
                break
            case .finished:
                break
            case .failure:
                preconditionFailure("Invalid state")
            case .streaming:
                preconditionFailure("Invalid state")
            }
        }
    }
    
    func cancel() {
        guard case .streaming(_, let cancel) = self.upstreamState else {
            // We don't need to cancel any upstream resource. All needed data is already
            // included in this 
            return
        }
        
        cancel()
    }
    
    var commandTag: String {
        guard case .consumed(.success(let commandTag)) = self.upstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        return commandTag
    }
        
    func onRow(_ onRow: @escaping (PSQLRow) -> EventLoopFuture<Void>) -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        func consumeNext(promise: EventLoopPromise<Void>) {
            self.next().whenComplete { result in
                switch result {
                case .success(.some(let row)):
                    onRow(row).whenComplete { result in
                        switch result {
                        case .success:
                            consumeNext(promise: promise)
                        case .failure(let error):
                            promise.fail(error)
                        }
                    }
                case .success(.none):
                    promise.succeed(Void())
                case .failure(let error):
                    promise.fail(error)
                }
            }
        }
        
        consumeNext(promise: promise)
        
        return promise.futureResult
    }
}
