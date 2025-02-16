import Synchronization

public struct LCPConnectionID: Sendable, Hashable {
    @usableFromInline
    var poolID: UInt8
    @usableFromInline
    var connectionID: UInt32
}

@available(macOS 9999.0, *)
public final class LCPConnectionIDGenerator: ConnectionIDGeneratorProtocol {
    static let globalGenerator = ConnectionIDGenerator()

    private let atomic: Atomic<UInt32>
    private let poolID: UInt8

    public init(poolID: UInt8) {
        self.poolID = poolID
        self.atomic = .init(0)
    }

    public func next() -> LCPConnectionID {
        let (connectionID, _) = self.atomic.wrappingAdd(1, ordering: .relaxed)
        return LCPConnectionID(poolID: self.poolID, connectionID: connectionID)
    }
}

@available(macOS 9999.0, *)
public final class LargeConnectionPool<
    Connection: PooledConnection,
    Request: ConnectionRequestProtocol,
    RequestID: Hashable & Sendable,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    ObservabilityDelegate: ConnectionPoolObservabilityDelegate,
    Clock: _Concurrency.Clock
>: Sendable where
    Connection.ID == LCPConnectionID,
    Request.Connection == Connection,
    Request.ID == RequestID,
    KeepAliveBehavior.Connection == Connection,
    ObservabilityDelegate.ConnectionID == LCPConnectionID,
    Clock.Duration == Duration
{
    @usableFromInline
    typealias Pool = ConnectionPool<
        Connection,
        LCPConnectionID,
        LCPConnectionIDGenerator,
        Request,
        RequestID,
        KeepAliveBehavior,
        ObservabilityDelegate,
        Clock
    >

    public typealias ConnectionFactory = @Sendable (LCPConnectionID, ConnectionPool<Connection, LCPConnectionID, LCPConnectionIDGenerator, Request, RequestID, KeepAliveBehavior, ObservabilityDelegate, Clock>) async throws -> ConnectionAndMetadata<Connection>

    @usableFromInline
    let poolForExecutor: [UnownedTaskExecutor: Pool]
    @usableFromInline
    let pools: [Pool]
    @usableFromInline
    let connectionIDGenerator = ConnectionIDGenerator()

    init(
        executors: some Collection<any TaskExecutor>,
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        requestType: Request.Type,
        keepAliveBehavior: KeepAliveBehavior,
        observabilityDelegate: ObservabilityDelegate,
        clock: Clock,
        connectionFactory: @escaping ConnectionFactory
    ) {
        var poolForExecutor = [UnownedTaskExecutor: Pool]()
        var pools = [Pool]()
        poolForExecutor.reserveCapacity(executors.count)
        pools.reserveCapacity(executors.count)
        var poolID: UInt8 = 0
        for executor in executors {
            let pool = Pool(
                configuration: configuration,
                idGenerator: LCPConnectionIDGenerator(poolID: poolID),
                requestType: requestType,
                keepAliveBehavior: keepAliveBehavior,
                observabilityDelegate: observabilityDelegate,
                clock: clock,
                connectionFactory: connectionFactory
            )
            poolForExecutor[executor.asUnownedTaskExecutor()] = pool
            pools.append(pool)
        }
        self.poolForExecutor = poolForExecutor
        self.pools = pools
    }

    @inlinable
    public func leaseConnection(_ request: Request) {
        withUnsafeCurrentTask { task in
            if let executor = task?.unownedTaskExecutor {
                if let pool = self.poolForExecutor[executor] {
                    return pool.leaseConnection(request)
                }
            }

            // TODO: Find the first pool that has room and don't use a random one
            let pool = self.pools.values.randomElement()!
            pool.leaseConnection(request)
        }
    }

    public func run() async {
        await withTaskGroup(of: Void.self) { taskGroup in
            for pool in self.pools {
                taskGroup.addTask {
                    await pool.run()
                }
            }
        }
    }
}
