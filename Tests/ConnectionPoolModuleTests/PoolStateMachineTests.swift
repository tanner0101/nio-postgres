import XCTest
@testable import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest,
    MockRequest.ID,
    MockTimerCancellationToken
>

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PoolStateMachineTests: XCTestCase {

    func testConnectionsAreCreatedAndParkedOnStartup() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 2
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        let connection1 = MockConnection(id: 0)
        let connection2 = MockConnection(id: 1)

        do {
            let requests = stateMachine.refillConnections()
            XCTAssertEqual(requests.count, 2)
            let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
            let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(10))
            let connection1KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer)
            XCTAssertEqual(createdAction1.request, .none)
            XCTAssertEqual(createdAction1.connection, .scheduleTimers([connection1KeepAliveTimer]))

            XCTAssertEqual(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: connection1KeepAliveTimerCancellationToken), .none)

            let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
            let connection2KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(10))
            let connection2KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer)
            XCTAssertEqual(createdAction2.request, .none)
            XCTAssertEqual(createdAction2.connection, .scheduleTimers([connection2KeepAliveTimer]))
            XCTAssertEqual(stateMachine.timerScheduled(connection2KeepAliveTimer, cancelContinuation: connection2KeepAliveTimerCancellationToken), .none)
        }
    }

    func testConnectionsNoKeepAliveRun() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(5)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        let connection1 = MockConnection(id: 0)

        // refill pool to at least one connection
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .none)
        XCTAssertEqual(createdAction1.connection, .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest1.request, .leaseConnection(.init(element: request1), connection1))

        // release connection 1
        XCTAssertEqual(stateMachine.releaseConnection(connection1, streams: 1), .none())

        // lease connection 1
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest2.request, .leaseConnection(.init(element: request2), connection1))

        // request connection while none is available
        let request3 = MockRequest()
        let leaseRequest3 = stateMachine.leaseConnection(request3)
        XCTAssertEqual(leaseRequest3.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest3.request, .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request3), connection2))
        XCTAssertEqual(createdAction2.connection, .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        XCTAssertEqual(
            stateMachine.releaseConnection(connection2, streams: 1),
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        XCTAssertEqual(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken), .none)
        XCTAssertEqual(stateMachine.timerTriggered(connection2IdleTimer), .init(request: .none, connection: .closeConnection(connection2, [connection2IdleTimerCancellationToken])))
    }

    func testOnlyOverflowConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 0
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)

        // request connection while none is available
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest1.request, .leaseConnection(.init(element: request2), connection1))
        XCTAssertEqual(releaseRequest1.connection, .none)

        // connection 2 comes up and should be closed right away
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .none)
        XCTAssertEqual(createdAction2.connection, .closeConnection(connection2, []))
        XCTAssertEqual(stateMachine.connectionClosed(connection2), .none())

        // release connection 1 should be closed as well
        let releaseRequest2 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest2.request, .none)
        XCTAssertEqual(releaseRequest2.connection, .closeConnection(connection1, []))

        let shutdownAction = stateMachine.triggerForceShutdown()
        XCTAssertEqual(shutdownAction.request, .failRequests(.init(), .poolShutdown))
        XCTAssertEqual(shutdownAction.connection, .shutdown(.init()))
    }

    func testDemandConnectionIsMadePermanentIfPermanentIsClose() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        let connection1 = MockConnection(id: 0)

        // refill pool to at least one connection
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .none)
        XCTAssertEqual(createdAction1.connection, .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest1.request, .leaseConnection(.init(element: request1), connection1))

        // request connection while none is available
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request2), connection2))
        XCTAssertEqual(createdAction2.connection, .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        XCTAssertEqual(
            stateMachine.releaseConnection(connection2, streams: 1),
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        XCTAssertEqual(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken), .none)

        // connection 1 is dropped
        XCTAssertEqual(stateMachine.connectionClosed(connection1), .init(request: .none, connection: .cancelTimers([connection2IdleTimerCancellationToken])))
    }

    func testReleaseLoosesRaceAgainstClosed() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)

        // connection got closed
        let closedAction = stateMachine.connectionClosed(connection1)
        XCTAssertEqual(closedAction.connection, .none)
        XCTAssertEqual(closedAction.request, .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest1.request, .none)
        XCTAssertEqual(releaseRequest1.connection, .none)
    }

    func testKeepAliveOnClosingConnection() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)
        _ = stateMachine.releaseConnection(connection1, streams: 1)

        // trigger keep alive
        let keepAliveAction1 = stateMachine.connectionKeepAliveTimerTriggered(connection1.id)
        XCTAssertEqual(keepAliveAction1.connection, .runKeepAlive(connection1, nil))

        // fail keep alive and cause closed
        let keepAliveFailed1 = stateMachine.connectionKeepAliveFailed(connection1.id)
        XCTAssertEqual(keepAliveFailed1.connection, .closeConnection(connection1, []))
        connection1.closeIfClosing()

        // request connection while none exists anymore
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // make connection 2
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request2), connection2))
        XCTAssertEqual(createdAction2.connection, .none)
        _ = stateMachine.releaseConnection(connection2, streams: 1)

        // trigger keep alive while connection is still open
        let keepAliveAction2 = stateMachine.connectionKeepAliveTimerTriggered(connection2.id)
        XCTAssertEqual(keepAliveAction2.connection, .runKeepAlive(connection2, nil))

        // close connection in the middle of keep alive
        connection2.close()
        connection2.closeIfClosing()

        // fail keep alive and cause closed
        let keepAliveFailed2 = stateMachine.connectionKeepAliveFailed(connection2.id)
        XCTAssertEqual(keepAliveFailed2.connection, .closeConnection(connection2, []))
    }

    func testConnectionIsEstablishedAfterFailedKeepAliveIfNotEnoughConnectionsLeft() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 1)

        // one connection should exist
        let request = MockRequest()
        let leaseRequest = stateMachine.leaseConnection(request)
        XCTAssertEqual(leaseRequest.connection, .none)
        XCTAssertEqual(leaseRequest.request, .none)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        XCTAssertEqual(createdAction.request, .leaseConnection(.init(element: request), connection))
        XCTAssertEqual(createdAction.connection, .none)
        _ = stateMachine.releaseConnection(connection, streams: 1)

        // trigger keep alive
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(connection.id)
        XCTAssertEqual(keepAliveAction.connection, .runKeepAlive(connection, nil))

        // fail keep alive, cause closed and make new connection
        let keepAliveFailed = stateMachine.connectionKeepAliveFailed(connection.id)
        XCTAssertEqual(keepAliveFailed.connection, .closeConnection(connection, []))
        let connectionClosed = stateMachine.connectionClosed(connection)
        XCTAssertEqual(connectionClosed.connection, .makeConnection(.init(connectionID: 1), []))
        connection.closeIfClosing()
        let establishAction = stateMachine.connectionEstablished(.init(id: 1), maxStreams: 1)
        XCTAssertEqual(establishAction.request, .none)
        guard case .scheduleTimers(let timers) = establishAction.connection else { return XCTFail("Unexpected connection action") }
        XCTAssertEqual(timers, [.init(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: configuration.keepAliveDuration!)])
    }

    func testConnectionsRequestedInBurstsWork() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 3
        configuration.maximumConnectionHardLimit = 3
        configuration.keepAliveDuration = .seconds(30)
        configuration.idleTimeoutDuration = .seconds(60)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        func makeKeepAliveTimer(timerID: Int, connectionID: Int) -> (timer: TestPoolStateMachine.Timer, cancellationToken: MockTimerCancellationToken) {
            let timer = TestPoolStateMachine.Timer(.init(timerID: timerID, connectionID: connectionID, usecase: .keepAlive), duration: .seconds(30))
            return (timer, MockTimerCancellationToken(timer))
        }
        func makeIdleTimer(timerID: Int, connectionID: Int) -> (timer: TestPoolStateMachine.Timer, cancellationToken: MockTimerCancellationToken) {
            let timer = TestPoolStateMachine.Timer(.init(timerID: timerID, connectionID: connectionID, usecase: .idleTimeout), duration: .seconds(60))
            return (timer, MockTimerCancellationToken(timer))
        }

        // Connections
        let connection0 = MockConnection(id: 0)
        let connection2 = MockConnection(id: 2)
        let connection1 = MockConnection(id: 1)

        // Requests
        let request0 = MockRequest()
        let request1 = MockRequest()
        let request2 = MockRequest()

        // Initial Timers
        let connection1KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(30))
        let connection1KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer0)
        let connection1IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 1, usecase: .idleTimeout), duration: .seconds(60))
        let connection1IdleTimer1CancellationToken = MockTimerCancellationToken(connection1IdleTimer1)
        let connection2KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 2, usecase: .keepAlive), duration: .seconds(30))
        let connection2KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer0)
        let connection2IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 2, usecase: .idleTimeout), duration: .seconds(60))
        let connection2IdleTimer1CancellationToken = MockTimerCancellationToken(connection2IdleTimer1)
        let connection0KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(30))
        let connection0KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection0KeepAliveTimer0)
        let connection0IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 0, usecase: .idleTimeout), duration: .seconds(60))
        let connection0IdleTimer1CancellationToken = MockTimerCancellationToken(connection0IdleTimer1)

        let requests = stateMachine.refillConnections()
        XCTAssertEqual(requests.count, 0)

        // one connection exists
        let lease1Result = stateMachine.leaseConnection(request1)
        XCTAssertEqual(lease1Result.request, .none)
        XCTAssertEqual(lease1Result.connection, .makeConnection(.init(connectionID: 0), []))

        let lease2Result = stateMachine.leaseConnection(request0)
        XCTAssertEqual(lease2Result.request, .none)
        XCTAssertEqual(lease2Result.connection, .makeConnection(.init(connectionID: 1), [])) // second request
        let lease3Result = stateMachine.leaseConnection(request2)
        XCTAssertEqual(lease3Result.request, .none)
        XCTAssertEqual(lease3Result.connection, .makeConnection(.init(connectionID: 2), [])) // third request

        // fulfil requests
        let connectionEstablished1 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(connectionEstablished1.request, .leaseConnection([request1], connection2))
        XCTAssertEqual(connectionEstablished1.connection, .none)
        let connectionEstablished2 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(connectionEstablished2.request, .leaseConnection([request0], connection1))
        XCTAssertEqual(connectionEstablished2.connection, .none)
        // do work and release connections...
        // Optional("hello")
        // Optional("hello")
        let releaseResult1 = stateMachine.releaseConnection(connection2, streams: 1)
        XCTAssertEqual(releaseResult1.request, .leaseConnection([request2], connection2)) // we still have a pending request
        XCTAssertEqual(releaseResult1.connection, .none)
        let releaseResult2 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseResult2.request, .none) // no more requests
        XCTAssertEqual(releaseResult2.connection, .scheduleTimers([connection1KeepAliveTimer0, connection1IdleTimer1]))

        // schedule timers as requested
        _ = stateMachine.timerScheduled(connection1KeepAliveTimer0, cancelContinuation: connection1KeepAliveTimer0CancellationToken)
        _ = stateMachine.timerScheduled(connection1IdleTimer1, cancelContinuation: connection1IdleTimer1CancellationToken)

        // do more work...
        // Optional("hello")
        let releaseResult3 = stateMachine.releaseConnection(connection2, streams: 1)
        XCTAssertEqual(releaseResult3.request, .none)
        XCTAssertEqual(releaseResult3.connection, .scheduleTimers([connection2KeepAliveTimer0, connection2IdleTimer1]))

        // schedule timers as requested
        _ = stateMachine.timerScheduled(connection2KeepAliveTimer0, cancelContinuation: connection2KeepAliveTimer0CancellationToken)
        _ = stateMachine.timerScheduled(connection2IdleTimer1, cancelContinuation: connection2IdleTimer1CancellationToken)

        let connection0Established = stateMachine.connectionEstablished(connection0, maxStreams: 1)
        XCTAssertEqual(connection0Established.request, .none) // it's not needed anymore, as we requested 3 connections, and all requests have been fulfilled already
        XCTAssertEqual(connection0Established.connection, .scheduleTimers([connection0KeepAliveTimer0, connection0IdleTimer1]))

//        _ = stateMachine.timerScheduled(connection0IdleTimer1, cancelContinuation: connection0IdleTimer1CancellationToken)
        _ = stateMachine.timerScheduled(connection0KeepAliveTimer0, cancelContinuation: connection0KeepAliveTimer0CancellationToken)

        // keep alive timers are triggered after 30s
        for (connection, timer, cancellationToken) in [
            (connection0, connection0KeepAliveTimer0, connection0KeepAliveTimer0CancellationToken),
            (connection1, connection1KeepAliveTimer0, connection1KeepAliveTimer0CancellationToken),
            (connection2, connection2KeepAliveTimer0, connection2KeepAliveTimer0CancellationToken)
        ] {
            let keepAliveResult = stateMachine.timerTriggered(timer)
            XCTAssertEqual(keepAliveResult.request, .none)
            XCTAssertEqual(keepAliveResult.connection, .runKeepAlive(connection, cancellationToken))
        }

        // keep alive requests are done and new timers are scheduled
        let connection0KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 0, usecase: .keepAlive), duration: .seconds(30))
        let connection0KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection0KeepAliveTimer2)
        let connection1KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 1, usecase: .keepAlive), duration: .seconds(30))
        let connection1KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer2)
        let connection2KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 2, usecase: .keepAlive), duration: .seconds(30))
        let connection2KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer2)
        for (connection, newTimer, newTimerCancellationToken) in [
            (connection0, connection0KeepAliveTimer2, connection0KeepAliveTimer2CancellationToken),
            (connection1, connection1KeepAliveTimer2, connection1KeepAliveTimer2CancellationToken),
            (connection2, connection2KeepAliveTimer2, connection2KeepAliveTimer2CancellationToken)
        ] {
            let keepAliveResult = stateMachine.connectionKeepAliveDone(connection)
            XCTAssertEqual(keepAliveResult.request, .none)
            XCTAssertEqual(keepAliveResult.connection, .scheduleTimers([newTimer]))
            _ = stateMachine.timerScheduled(newTimer, cancelContinuation: newTimerCancellationToken)
        }

        // now connections might go idle or trigger another keep alive
        let connection1IdleResult = stateMachine.timerTriggered(connection1IdleTimer1)
        XCTAssertEqual(connection1IdleResult.request, .none)
        XCTAssertEqual(connection1IdleResult.connection, .closeConnection(connection1, [connection1KeepAliveTimer2CancellationToken, connection1IdleTimer1CancellationToken]))
        // Burst done: 1/50
        let keepAliveTriggerAfterGoneResult = stateMachine.timerTriggered(connection1KeepAliveTimer2)
        XCTAssertEqual(keepAliveTriggerAfterGoneResult, .none())

        // we want to start new work on all connections, but a few are occupied or closed already
        let request3 = MockRequest()
        let leaseResult = stateMachine.leaseConnection(request3) // this one works, connections are available
        XCTAssertEqual(leaseResult.request, .leaseConnection(.init(element: request3), connection0))
        XCTAssertEqual(leaseResult.connection, .cancelTimers([connection0KeepAliveTimer2CancellationToken]))


        // a few more timers are getting triggered

        // we wanted to cancel the timer, but it's triggered before we could cancel it
        let keepAliveTriggerOnBusyConnectionResult = stateMachine.timerTriggered(connection0KeepAliveTimer2)
        XCTAssertEqual(keepAliveTriggerOnBusyConnectionResult.request, .none)
        XCTAssertEqual(keepAliveTriggerOnBusyConnectionResult.connection, .none)
        
        let keepAliveTriggerResult = stateMachine.timerTriggered(connection2KeepAliveTimer2)
        XCTAssertEqual(keepAliveTriggerResult.request, .none)
        XCTAssertEqual(keepAliveTriggerResult.connection, .runKeepAlive(connection2, connection2KeepAliveTimer2CancellationToken))

        let idleResult = stateMachine.timerTriggered(connection2IdleTimer1)
        XCTAssertEqual(idleResult.request, .none)
        XCTAssertEqual(idleResult.connection, .closeConnection(connection2, [connection2IdleTimer1CancellationToken]))

        let request4 = MockRequest() // we need another connection, this will cause a crash
        let leaseUnavailableResult = stateMachine.leaseConnection(request4) // it adds a request to the queue, as no connections are available
        XCTAssertEqual(leaseUnavailableResult.request, .none)
        XCTAssertEqual(leaseUnavailableResult.connection, .makeConnection(.init(connectionID: 3), []))
        
        stateMachine.timerTriggered(connection0IdleTimer1) // here the crash happens, either in idle or keep alive timer

        // The reason for the crash might be that all connections are currently unavailable:
        // 0: currently leased
        // 1: marked as going away
        // 2: marked as going away

//        _ConnectionPoolModule/PoolStateMachine.swift:422: Precondition failed
    }


}
