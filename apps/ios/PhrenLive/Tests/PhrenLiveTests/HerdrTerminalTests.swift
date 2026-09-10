import Foundation
import PhrenKit
import XCTest
@testable import PhrenLive

final class HerdrTerminalTests: XCTestCase {
    func testBurstPreservesEveryByteAcrossCoalescedWakeupsAndPartialDrains() async throws {
        let buffer = TerminalOutputBuffer()
        let (signals, continuation) = AsyncThrowingStream<Void, Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
        let output = HerdrTerminalOutput(buffer: buffer, signals: signals)
        let expected = Data(String(repeating: "\u{1B}[32mPhren 🦀\u{1B}[0m\r\n", count: 8_000).utf8)
        // Thousands of tiny frames must not overflow an eight-frame queue or
        // lose parts of a multibyte character/control sequence.
        for byte in expected { try buffer.append(Data([byte])); continuation.yield(()) }
        var iterator = output.makeAsyncIterator()
        let first = try await iterator.next()
        var actual = try XCTUnwrap(first)
        XCTAssertEqual(actual.count, 65_536)
        let suffix = Data("after partial drain".utf8)
        try buffer.append(suffix); continuation.yield(())
        continuation.finish(throwing: LiveConnectionError.disconnected)
        do {
            while let bytes = try await iterator.next() {
                XCTAssertLessThanOrEqual(bytes.count, 65_536)
                actual.append(bytes)
            }
            XCTFail("The disconnect must reach the reader after buffered output")
        } catch { XCTAssertEqual(error as? LiveConnectionError, .disconnected) }
        XCTAssertEqual(actual, expected + suffix)
    }

    func testTerminalBufferBoundsBytesAndReleasesConsumedCapacity() throws {
        let buffer = TerminalOutputBuffer()
        try buffer.append(Data(repeating: 65, count: TerminalOutputBuffer.capacity))
        XCTAssertThrowsError(try buffer.append(Data([66]))) { XCTAssertEqual($0 as? LiveConnectionError, .oversized) }
        let consumed = try XCTUnwrap(buffer.take())
        try buffer.append(Data(repeating: 66, count: consumed.count))
        var count = 0, last = Data()
        while let bytes = buffer.take() { count += bytes.count; last = bytes }
        XCTAssertEqual(count, TerminalOutputBuffer.capacity)
        XCTAssertEqual(last, Data(repeating: 66, count: consumed.count))
    }

    func testCancelledTerminalDoesNotRenderBufferedOutput() async throws {
        let buffer = TerminalOutputBuffer()
        try buffer.append(Data("pending".utf8))
        let (signals, continuation) = AsyncThrowingStream<Void, Error>.makeStream()
        defer { continuation.finish() }
        let output = HerdrTerminalOutput(buffer: buffer, signals: signals)
        let task = Task {
            while !Task.isCancelled { await Task.yield() }
            var iterator = output.makeAsyncIterator()
            do { _ = try await iterator.next(); XCTFail("Cancelled output must not render") }
            catch { XCTAssertTrue(error is CancellationError) }
        }
        task.cancel(); await task.value
    }

    func testRecoveryBoundsFlappingAndNeverRetriesAuthenticationOrCancellation() {
        var recovery = HerdrTerminalRecovery()
        for (time, expected) in [(1.0, 1), (2.0, 2), (3.0, 4)] {
            recovery.connected(at: time)
            XCTAssertEqual(recovery.delay(after: LiveConnectionError.disconnected, now: time + 0.5), expected)
        }
        XCTAssertNil(recovery.delay(after: LiveConnectionError.timeout, now: 5))
        recovery.connected(at: 10)
        XCTAssertEqual(recovery.delay(after: LiveConnectionError.timeout, now: 21), 1)
        for error: Error in [LiveConnectionError.changedHost, LiveConnectionError.authentication,
                             LiveConnectionError.response(403), LiveConnectionError.oversized, CancellationError()] {
            XCTAssertNil(recovery.delay(after: error, now: 30))
        }
    }

    func testBusyTerminalWithSlowRendererAndIdleHeartbeatThroughSSH() async throws {
        guard ProcessInfo.processInfo.environment["PHREN_TERMINAL_RELIABILITY"] == "1" else {
            throw XCTSkip("Requires our isolated phren-terminal-reliability Herdr server")
        }
        let relay = try await ChatRelaySSH.start()
        defer { Task { try await relay.close() } }
        var host = try relay.host(); host.herdrSession = "phren-terminal-reliability"
        let key = relay.deviceKey.rawRepresentation
        let before = try await MoshiConnection.fetch(host: host, privateKey: key)
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .create, label: "Terminal reliability", cwd: "/tmp")
        let created = try await MoshiConnection.fetch(host: host, privateKey: key)
        let workspace = try XCTUnwrap(created.groups.first { !before.groups.map(\.id).contains($0.id) })
        defer { Task { try? await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: workspace.id) } }
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .focus, workspaceID: workspace.id)
        let socket = HerdrTerminalSocket()
        let nonce = UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let burstMarker = "BURST_" + nonce, idleMarker = "IDLE_" + nonce
        let ready = expectation(description: "PTY attached"), done = expectation(description: "Output finished")
        let echo = expectation(description: "Input works after idle heartbeat")
        let reader = Task {
            var first = true, count = 0, bytesRead = 0, sawDone = false, sawEcho = false
            var tail = ""
            do {
                for try await bytes in MoshiConnection.herdrTerminal(host: host, privateKey: key, socket: socket) {
                    count += 1; bytesRead += bytes.count
                    tail = String((tail + String(decoding: bytes, as: UTF8.self)).suffix(10_000))
                    if first { first = false; ready.fulfill() }
                    if tail.contains(burstMarker), !sawDone { sawDone = true; done.fulfill() }
                    if tail.contains(idleMarker), !sawEcho { sawEcho = true; echo.fulfill() }
                    // Deliberately hold rendering so multiple network frames arrive.
                    try await Task.sleep(for: .milliseconds(100))
                    try await socket.acknowledge(bytes.count)
                }
            } catch { if !Task.isCancelled { XCTFail("Terminal failed after \(count) batches / \(bytesRead) bytes: \(error)") } }
            print("Terminal verification: \(count) batches, \(bytesRead) bytes")
        }
        defer { reader.cancel() }
        await fulfillment(of: [ready], timeout: 10)
        try await Task.sleep(for: .seconds(2))
        try await socket.resize(columns: 160, rows: 60)
        // The explicitly named server contains only our disposable shell.
        let script = FileManager.default.temporaryDirectory.appendingPathComponent("phren-terminal-burst-" + nonce + ".py")
        try Data("""
        import sys,time
        phase = sys.argv[2]
        if phase == "burst":
            for i in range(300):
                sys.stdout.write(chr(27)+"[H"+(str(i)+" x"*75+"\\r\\n")*55)
                sys.stdout.flush()
                time.sleep(.01)
        # Give Herdr a blank frame before the marker. It otherwise emits only
        # changed cells, so a raw substring check can miss unchanged letters.
        sys.stdout.write(chr(27)+"[2J"+chr(27)+"[H")
        sys.stdout.flush()
        time.sleep(.3)
        print(phase.upper()+"_"+sys.argv[1], flush=True)
        """.utf8).write(to: script)
        defer { try? FileManager.default.removeItem(at: script) }
        try await socket.input("python3 \(script.path) \(nonce) burst\r")
        await fulfillment(of: [done], timeout: 30)
        try await Task.sleep(for: .seconds(50))
        try await socket.input("python3 \(script.path) \(nonce) idle\r")
        await fulfillment(of: [echo], timeout: 10)
        reader.cancel(); await reader.value
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: workspace.id)
    }
}
