import Foundation
import NIOCore

/// Retry transport loss, never host-key/authentication or HTTP rejection.
/// A stable connection earns a fresh retry budget; rapid flapping does not.
public struct HerdrTerminalRecovery {
    private var attempts = 0
    private var connectedAt: TimeInterval?
    public init() {}
    public mutating func connected(at time: TimeInterval) { connectedAt = time }
    public mutating func delay(after error: Error, now: TimeInterval) -> Int? {
        let transient: Bool
        if let error = error as? LiveConnectionError { transient = error == .disconnected || error == .timeout }
        else { transient = error is IOError || error is ChannelError }
        guard transient else { return nil }
        if let connectedAt, now - connectedAt >= 10 { attempts = 0 }
        connectedAt = nil
        guard attempts < 3 else { return nil }
        let seconds = 1 << attempts
        attempts += 1
        return seconds
    }
}
