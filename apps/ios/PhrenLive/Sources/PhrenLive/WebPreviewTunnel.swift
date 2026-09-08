import Crypto
import Foundation
import NIOCore
import NIOPosix
import NIOSSH
import PhrenKit

public enum WebPreviewError: LocalizedError, Equatable {
    case unavailable
    public var errorDescription: String? {
        "The app stopped or SSH blocked its port. Refresh the server list. For an older connection, replace its Phren authorization line with the one in Connection settings to enable web previews."
    }
}

/// A phone-loopback listener backed by one pinned SSH connection. Bytes pass
/// through unchanged, including WebSocket upgrades, uploads, and TLS. The
/// browser owns its lifetime; no remote process or public listener is created.
public final class WebPreviewTunnel: @unchecked Sendable {
    public let url: URL
    private let state: PreviewTunnelState
    private init(url: URL, state: PreviewTunnelState) { self.url = url; self.state = state }

    public static func open(host: LiveHost, privateKey: Data, server: WebServer) async throws -> WebPreviewTunnel {
        try host.validate()
        let key = try Curve25519.Signing.PrivateKey(rawRepresentation: privateKey)
        let loop = MultiThreadedEventLoopGroup.singleton.next()
        let state = PreviewTunnelState(loop: loop, destination: server.loopbackHost, port: server.port)
        let ready = Exchange(result: loop.makePromise(of: Data.self))
        return try await withTaskCancellationHandler {
            do {
                try Task.checkCancellation()
                let deadline = loop.scheduleTask(in: .seconds(20)) { ready.finish(.failure(LiveConnectionError.timeout)) }
                ready.result.futureResult.whenComplete { _ in deadline.cancel() }
                let bootstrap = ClientBootstrap(group: loop).connectTimeout(.seconds(10)).channelInitializer { channel in
                    state.parent = channel
                    guard !state.closed else { return channel.close() }
                    return channel.eventLoop.makeCompletedFuture {
                        let ssh = NIOSSHHandler(role: .client(.init(
                            userAuthDelegate: DeviceAuthentication(username: host.username, key: key, exchange: ready),
                            serverAuthDelegate: PinnedHost(fingerprint: host.fingerprint))),
                            allocator: channel.allocator, inboundChildChannelInitializer: { child, _ in
                                child.eventLoop.makeFailedFuture(LiveConnectionError.disconnected)
                            })
                        try channel.pipeline.syncOperations.addHandlers(ssh, PreviewSSHEvents(state: state, ready: ready))
                    }
                }
                bootstrap.connect(host: host.address, port: host.port).whenFailure { ready.finish(.failure($0)) }
                _ = try await ready.result.futureResult.get()
                try Task.checkCancellation()
                // Probe before presenting a blank browser; permission failures are actionable.
                let probe = try await loop.flatSubmit { state.openChannel() }.get()
                try await probe.close()
                let listener = ServerBootstrap(group: loop)
                    .childChannelOption(ChannelOptions.autoRead, value: false)
                    .childChannelInitializer { local in state.attach(local) }
                // Preserving the port also supports apps which emit absolute localhost URLs.
                let channel: Channel
                do { channel = try await listener.bind(host: "127.0.0.1", port: server.port).get() }
                catch { channel = try await listener.bind(host: "127.0.0.1", port: 0).get() }
                try await loop.submit {
                    state.listener = channel
                    if state.closed { channel.close(promise: nil) }
                }.get()
                try Task.checkCancellation()
                guard channel.isActive, let port = channel.localAddress?.port else { throw LiveConnectionError.disconnected }
                return WebPreviewTunnel(url: URL(string: "\(server.scheme)://127.0.0.1:\(port)/")!, state: state)
            } catch {
                await state.loop.submit { state.close(); ready.finish(.failure(error)) }.getIgnoringFailure()
                throw error
            }
        } onCancel: {
            loop.execute { state.close(); ready.finish(.failure(CancellationError())) }
        }
    }

    public func close() { state.loop.execute { self.state.close() } }
    public func waitUntilClosed() async { _ = try? await state.ended.futureResult.get() }
    deinit { let state = state; state.loop.execute { state.close() } }
}

// State and all relay channels share this single event loop.
private final class PreviewTunnelState: @unchecked Sendable {
    let loop: EventLoop
    let port: Int
    let destination: String
    let ended: EventLoopPromise<Void>
    var parent: Channel?
    var listener: Channel?
    var clients: [ObjectIdentifier: Channel] = [:]
    var closed = false
    init(loop: EventLoop, destination: String, port: Int) {
        self.loop = loop; self.destination = destination; self.port = port; ended = loop.makePromise()
    }

    func close() {
        guard !closed else { return }; closed = true
        listener?.close(promise: nil); parent?.close(promise: nil)
        for channel in clients.values { channel.close(promise: nil) }
        clients.removeAll()
        ended.succeed(())
    }

    func openChannel(local: Channel? = nil) -> EventLoopFuture<Channel> {
        guard !closed, let parent, parent.isActive else { return loop.makeFailedFuture(LiveConnectionError.disconnected) }
        let promise = loop.makePromise(of: Channel.self)
        let deadline = loop.scheduleTask(in: .seconds(10)) { self.close() }
        promise.futureResult.whenComplete { _ in deadline.cancel() }
        do {
            let ssh = try parent.pipeline.syncOperations.handler(type: NIOSSHHandler.self)
            let target = SSHChannelType.DirectTCPIP(targetHost: destination, targetPort: port,
                originatorAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 0))
            ssh.createChannel(promise, channelType: .directTCPIP(target)) { remote, _ in
                remote.setOption(ChannelOptions.autoRead, value: false).flatMap {
                    if let local { return remote.pipeline.addHandlers(SSHHTTPBytes(), PreviewRelay(peer: local)) }
                    return remote.eventLoop.makeSucceededVoidFuture()
                }
            }
        } catch { promise.fail(error) }
        return promise.futureResult.flatMapError { _ in self.loop.makeFailedFuture(WebPreviewError.unavailable) }
    }

    func attach(_ local: Channel) -> EventLoopFuture<Void> {
        guard clients.count < 64, !closed else { return local.close() }
        let id = ObjectIdentifier(local)
        clients[id] = local
        local.closeFuture.whenComplete { _ in self.clients.removeValue(forKey: id) }
        return openChannel(local: local).flatMap { remote in
            guard !self.closed else { return remote.close() }
            return local.pipeline.addHandler(PreviewRelay(peer: remote)).flatMap {
                remote.setOption(ChannelOptions.autoRead, value: true)
            }.flatMap { local.setOption(ChannelOptions.autoRead, value: true) }
        }.flatMapError { error in
            local.close(promise: nil)
            return self.loop.makeFailedFuture(error)
        }
    }
}

private final class PreviewSSHEvents: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    let state: PreviewTunnelState
    let ready: Exchange
    init(state: PreviewTunnelState, ready: Exchange) { self.state = state; self.ready = ready }
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is UserAuthSuccessEvent { ready.finish(.success(Data())) }
    }
    func errorCaught(context: ChannelHandlerContext, error: Error) { ready.finish(.failure(error)); state.close() }
    func channelInactive(context: ChannelHandlerContext) { ready.finish(.failure(LiveConnectionError.disconnected)); state.close() }
}

/// Gate reads on the other channel's writability to bound queued page data.
private final class PreviewRelay: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = IOData
    var peer: Channel?
    init(peer: Channel) { self.peer = peer }
    func channelActive(context: ChannelHandlerContext) {
        context.fireChannelActive()
        context.read(); peer?.read()
    }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        peer?.write(IOData.byteBuffer(unwrapInboundIn(data))).whenFailure { _ in context.close(promise: nil) }
    }
    func channelReadComplete(context: ChannelHandlerContext) { peer?.flush() }
    func read(context: ChannelHandlerContext) { if peer?.isWritable == true { context.read() } }
    func channelWritabilityChanged(context: ChannelHandlerContext) { if context.channel.isWritable { peer?.read() } }
    func channelInactive(context: ChannelHandlerContext) { peer?.close(promise: nil) }
    func errorCaught(context: ChannelHandlerContext, error: Error) { context.close(promise: nil); peer?.close(promise: nil) }
    func handlerRemoved(context: ChannelHandlerContext) { peer = nil }
}

private extension EventLoopFuture where Value == Void {
    func getIgnoringFailure() async { _ = try? await get() }
}
