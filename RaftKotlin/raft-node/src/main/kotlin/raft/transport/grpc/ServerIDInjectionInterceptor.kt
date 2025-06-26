package raft.transport.grpc

import io.grpc.*

/**
 * Injects the peer ID into the request metadata.
 */
class ServerIDInjectionInterceptor(
    /**
     * The ID of the peer sending the requests.
     */
    private val peerID: Int
) : ClientInterceptor {

    override fun <ReqT, RespT> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {
        return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)
        ) {
            override fun start(responseListener: Listener<RespT>, headers: Metadata) {
                headers.put(
                    Metadata.Key.of("x-peer-id", Metadata.ASCII_STRING_MARSHALLER),
                    peerID.toString()
                )
                super.start(responseListener, headers)
            }
        }
    }
}