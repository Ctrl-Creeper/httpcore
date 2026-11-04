import anyio
import pytest

import httpcore
from httpcore._backends.anyio import AnyIOBackend
from httpcore._backends.trio import TrioBackend


@pytest.mark.anyio
@pytest.mark.parametrize("backend", [AnyIOBackend, TrioBackend])
@pytest.mark.parametrize("failure", ["cancel", "tls_error"])
async def test_failed_proxy_tls_handshake_releases_connection(anyio_backend, backend, failure):
    """A failed handshake must release both the socket and the pool slot."""
    if backend is TrioBackend and anyio_backend != "trio":
        pytest.skip("The native Trio backend requires Trio")
    client_closed = anyio.Event()

    async def proxy(stream):
        async with stream:
            request = b""
            while b"\r\n\r\n" not in request:
                request += await stream.receive()
            await stream.send(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await stream.receive()  # ClientHello; never complete TLS negotiation.
            if failure == "tls_error":
                await stream.send_eof()
            try:
                await stream.receive()
            except anyio.EndOfStream:
                client_closed.set()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    port = listener.extra(anyio.abc.SocketAttribute.local_port)
    async with listener, anyio.create_task_group() as tasks:
        tasks.start_soon(listener.serve, proxy)
        async with httpcore.AsyncHTTPProxy(
            f"http://127.0.0.1:{port}", network_backend=backend()
        ) as pool:
            if failure == "cancel":
                with anyio.move_on_after(0.1) as cancelled:
                    await pool.request("GET", "https://example.com/")
                assert cancelled.cancel_called
            else:
                with pytest.raises(httpcore.ConnectError):
                    await pool.request("GET", "https://example.com/")
            # The pool remains open. Its eventual aclose() must not be needed
            # to release the stream whose TLS handshake was cancelled.
            with anyio.fail_after(1):
                await client_closed.wait()
            assert not pool.connections
        tasks.cancel_scope.cancel()
