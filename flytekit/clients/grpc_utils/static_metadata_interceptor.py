"""gRPC client interceptor that stamps a fixed set of metadata on every call.

Some deployments front Flyte Admin with a proxy that expects extra request
metadata (gRPC/HTTP2 headers) beyond the auth token -- for example caller- or
session-attribution headers used for routing or accounting. This interceptor
adds a fixed set of ``(key, value)`` pairs to the metadata of every unary and
stream call, mirroring :class:`DefaultMetadataInterceptor`.

The metadata is static for the lifetime of the channel: the values are resolved
once (typically from the environment) when the channel is built, not per call.
This is independent of the auth flow, so it composes with any ``auth_mode``.
"""

import typing

import grpc

from flytekit.clients.grpc_utils.auth_interceptor import _ClientCallDetails


class StaticMetadataInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    def __init__(self, metadata: typing.List[typing.Tuple[str, str]]):
        # gRPC metadata keys must be lowercase ASCII; drop pairs with empty key/value.
        self._metadata: typing.List[typing.Tuple[str, str]] = [
            (key.lower(), value) for key, value in metadata if key and value
        ]

    def _inject_static_metadata(self, call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        metadata = list(self._metadata)
        if call_details.metadata:
            metadata.extend(list(call_details.metadata))
        return _ClientCallDetails(
            call_details.method,
            call_details.timeout,
            metadata,
            call_details.credentials,
        )

    def intercept_unary_unary(
        self,
        continuation: typing.Callable,
        client_call_details: grpc.ClientCallDetails,
        request: typing.Any,
    ):
        """Intercepts unary calls and injects the static metadata."""
        return continuation(self._inject_static_metadata(client_call_details), request)

    def intercept_unary_stream(
        self,
        continuation: typing.Callable,
        client_call_details: grpc.ClientCallDetails,
        request: typing.Any,
    ):
        """Handles a stream call and injects the static metadata."""
        return continuation(self._inject_static_metadata(client_call_details), request)
