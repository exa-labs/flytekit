from unittest.mock import MagicMock

from flytekit.clients.grpc_utils.static_metadata_interceptor import StaticMetadataInterceptor


def _call_details(metadata=None):
    d = MagicMock()
    d.method = "/foo.Service/Method"
    d.timeout = None
    d.metadata = metadata
    d.credentials = None
    return d


def test_injects_metadata_when_none_present():
    interceptor = StaticMetadataInterceptor([("agent-session-id", "abc"), ("agent-session-owner", "octocat")])
    continuation = MagicMock()

    interceptor.intercept_unary_unary(continuation, _call_details(), request="req")

    passed_details = continuation.call_args.args[0]
    assert list(passed_details.metadata) == [
        ("agent-session-id", "abc"),
        ("agent-session-owner", "octocat"),
    ]


def test_preserves_existing_metadata():
    interceptor = StaticMetadataInterceptor([("agent-session-id", "abc")])
    continuation = MagicMock()

    interceptor.intercept_unary_stream(continuation, _call_details(metadata=[("authorization", "Bearer x")]), "req")

    passed_details = continuation.call_args.args[0]
    assert list(passed_details.metadata) == [
        ("agent-session-id", "abc"),
        ("authorization", "Bearer x"),
    ]


def test_lowercases_keys_and_drops_empty_pairs():
    interceptor = StaticMetadataInterceptor(
        [("Agent-Session-Id", "abc"), ("", "novalue"), ("nokey", ""), ("agent-session-owner", "octocat")]
    )
    continuation = MagicMock()

    interceptor.intercept_unary_unary(continuation, _call_details(), request="req")

    passed_details = continuation.call_args.args[0]
    assert list(passed_details.metadata) == [
        ("agent-session-id", "abc"),
        ("agent-session-owner", "octocat"),
    ]
