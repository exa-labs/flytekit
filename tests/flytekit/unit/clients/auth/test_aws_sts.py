import base64
import urllib.parse

import pytest

from flytekit.clients.auth.aws_sts import (
    STSCredentialsError,
    create_sts_token,
    resolve_aws_credentials,
)


def _decode_token(token: str) -> urllib.parse.ParseResult:
    # base64url-decode (restoring stripped padding) back to the signed URL.
    padded = token + "=" * (-len(token) % 4)
    signed_url = base64.urlsafe_b64decode(padded).decode()
    return urllib.parse.urlparse(signed_url)


def test_create_sts_token_is_presigned_get_caller_identity_url():
    token = create_sts_token("AKIAEXAMPLE", "secret", expires_in=60)
    parsed = _decode_token(token)
    assert parsed.scheme == "https"
    assert parsed.netloc == "sts.amazonaws.com"

    query = urllib.parse.parse_qs(parsed.query)
    assert query["Action"] == ["GetCallerIdentity"]
    assert query["Version"] == ["2011-06-15"]
    assert query["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert query["X-Amz-Expires"] == ["60"]
    assert query["X-Amz-SignedHeaders"] == ["host"]
    assert query["X-Amz-Credential"][0].startswith("AKIAEXAMPLE/")
    assert query["X-Amz-Credential"][0].endswith("/us-east-1/sts/aws4_request")
    # A signature is always appended last.
    assert len(query["X-Amz-Signature"][0]) == 64


def test_create_sts_token_includes_session_token_when_present():
    with_token = _decode_token(create_sts_token("ak", "sk", "session-token-123"))
    without_token = _decode_token(create_sts_token("ak", "sk"))

    assert "X-Amz-Security-Token" in urllib.parse.parse_qs(with_token.query)
    assert "X-Amz-Security-Token" not in urllib.parse.parse_qs(without_token.query)


def test_create_sts_token_has_no_base64_padding():
    # The proxy expects a base64url value without trailing '=' padding.
    assert "=" not in create_sts_token("ak", "sk")


def test_create_sts_token_respects_region_override():
    parsed = _decode_token(create_sts_token("ak", "sk", region="us-west-2"))
    query = urllib.parse.parse_qs(parsed.query)
    assert query["X-Amz-Credential"][0].endswith("/us-west-2/sts/aws4_request")


def test_resolve_aws_credentials_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secretenv")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "tokenenv")
    assert resolve_aws_credentials() == ("AKIAENV", "secretenv", "tokenenv")


def test_resolve_aws_credentials_raises_when_unavailable(monkeypatch):
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    # Point credential/config file resolution at empty paths and disable IMDS.
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent/config")
    monkeypatch.setattr("flytekit.clients.auth.aws_sts._credentials_from_imds", lambda: None)
    with pytest.raises(STSCredentialsError):
        resolve_aws_credentials()
