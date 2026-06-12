"""AWS STS presigned-URL bearer tokens for Flyte Admin authentication.

Some Flyte deployments front Admin with a proxy that authenticates callers by
validating an AWS identity instead of an OAuth2 id-token. The bearer token is a
base64url-encoded, presigned ``sts:GetCallerIdentity`` URL: the proxy decodes
it, replays the ``GetCallerIdentity`` call against AWS STS, and trusts the
returned principal ARN as the caller's identity.

This module mints that token using only the standard library so flytekit gains
no new dependency.
"""

import configparser
import hashlib
import hmac
import json
import os
import subprocess
import urllib.parse
import urllib.request
from base64 import urlsafe_b64encode
from datetime import datetime, timezone

#: STS endpoint the presigned ``GetCallerIdentity`` URL targets. The global
#: endpoint is used so the proxy can replay it from any region.
DEFAULT_STS_HOST = "sts.amazonaws.com"
DEFAULT_STS_REGION = "us-east-1"
_STS_SERVICE = "sts"

#: Default validity window for a presigned URL. Tokens are minted fresh per
#: request, so this only needs to cover clock skew plus request latency.
DEFAULT_EXPIRES_IN_SECONDS = 60

_IMDS_BASE = "http://169.254.169.254"
_IMDS_TIMEOUT_SECONDS = 2


class STSCredentialsError(Exception):
    """Raised when no AWS credentials can be resolved for STS auth."""


def _hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _credentials_from_env() -> "tuple[str, str, str | None] | None":
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return access_key, secret_key, os.environ.get("AWS_SESSION_TOKEN")
    return None


def _credentials_from_shared_file(profile: str) -> "tuple[str, str, str | None] | None":
    creds_path = os.path.expanduser(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials"))
    if not os.path.exists(creds_path):
        return None
    ini = configparser.ConfigParser()
    ini.read(creds_path)
    if not ini.has_section(profile):
        return None
    access_key = ini.get(profile, "aws_access_key_id", fallback=None)
    secret_key = ini.get(profile, "aws_secret_access_key", fallback=None)
    if access_key and secret_key:
        return access_key, secret_key, ini.get(profile, "aws_session_token", fallback=None)
    return None


def _credentials_from_export(profile: str) -> "tuple[str, str, str | None] | None":
    """Resolve SSO / ``credential_process`` profiles via the AWS CLI.

    These credential sources are not stored as plain keys, so we shell out to
    ``aws configure export-credentials`` (the supported way to materialize
    them) only when the profile actually declares such a source.
    """
    config_path = os.path.expanduser(os.environ.get("AWS_CONFIG_FILE", "~/.aws/config"))
    config_ini = configparser.ConfigParser()
    if os.path.exists(config_path):
        config_ini.read(config_path)
    section = "default" if profile == "default" else f"profile {profile}"
    needs_export = any(
        config_ini.has_option(section, opt) for opt in ("sso_start_url", "sso_session", "credential_process")
    )
    if not needs_export:
        return None
    try:
        result = subprocess.run(
            ["aws", "configure", "export-credentials", "--profile", profile, "--format", "env"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    exported: "dict[str, str]" = {}
    for line in result.stdout.strip().splitlines():
        line = line.removeprefix("export ").strip()
        if "=" in line:
            key, value = line.split("=", 1)
            exported[key.strip()] = value.strip()
    access_key = exported.get("AWS_ACCESS_KEY_ID")
    secret_key = exported.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return access_key, secret_key, exported.get("AWS_SESSION_TOKEN")
    return None


def _credentials_from_imds() -> "tuple[str, str, str | None] | None":
    """Resolve the instance/pod role credentials via IMDSv2."""
    try:
        token_req = urllib.request.Request(
            f"{_IMDS_BASE}/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        )
        imds_token = urllib.request.urlopen(token_req, timeout=_IMDS_TIMEOUT_SECONDS).read().decode()
        headers = {"X-aws-ec2-metadata-token": imds_token}
        role_req = urllib.request.Request(
            f"{_IMDS_BASE}/latest/meta-data/iam/security-credentials/",
            headers=headers,
        )
        role_name = urllib.request.urlopen(role_req, timeout=_IMDS_TIMEOUT_SECONDS).read().decode().strip()
        creds_req = urllib.request.Request(
            f"{_IMDS_BASE}/latest/meta-data/iam/security-credentials/{role_name}",
            headers=headers,
        )
        creds = json.loads(urllib.request.urlopen(creds_req, timeout=_IMDS_TIMEOUT_SECONDS).read())
    except Exception:
        return None
    access_key = creds.get("AccessKeyId")
    secret_key = creds.get("SecretAccessKey")
    if access_key and secret_key:
        return access_key, secret_key, creds.get("Token")
    return None


def resolve_aws_credentials() -> "tuple[str, str, str | None]":
    """Resolve AWS credentials: env vars → config file → SSO export → IMDS.

    Returns an ``(access_key, secret_key, session_token)`` triple, where the
    session token is ``None`` for long-lived IAM-user credentials. Raises
    :class:`STSCredentialsError` when no source yields credentials.
    """
    profile = os.environ.get("AWS_PROFILE", "default")
    for source in (
        _credentials_from_env,
        lambda: _credentials_from_shared_file(profile),
        lambda: _credentials_from_export(profile),
        _credentials_from_imds,
    ):
        creds = source()
        if creds is not None:
            return creds
    raise STSCredentialsError(
        "no AWS credentials found for STS auth (checked env vars, AWS config/credentials, and IMDS)"
    )


def create_sts_token(
    access_key: str,
    secret_key: str,
    session_token: "str | None" = None,
    *,
    expires_in: int = DEFAULT_EXPIRES_IN_SECONDS,
    region: str = DEFAULT_STS_REGION,
    host: str = DEFAULT_STS_HOST,
) -> str:
    """Build the base64url-encoded presigned ``GetCallerIdentity`` URL.

    The result is the value an STS auth proxy expects as the bearer token. The
    URL is signed with SigV4 over the ``host`` header only, matching the
    minimal canonical request the proxy replays.
    """
    now = datetime.now(timezone.utc)
    date_stamp = now.strftime("%Y%m%d")
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{date_stamp}/{region}/{_STS_SERVICE}/aws4_request"

    params: "dict[str, str]" = {
        "Action": "GetCallerIdentity",
        "Version": "2011-06-15",
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires_in),
        "X-Amz-SignedHeaders": "host",
    }
    if session_token:
        params["X-Amz-Security-Token"] = session_token

    canonical_query = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(params.items())
    )
    canonical_request = "\n".join(["GET", "/", canonical_query, f"host:{host}\n", "host", _sha256_hex("")])
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, _sha256_hex(canonical_request)])
    signing_key = _hmac_sha256(
        _hmac_sha256(
            _hmac_sha256(
                _hmac_sha256(f"AWS4{secret_key}".encode("utf-8"), date_stamp),
                region,
            ),
            _STS_SERVICE,
        ),
        "aws4_request",
    )
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    signed_url = f"https://{host}/?{canonical_query}&X-Amz-Signature={signature}"
    return urlsafe_b64encode(signed_url.encode()).decode().rstrip("=")
