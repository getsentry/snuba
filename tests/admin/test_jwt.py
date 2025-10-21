from datetime import datetime, timedelta, timezone
from typing import Tuple
from unittest.mock import MagicMock

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from snuba.admin.jwt import validate_assertion
from snuba.admin.user import AdminUser
from tests.conftest import TEST_AUDIENCE, TEST_EMAIL, TEST_SUB, create_test_token


def test_validate_assertion_success(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
    key_pair: Tuple[bytes, bytes],
) -> None:
    """Test that a valid JWT token is properly validated and returns an AdminUser"""
    private_key, _ = key_pair
    token = create_test_token(private_key)
    user = validate_assertion(token)
    assert isinstance(user, AdminUser)
    assert user.email == TEST_EMAIL
    assert user.id == TEST_SUB


def test_validate_assertion_invalid_signature(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
) -> None:
    """Test that an invalid signature raises an exception"""
    # Create a different key pair for signing
    different_private_key = ec.generate_private_key(ec.SECP256K1()).private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    token = create_test_token(different_private_key)
    with pytest.raises(jwt.InvalidSignatureError):
        validate_assertion(token)


def test_validate_assertion_expired(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
    key_pair: Tuple[bytes, bytes],
) -> None:
    """Test that an expired token raises an exception"""
    private_key, _ = key_pair
    token = create_test_token(private_key, exp_delta=timedelta(hours=-1))
    with pytest.raises(jwt.ExpiredSignatureError):
        validate_assertion(token)


def test_validate_assertion_invalid_audience(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
    key_pair: Tuple[bytes, bytes],
) -> None:
    """Test that a token with wrong audience raises an exception"""
    private_key, _ = key_pair
    token = create_test_token(private_key, audience="wrong-audience")
    with pytest.raises(jwt.InvalidAudienceError):
        validate_assertion(token)


def test_validate_assertion_missing_email(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
    key_pair: Tuple[bytes, bytes],
) -> None:
    """Test that a token without email claim raises an exception"""
    private_key, _ = key_pair
    now = datetime.now(timezone.utc)
    payload = {
        "sub": TEST_SUB,
        "aud": TEST_AUDIENCE,
        "exp": now + timedelta(hours=1),
        "iat": now,
    }
    token = jwt.encode(payload, private_key.decode("utf-8"), algorithm="ES256")
    with pytest.raises(KeyError):
        validate_assertion(token)


def test_validate_assertion_missing_sub(
    mock_settings: MagicMock,
    mock_certs: MagicMock,
    key_pair: Tuple[bytes, bytes],
) -> None:
    """Test that a token without sub claim raises an exception"""
    private_key, _ = key_pair
    now = datetime.now(timezone.utc)
    payload = {
        "email": TEST_EMAIL,
        "aud": TEST_AUDIENCE,
        "exp": now + timedelta(hours=1),
        "iat": now,
    }
    token = jwt.encode(payload, private_key.decode("utf-8"), algorithm="ES256")
    with pytest.raises(KeyError):
        validate_assertion(token)
