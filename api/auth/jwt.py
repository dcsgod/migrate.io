"""
api/auth/jwt.py
JWT token creation and verification for multi-tenant RBAC.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from pydantic import BaseModel

SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "change-me-in-production")
ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))


class TokenPayload(BaseModel):
    sub: str           # user ID
    tenant_id: str
    roles: list[str]
    exp: int


def create_access_token(
    user_id: str,
    tenant_id: str,
    roles: list[str],
    expires_delta: timedelta | None = None,
) -> str:
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "roles": roles,
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> TokenPayload:
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return TokenPayload(**data)
    except JWTError as exc:
        raise ValueError(f"Invalid token: {exc}") from exc


def verify_token(token: str) -> bool:
    try:
        decode_token(token)
        return True
    except ValueError:
        return False
