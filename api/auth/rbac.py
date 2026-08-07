"""
api/auth/rbac.py
Role-based access control for multi-tenant migrations.
"""
from __future__ import annotations

from enum import Enum
from functools import wraps
from typing import Callable

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.auth.jwt import decode_token

bearer = HTTPBearer(auto_error=False)


class Role(str, Enum):
    VIEWER = "viewer"      # Can see graphs, intents, previews
    EDITOR = "editor"      # Can submit commands, build DAGs, run previews
    APPROVER = "approver"  # Can approve / reject staged runs
    ADMIN = "admin"        # Full access including user management


ROLE_HIERARCHY = {
    Role.VIEWER: 0,
    Role.EDITOR: 1,
    Role.APPROVER: 2,
    Role.ADMIN: 3,
}


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Security(bearer),
) -> dict:
    """
    FastAPI dependency — extracts and validates the JWT bearer token.
    Returns the decoded token payload dict.

    In development mode (no token provided), returns a default admin user.
    """
    if credentials is None:
        # No auth in dev mode
        return {"sub": "dev-user", "tenant_id": "default", "roles": [Role.ADMIN.value]}

    try:
        payload = decode_token(credentials.credentials)
        return payload.model_dump()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_role(minimum_role: Role) -> Callable:
    """
    FastAPI dependency factory — requires at least `minimum_role`.

    Usage:
        @router.post("/commit/approve")
        async def approve(..., user=Depends(require_role(Role.APPROVER))):
            ...
    """
    def dependency(user: dict = Depends(get_current_user)) -> dict:
        user_roles = user.get("roles", [])
        max_level = max(
            (ROLE_HIERARCHY.get(Role(r), 0) for r in user_roles),
            default=0,
        )
        required_level = ROLE_HIERARCHY[minimum_role]
        if max_level < required_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role {minimum_role.value!r} required.",
            )
        return user
    return dependency


def require_tenant(user: dict = Depends(get_current_user)) -> str:
    """Dependency — returns the tenant_id from the current token."""
    return user.get("tenant_id", "default")
