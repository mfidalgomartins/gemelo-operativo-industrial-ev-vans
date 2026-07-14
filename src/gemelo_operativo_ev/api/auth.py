from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .settings import ApiSettings


class Role(IntEnum):
    VIEWER = 10
    OPERATOR = 20


@dataclass(frozen=True)
class Principal:
    subject: str
    role: Role


BEARER_SCHEME = HTTPBearer(auto_error=False)


class BearerAuthorizer:
    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings

    def _authenticate(self, credentials: HTTPAuthorizationCredentials | None) -> Principal:
        if credentials is None or credentials.scheme.lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Credenciales requeridas",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = credentials.credentials
        if secrets.compare_digest(token, self._settings.operator_token):
            return Principal(subject="operator", role=Role.OPERATOR)
        if secrets.compare_digest(token, self._settings.viewer_token):
            return Principal(subject="viewer", role=Role.VIEWER)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    def require(self, minimum_role: Role) -> Callable[..., Principal]:
        async def dependency(
            credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(BEARER_SCHEME)],
        ) -> Principal:
            principal = self._authenticate(credentials)
            if principal.role < minimum_role:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permisos insuficientes")
            return principal

        return dependency
