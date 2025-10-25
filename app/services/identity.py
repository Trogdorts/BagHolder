"""Identity and authentication service layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.auth import hash_password, verify_password
from app.core.models import User


@dataclass(slots=True)
class IdentityOperationResult:
    """Represents the outcome of an identity-related operation."""

    success: bool
    user: Optional[User] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass(slots=True)
class PasswordValidationResult:
    """Outcome of validating a password against the policy."""

    valid: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class PasswordPolicy:
    """Encapsulates password strength requirements."""

    def __init__(self, *, minimum_length: int = 8) -> None:
        self.minimum_length = minimum_length

    def validate(self, password: str) -> PasswordValidationResult:
        if not isinstance(password, str) or not password:
            return PasswordValidationResult(
                valid=False,
                error_code="missing_fields",
                error_message="Password is required.",
            )

        if len(password) < self.minimum_length:
            return PasswordValidationResult(
                valid=False,
                error_code="too_short",
                error_message=f"Password must be at least {self.minimum_length} characters long.",
            )

        return PasswordValidationResult(valid=True)


class UserRepository:
    """Data-access layer for :class:`~app.core.models.User` records."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def session(self) -> Session:
        return self._session

    def count(self) -> int:
        return int(self._session.query(func.count(User.id)).scalar() or 0)

    def get_by_username(self, username: str) -> Optional[User]:
        return self._session.query(User).filter(User.username == username).first()

    def get_by_id(self, user_id: int) -> Optional[User]:
        return self._session.get(User, user_id)

    def add(self, user: User) -> User:
        self._session.add(user)
        self._session.commit()
        self._session.refresh(user)
        return user

    def save(self) -> None:
        self._session.commit()


class IdentityService:
    """High-level user account and authentication operations."""

    def __init__(self, session: Session, *, password_policy: Optional[PasswordPolicy] = None) -> None:
        self._repo = UserRepository(session)
        self._policy = password_policy or PasswordPolicy()

    @property
    def session(self) -> Session:
        return self._repo.session

    def allow_self_registration(self) -> bool:
        """Return ``True`` if self-service registration is currently permitted."""

        return self._repo.count() == 0

    def authenticate(self, username: str, password: str) -> IdentityOperationResult:
        normalized = self._normalize_username(username)
        if not normalized or not password:
            return IdentityOperationResult(
                success=False,
                error_code="missing_credentials",
                error_message="Username and password are required.",
            )

        user = self._repo.get_by_username(normalized)
        if user is None:
            return IdentityOperationResult(
                success=False,
                error_code="invalid_credentials",
                error_message="Invalid username or password.",
            )

        if not verify_password(password, user.password_salt, user.password_hash):
            return IdentityOperationResult(
                success=False,
                error_code="invalid_credentials",
                error_message="Invalid username or password.",
            )

        return IdentityOperationResult(success=True, user=user)

    def register(self, username: str, password: str, *, confirm_password: str) -> IdentityOperationResult:
        user_count = self._repo.count()
        if user_count > 0:
            return IdentityOperationResult(
                success=False,
                error_code="registration_disabled",
                error_message="Registration is disabled once an account exists.",
            )

        normalized = self._normalize_username(username)
        if not normalized:
            return IdentityOperationResult(
                success=False,
                error_code="username_required",
                error_message="Username is required.",
            )

        validation = self._policy.validate(password)
        if not validation.valid:
            return IdentityOperationResult(
                success=False,
                error_code=validation.error_code,
                error_message=validation.error_message,
            )

        if password != confirm_password:
            return IdentityOperationResult(
                success=False,
                error_code="mismatch",
                error_message="Passwords do not match.",
            )

        if self._repo.get_by_username(normalized) is not None:
            return IdentityOperationResult(
                success=False,
                error_code="username_taken",
                error_message="Username is already in use.",
            )

        salt, password_hash = hash_password(password)
        user = User(
            username=normalized,
            password_hash=password_hash,
            password_salt=salt,
            is_admin=user_count == 0,
        )

        try:
            persisted = self._repo.add(user)
        except SQLAlchemyError:
            self.session.rollback()
            return IdentityOperationResult(
                success=False,
                error_code="unknown",
                error_message="Failed to create the account due to an unexpected error.",
            )

        return IdentityOperationResult(success=True, user=persisted)

    def change_password(
        self,
        user_id: int,
        current_password: str,
        new_password: str,
        *,
        confirm_password: str,
    ) -> IdentityOperationResult:
        if not all([current_password, new_password, confirm_password]):
            return IdentityOperationResult(
                success=False,
                error_code="missing_fields",
                error_message="All password fields are required.",
            )

        validation = self._policy.validate(new_password)
        if not validation.valid:
            return IdentityOperationResult(
                success=False,
                error_code=validation.error_code,
                error_message=validation.error_message,
            )

        if new_password != confirm_password:
            return IdentityOperationResult(
                success=False,
                error_code="mismatch",
                error_message="Passwords do not match.",
            )

        user = self._repo.get_by_id(user_id)
        if user is None:
            return IdentityOperationResult(
                success=False,
                error_code="missing_user",
                error_message="User could not be found.",
            )

        if not verify_password(current_password, user.password_salt, user.password_hash):
            return IdentityOperationResult(
                success=False,
                error_code="invalid_current",
                error_message="Current password is incorrect.",
            )

        salt, password_hash = hash_password(new_password)
        user.password_salt = salt
        user.password_hash = password_hash

        try:
            self._repo.save()
        except SQLAlchemyError:
            self.session.rollback()
            return IdentityOperationResult(
                success=False,
                error_code="unknown",
                error_message="Failed to update the password due to an unexpected error.",
            )

        return IdentityOperationResult(success=True, user=user)

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        return self._repo.get_by_id(user_id)

    @staticmethod
    def _normalize_username(username: str | None) -> str:
        if not isinstance(username, str):
            return ""
        return username.strip().lower()
