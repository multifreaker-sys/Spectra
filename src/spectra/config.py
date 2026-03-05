"""Centralised configuration — every setting comes from env vars / .env file."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("spectra")


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """All Spectra settings, loaded from environment or .env file."""

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # ── Google Sheets ────────────────────────────────────────────
    google_sheets_credentials_b64: str = ""
    google_sheets_credentials_file: str = "credentials.json"
    spreadsheet_id: str = ""

    # ── Base Currency ────────────────────────────────────────────
    base_currency: str = Field(default="EUR")

    @field_validator("base_currency")
    @classmethod
    def _uppercase_currency(cls, v: str) -> str:
        return v.strip().upper()

    # ── AI Provider ──────────────────────────────────────────────
    ai_provider: Literal["gemini", "openai", "local"] = Field(
        default="gemini",
        validation_alias=AliasChoices("AI_PROVIDER", "AI_PrOVIDER"),
    )

    @field_validator("ai_provider", mode="before")
    @classmethod
    def _normalize_provider(cls, v: str) -> str:
        return v.strip().lower() if isinstance(v, str) else v

    gemini_api_key: str = ""
    gemini_model: str = "gemma-3-27b-it"

    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    # ── Database ─────────────────────────────────────────────────
    db_path: Path = Field(default=_PROJECT_ROOT / "data" / "prism.db")

    # ── Behaviour ────────────────────────────────────────────────
    log_level: str = "INFO"

    # ── Validation ───────────────────────────────────────────────
    @model_validator(mode="after")
    def _check_required_secrets(self) -> "Settings":
        """Warn (don't crash) about missing secrets."""
        if not self.db_path.is_absolute():
            self.db_path = (_PROJECT_ROOT / self.db_path).resolve()

        credentials_file = Path(self.google_sheets_credentials_file)
        if not credentials_file.is_absolute():
            credentials_file = (_PROJECT_ROOT / credentials_file).resolve()
            self.google_sheets_credentials_file = str(credentials_file)

        missing: list[str] = []

        if not self.spreadsheet_id:
            missing.append("SPREADSHEET_ID")

        if not self.google_sheets_credentials_b64 and not credentials_file.exists():
            missing.append(
                "GOOGLE_SHEETS_CREDENTIALS_B64 or GOOGLE_SHEETS_CREDENTIALS_FILE"
            )

        if self.ai_provider == "gemini" and not self.gemini_api_key:
            missing.append("GEMINI_API_KEY")
        elif self.ai_provider == "openai" and not self.openai_api_key:
            missing.append("OPENAI_API_KEY")
        # 'local' mode needs no API keys

        if missing:
            logger.warning(
                "Missing secrets (some features may fail): %s", ", ".join(missing)
            )

        return self


def load_settings() -> Settings:
    """Load settings and configure logging."""
    settings = Settings()  # type: ignore[call-arg]

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(name)-12s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info(
        "Spectra config loaded (provider=%s, db=%s, env=%s)",
        settings.ai_provider,
        settings.db_path,
        _ENV_FILE,
    )
    return settings
