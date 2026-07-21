"""Configuration for the detection engine.

Detection thresholds live in a YAML file (``config/detection_rules.yaml`` by
default) so risk/compliance teams can tune sensitivity without touching code.
Runtime/service settings come from environment variables (``TRADEWATCH_*``)
via pydantic-settings, which keeps secrets and deployment knobs out of source.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "detection_rules.yaml"


class WindowConfig(BaseModel):
    """Sliding-window sizing shared by the statistical detectors."""

    max_trades: int = 200
    min_trades: int = 20
    horizon_seconds: float = 300.0


class ZScoreConfig(BaseModel):
    enabled: bool = True
    price_threshold: float = 3.0
    critical_multiplier: float = 1.6


class PriceSpikeConfig(BaseModel):
    enabled: bool = True
    pct_threshold: float = 0.03
    critical_pct: float = 0.08


class VolumeSpikeConfig(BaseModel):
    enabled: bool = True
    median_multiplier: float = 6.0
    critical_multiplier: float = 15.0


class VelocityConfig(BaseModel):
    enabled: bool = True
    window_seconds: float = 5.0
    max_trades: int = 25
    critical_trades: int = 60


class SpoofingConfig(BaseModel):
    enabled: bool = True
    window_seconds: float = 3.0
    imbalance_ratio: float = 5.0
    min_events: int = 8


class WashTradeConfig(BaseModel):
    enabled: bool = True
    window_seconds: float = 5.0
    price_tolerance: float = 0.001
    quantity_tolerance: float = 0.02


class IsolationForestConfig(BaseModel):
    enabled: bool = True
    train_size: int = 300
    retrain_every: int = 150
    contamination: float = 0.02
    score_threshold: float = 0.62


class DetectionConfig(BaseModel):
    """Full detection ruleset."""

    #: Suppress repeat alerts from the same (symbol, detector) within this many
    #: seconds of event-time. Collapses a multi-tick episode into one alert and
    #: kills burst-aftermath noise. Set to 0 to disable deduplication.
    alert_cooldown_seconds: float = 8.0

    window: WindowConfig = Field(default_factory=WindowConfig)
    zscore: ZScoreConfig = Field(default_factory=ZScoreConfig)
    price_spike: PriceSpikeConfig = Field(default_factory=PriceSpikeConfig)
    volume_spike: VolumeSpikeConfig = Field(default_factory=VolumeSpikeConfig)
    velocity: VelocityConfig = Field(default_factory=VelocityConfig)
    spoofing: SpoofingConfig = Field(default_factory=SpoofingConfig)
    wash_trade: WashTradeConfig = Field(default_factory=WashTradeConfig)
    isolation_forest: IsolationForestConfig = Field(default_factory=IsolationForestConfig)

    @classmethod
    def load(cls, path: str | Path | None = None) -> DetectionConfig:
        """Load rules from YAML, falling back to built-in defaults."""
        target = Path(path) if path else _DEFAULT_RULES_PATH
        if not target.exists():
            return cls()
        with target.open("r", encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh) or {}
        return cls.model_validate(raw)


class Settings(BaseSettings):
    """Service-level settings, overridable via ``TRADEWATCH_*`` env vars."""

    model_config = SettingsConfigDict(env_prefix="TRADEWATCH_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000
    rules_path: str | None = None
    alert_buffer_size: int = 500

    # Which trade source backs the background pipeline: "simulator" or "kafka".
    source: str = "simulator"

    # Built-in simulator (used when source == "simulator").
    simulator_enabled: bool = True
    simulator_symbols: str = "AAPL,MSFT,BTC-USD,ETH-USD,TSLA"
    simulator_trades_per_second: float = 20.0
    simulator_anomaly_rate: float = 0.015
    simulator_seed: int | None = None

    # Kafka consumer (used when source == "kafka"). Requires the [kafka] extra.
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "trades"
    kafka_group_id: str = "tradewatch"
    kafka_auto_offset_reset: str = "latest"

    # --- Security & observability ---
    api_key: str | None = None            # if set, POST /trades requires X-API-Key
    rate_limit_per_min: int = 600         # per-client cap on POST /trades (0 = off)
    cors_origins: str = ""                # comma-separated allowlist ("" = none)
    log_json: bool = False                # structured JSON logs
    log_level: str = "INFO"
    audit_log_path: str | None = None     # JSONL audit trail (None = stderr)
    guardrails_enabled: bool = True

    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.simulator_symbols.split(",") if s.strip()]

    def cors_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
