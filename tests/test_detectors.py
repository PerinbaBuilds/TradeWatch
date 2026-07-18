from __future__ import annotations

from tradewatch.config import (
    PriceSpikeConfig,
    SpoofingConfig,
    VelocityConfig,
    VolumeSpikeConfig,
    WashTradeConfig,
    ZScoreConfig,
)
from tradewatch.detectors import (
    PriceSpikeDetector,
    SpoofingDetector,
    VelocityDetector,
    VolumeSpikeDetector,
    WashTradeDetector,
    ZScoreDetector,
)
from tradewatch.models import Severity, Side
from tradewatch.windows import SymbolWindow

from .conftest import make_trade


def _window(trades):
    win = SymbolWindow(symbol="AAPL", max_trades=1000, horizon_seconds=1e9)
    for t in trades:
        win.add(t)
    return win


# --- z-score -----------------------------------------------------------------
def test_zscore_flags_price_outlier():
    trades = [make_trade(price=100 + (i % 3 - 1) * 0.5, offset_seconds=i) for i in range(30)]
    spike = make_trade(price=115, offset_seconds=30)
    trades.append(spike)
    win = _window(trades)
    det = ZScoreDetector(ZScoreConfig(), min_trades=20)
    alert = det.inspect(spike, win)
    assert alert is not None
    assert alert.detector == "zscore"
    assert alert.severity in (Severity.HIGH, Severity.CRITICAL)


def test_zscore_silent_during_warmup():
    trades = [make_trade(price=100, offset_seconds=i) for i in range(5)]
    win = _window(trades)
    det = ZScoreDetector(ZScoreConfig(), min_trades=20)
    assert det.inspect(trades[-1], win) is None


def test_zscore_silent_when_normal():
    trades = [make_trade(price=100 + (i % 3 - 1) * 0.5, offset_seconds=i) for i in range(40)]
    win = _window(trades)
    det = ZScoreDetector(ZScoreConfig(), min_trades=20)
    assert det.inspect(trades[-1], win) is None


# --- price spike -------------------------------------------------------------
def test_price_spike_detects_jump():
    prev = make_trade(price=100, offset_seconds=0)
    jump = make_trade(price=110, offset_seconds=1)  # +10%
    win = _window([prev, jump])
    det = PriceSpikeDetector(PriceSpikeConfig())
    alert = det.inspect(jump, win)
    assert alert is not None
    assert alert.severity is Severity.CRITICAL  # > critical_pct (8%)


def test_price_spike_ignores_small_move():
    prev = make_trade(price=100, offset_seconds=0)
    small = make_trade(price=100.5, offset_seconds=1)
    win = _window([prev, small])
    det = PriceSpikeDetector(PriceSpikeConfig())
    assert det.inspect(small, win) is None


# --- volume spike ------------------------------------------------------------
def test_volume_spike_detects_block():
    trades = [make_trade(quantity=100 + (i % 5), offset_seconds=i) for i in range(30)]
    block = make_trade(quantity=1200, offset_seconds=30)
    trades.append(block)
    win = _window(trades)
    det = VolumeSpikeDetector(VolumeSpikeConfig(), min_trades=20)
    alert = det.inspect(block, win)
    assert alert is not None
    assert alert.details["ratio"] >= 6.0


# --- velocity ----------------------------------------------------------------
def test_velocity_detects_burst():
    trades = [make_trade(offset_seconds=i * 0.1) for i in range(30)]  # 30 trades in 3s
    win = _window(trades)
    det = VelocityDetector(VelocityConfig())
    alert = det.inspect(trades[-1], win)
    assert alert is not None
    assert alert.details["count"] >= 25


def test_velocity_silent_when_slow():
    trades = [make_trade(offset_seconds=i * 2.0) for i in range(30)]  # spread out
    win = _window(trades)
    det = VelocityDetector(VelocityConfig())
    assert det.inspect(trades[-1], win) is None


# --- spoofing ----------------------------------------------------------------
def test_spoofing_detects_one_sided_burst():
    trades = [make_trade(side=Side.BUY, offset_seconds=i * 0.1) for i in range(12)]
    win = _window(trades)
    det = SpoofingDetector(SpoofingConfig())
    alert = det.inspect(trades[-1], win)
    assert alert is not None
    assert alert.details["dominant_side"] == "buy"


def test_spoofing_silent_when_balanced():
    trades = []
    for i in range(12):
        side = Side.BUY if i % 2 == 0 else Side.SELL
        trades.append(make_trade(side=side, offset_seconds=i * 0.1))
    win = _window(trades)
    det = SpoofingDetector(SpoofingConfig())
    assert det.inspect(trades[-1], win) is None


# --- wash trade --------------------------------------------------------------
def test_wash_trade_detects_self_dealing():
    buy = make_trade(side=Side.BUY, price=100.0, account_id="acct_1", offset_seconds=0)
    sell = make_trade(side=Side.SELL, price=100.0, account_id="acct_1", offset_seconds=2)
    win = _window([buy, sell])
    det = WashTradeDetector(WashTradeConfig())
    alert = det.inspect(sell, win)
    assert alert is not None
    assert alert.severity is Severity.CRITICAL
    assert alert.details["account_id"] == "acct_1"


def test_wash_trade_ignores_different_accounts():
    buy = make_trade(side=Side.BUY, price=100.0, account_id="acct_1", offset_seconds=0)
    sell = make_trade(side=Side.SELL, price=100.0, account_id="acct_2", offset_seconds=2)
    win = _window([buy, sell])
    det = WashTradeDetector(WashTradeConfig())
    assert det.inspect(sell, win) is None
