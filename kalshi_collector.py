"""Collect Kalshi market data: exchange-wide trade tape, market metadata, per-series trades.

Three modes, in decreasing order of what they cover per request:

  --tape START END    Every trade on the exchange, day by day. The trade endpoints
                      accept min_ts/max_ts with NO ticker, so one cursor walk per day
                      captures all series at once. This is the only practical way to
                      back-fill "all markets": the per-market path below costs one
                      request per market and the exchange has millions of them.
  --markets           Market metadata (strikes, close_time, volume, settlement) for
                      every series, via per-series cursor walks over both tiers.
                      1 request per 1000 markets. Join onto the tape on `ticker`.
  --update            Legacy per-event path: series -> /events -> markets -> trades.
                      Exact and self-verifying, but ~1 request per market. Use it to
                      patch individual series, not to sweep the exchange.

API quirks this file works around (all measured, 2026-08-04):

  * min_close_ts / max_close_ts are SILENTLY IGNORED by /events and /historical/markets.
    Passing a Jan-2026 window to /events?series_ticker=KXNBAGAME returns the exact same
    page as passing nothing (Mar/Apr/May/Jun events). Date ranges must be applied
    client-side, and any "incremental" run still pays for the full enumeration.
  * /markets and /historical/markets paginate. Requesting limit=200 for an event with
    400 markets (KXINXU-26JUN04H1500) returns 200 and a cursor; ignoring that cursor
    silently drops half the strike ladder.
  * Neither trade tier is complete near the boundary. The live tape reaches ~6 days
    further back than /historical/cutoff claims and silently drops the oldest trades of
    markets that straddle it, so trades are read from both tiers and unioned.
  * Retrying only 429 is not enough; a single 504 kills a long cursor walk.

Output (parquet, under data/kalshi/):
  tape/<date>/part-*.parquet    one row per exchange fill that day (--tape)
  markets/<series>.parquet      one row per market (--markets)
  <category>/<series>/<event>.parquet   one row per trade, event-partitioned (--update)

Usage:
  python kalshi_collector.py --tape 2025-10-01 2026-06-30
  python kalshi_collector.py --markets
  python kalshi_collector.py --update --category crypto
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

UTC = timezone.utc

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DATA_ROOT = os.environ.get("KALSHI_DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "kalshi"))
TRADE_PAGE_LIMIT = 1000
CONCURRENT_MARKET_WORKERS = 6
# Conservative rates tuned for the rate-limited /historical/* endpoints. The live
# /markets/* endpoints tolerate much more, but /events and /historical/trades 429 hard
# under concurrency, so keep these low and run a single process.
CONCURRENT_EVENT_WORKERS = 4
MAX_REQUESTS_PER_SEC = 18
MAX_INFLIGHT_REQUESTS = 6
LOOKBACK_DAYS = 120  # 4 months

# A cursor walk is sequential, so a single day tops out at ~3 req/s no matter what the
# rate limiter allows. Days are independent, so concurrency comes from running several
# of them at once; the global limiter still caps the aggregate.
TAPE_DAY_WORKERS = 6
TAPE_FLUSH_ROWS = 500_000  # bound peak memory: a busy day is ~4.2M trades

CATEGORIES = ["Sports", "Politics", "Crypto"]
TOP_N = 10

TRADE_COLS = [
    "trade_id", "ticker", "created_time", "count_fp",
    "yes_price_dollars", "no_price_dollars",
    "taker_side", "taker_book_side", "taker_outcome_side", "is_block_trade",
]

MARKET_COLS = [
    "ticker", "event_ticker", "market_type", "status", "result",
    "open_time", "close_time", "expected_expiration_time", "expiration_time",
    "settlement_ts", "volume_fp", "open_interest_fp", "liquidity_dollars",
    "notional_value_dollars", "last_price_dollars", "previous_price_dollars",
    "yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
    "title", "yes_sub_title", "no_sub_title", "expiration_value",
    "custom_strike", "rules_primary",
]


_thread_local = threading.local()
_rate_lock = threading.Lock()
_last_request_time = 0.0
_inflight = threading.Semaphore(MAX_INFLIGHT_REQUESTS)


def _rate_limit():
    """Enforce a global rate limit across all threads."""
    global _last_request_time
    interval = 1.0 / MAX_REQUESTS_PER_SEC
    with _rate_lock:
        wait = _last_request_time + interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def _check_connectivity():
    """Fail fast if TLS/certs are broken (e.g. .venv was rebuilt by another process)."""
    try:
        _get_session().get(f"{BASE_URL}/series", params={"limit": 1}, timeout=10)
    except OSError as e:
        if "certificate" in str(e).lower() or "TLS" in str(e):
            logger.error("TLS certificate bundle is broken. Run: uv sync")
            raise SystemExit(1)
        raise


RETRY_STATUS = (429, 500, 502, 503, 504)


def _get(path: str, params: dict | None = None) -> dict:
    """GET with backoff on rate limits, gateway errors and transport failures.

    Retrying only 429 loses long cursor walks to a single transient 504.
    """
    resp = None
    for attempt in range(12):
        _rate_limit()
        try:
            with _inflight:
                resp = _get_session().get(f"{BASE_URL}{path}", params=params, timeout=60)
        except requests.RequestException as e:
            wait = min(2 ** attempt, 60)
            logger.warning("%s on %s, retrying in %ds", type(e).__name__, path, wait)
            time.sleep(wait)
            continue
        if resp.status_code in RETRY_STATUS:
            wait = min(2 ** attempt, 120)
            logger.warning("%d on %s, retrying in %ds", resp.status_code, path, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    if resp is None:
        raise RuntimeError(f"no response from {path} after retries")
    resp.raise_for_status()
    return resp.json()


def _paginate_pages(path: str, params: dict, key: str, start_cursor: str = ""):
    """Yield (page_items, cursor_after_that_page) across a cursor-paginated endpoint.

    Callers that checkpoint must do so on this boundary. Checkpointing part-way through
    a page would record the *next* page's cursor while the rest of the current page is
    still unwritten, so a crash right after would skip those rows silently.
    """
    cursor = start_cursor
    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor
        data = _get(path, p)
        page = data.get(key, [])
        cursor = data.get("cursor", "") or ""
        yield page, cursor
        if not cursor or not page:
            return


def _paginate(path: str, params: dict, key: str, start_cursor: str = ""):
    """Yield every item across a cursor-paginated endpoint, with the cursor after it."""
    for page, cursor in _paginate_pages(path, params, key, start_cursor):
        for item in page:
            yield item, cursor


# --------------------------------------------------------------------------- #
# Tier routing
# --------------------------------------------------------------------------- #

_HISTORICAL_CUTOFF_ISO: str | None = None
_cutoff_lock = threading.Lock()


def _historical_cutoff() -> str:
    """Returns the trades-created cutoff ISO string. Older trades live in /historical/*."""
    global _HISTORICAL_CUTOFF_ISO
    with _cutoff_lock:
        if _HISTORICAL_CUTOFF_ISO is None:
            data = _get("/historical/cutoff")
            _HISTORICAL_CUTOFF_ISO = data.get("trades_created_ts", "")
            logger.info("Kalshi historical cutoff: %s (older data via /historical/*)", _HISTORICAL_CUTOFF_ISO)
    return _HISTORICAL_CUTOFF_ISO


def _is_historical(close_time_iso: str) -> bool:
    """Whether to *prefer* the historical tier. Both tiers are still consulted."""
    cutoff = _historical_cutoff()
    return bool(close_time_iso) and bool(cutoff) and close_time_iso < cutoff


def _write_parquet(df: pd.DataFrame, path: str):
    """Write atomically, so a kill mid-write can never leave a half file behind.

    Without this, an interrupted write leaves a truncated parquet that every later run
    treats as done, because the resume logic only ever asked whether the path exists.
    """
    tmp = f"{path}.tmp"
    df.to_parquet(tmp, index=False, engine="pyarrow")
    os.replace(tmp, path)


def _parquet_rows(path: str) -> int:
    """Row count of a parquet file, or -1 if it is missing or unreadable.

    Reads the footer only, so this is cheap enough to run over every file at startup.
    """
    import pyarrow.parquet as pq

    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return -1


def _usable(path: str, *, allow_empty: bool = True) -> bool:
    """Whether an output file counts as already collected.

    Existence is not enough: an empty or truncated parquet has to be re-fetched, not
    skipped. Callers that can legitimately produce zero rows pass allow_empty=True.
    """
    if not os.path.exists(path):
        return False
    n = _parquet_rows(path)
    if n < 0:
        return False
    return allow_empty or n > 0


def _norm_price(int_val, dollars_str) -> int | None:
    """Normalize yes_price/no_price to integer cents (1-99) regardless of API tier."""
    if int_val is not None:
        return int(int_val)
    if dollars_str is None or dollars_str == "":
        return None
    return int(round(float(dollars_str) * 100))


def _norm_count(int_val, fp_str) -> int | None:
    """Normalize trade count to integer regardless of API tier."""
    if int_val is not None:
        return int(int_val)
    if fp_str is None or fp_str == "":
        return None
    return int(float(fp_str))


# --------------------------------------------------------------------------- #
# Tape mode: stream the exchange-wide trade feed, all series at once
# --------------------------------------------------------------------------- #

def _day_dir(day: pd.Timestamp) -> str:
    return os.path.join(DATA_ROOT, "tape", f"{day:%Y-%m-%d}")


def _day_done(out: str) -> bool:
    """Whether a tape day is finished AND its parts are all readable.

    The _COMPLETE marker is written last, so its presence normally implies the parts
    landed. It still gets verified: a marker with no parts, or a part whose footer will
    not parse, means the day has to be redone rather than skipped forever.
    """
    if not os.path.exists(os.path.join(out, "_COMPLETE")):
        return False
    parts = glob.glob(os.path.join(out, "part-*.parquet"))
    if not parts:
        return False
    return all(_parquet_rows(p) >= 0 for p in parts)


def _reset_day(out: str):
    """Drop a day's outputs so it is re-downloaded from scratch."""
    for p in glob.glob(os.path.join(out, "*")):
        os.remove(p)


def _tape_day(day: pd.Timestamp) -> tuple[str, int, int]:
    """Download one UTC day of the exchange tape. Returns (date, kept, scanned).

    Resumable mid-day: the cursor and part number are checkpointed on every flush, so a
    killed run re-enters where it stopped instead of replaying millions of trades.
    """
    date = f"{day:%Y-%m-%d}"
    out = _day_dir(day)
    done = os.path.join(out, "_COMPLETE")
    if _day_done(out):
        return date, -1, -1
    if os.path.exists(done):
        logger.warning("[tape] %s: marked complete but its parts do not read back; redoing", date)
        _reset_day(out)

    os.makedirs(out, exist_ok=True)
    state_path = os.path.join(out, "_state.json")
    state = {"tiers": None, "tier_idx": 0, "cursor": "", "part": 0, "kept": 0, "scanned": 0}
    if os.path.exists(state_path):
        try:
            state |= json.load(open(state_path))
        except (OSError, ValueError):
            logger.warning("[tape] %s: unreadable checkpoint, restarting the day", date)

    # min_ts is inclusive and max_ts exclusive, so [day, day+1) partitions the tape
    # exactly. Which tier serves a day is NOT what /historical/cutoff says -- the live
    # tape reaches further back than advertised and the boundary moves day to day -- so
    # probe both and read whichever actually returns rows, unioning inside the overlap.
    window = {"min_ts": int(day.timestamp()),
              "max_ts": int((day + pd.Timedelta(days=1)).timestamp()),
              "limit": TRADE_PAGE_LIMIT}
    if state["tiers"] is None:
        tiers = [p for p in ("/historical/trades", "/markets/trades")
                 if _get(p, dict(window, limit=1)).get("trades")]
        state["tiers"] = tiers
    tiers = state["tiers"]
    if not tiers:
        _write_parquet(pd.DataFrame(columns=TRADE_COLS), os.path.join(out, "part-0000.parquet"))
        open(done, "w").close()
        logger.info("[tape] %s: no trades on either tier", date)
        return date, 0, 0

    # Both tiers serve the days around the cutoff, so their pages overlap and the union
    # has to be deduped on trade_id. Only pay for the id set when it can actually
    # happen: single-tier days (the vast majority) cannot produce a duplicate. On a
    # resume the set is rebuilt from the parts already on disk.
    seen: set[str] | None = None
    if len(tiers) > 1:
        seen = set()
        for p in sorted(glob.glob(os.path.join(out, "part-*.parquet"))):
            try:
                seen |= set(pd.read_parquet(p, columns=["trade_id"])["trade_id"].dropna())
            except Exception:
                logger.warning("[tape] %s: could not re-read %s for dedup", date, os.path.basename(p))

    buf: list[dict] = []

    def flush(cursor: str, tier_idx: int):
        if buf:
            path = os.path.join(out, f"part-{state['part']:04d}.parquet")
            _write_parquet(pd.DataFrame(buf, columns=TRADE_COLS), path)
            state["part"] += 1
            buf.clear()
        state["cursor"] = cursor
        state["tier_idx"] = tier_idx
        tmp = state_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(state, fh)
        os.replace(tmp, state_path)

    # flush() mutates state["tier_idx"], so capture where the checkpoint left off first.
    resume_idx, resume_cursor = state["tier_idx"], state["cursor"]
    for idx in range(resume_idx, len(tiers)):
        path = tiers[idx]
        cursor = resume_cursor if idx == resume_idx else ""
        prev_decade = state["kept"] // 100_000
        for page, next_cursor in _paginate_pages(path, window, "trades", start_cursor=cursor):
            for t in page:
                state["scanned"] += 1
                if seen is not None:
                    if t["trade_id"] in seen:
                        continue
                    seen.add(t["trade_id"])
                buf.append({c: t.get(c) for c in TRADE_COLS})
                state["kept"] += 1
            # Report on trade count, not on flush: a day under TAPE_FLUSH_ROWS would
            # otherwise run to completion in total silence.
            if state["kept"] // 100_000 > prev_decade:
                prev_decade = state["kept"] // 100_000
                logger.info("  [tape] %s: %d trades", date, state["kept"])
            # Flush only on a page boundary, so the checkpointed cursor and the rows on
            # disk describe exactly the same prefix of the stream.
            if len(buf) >= TAPE_FLUSH_ROWS:
                flush(next_cursor, idx)
                logger.info("  [tape] %s: %d trades stored", date, state["kept"])
        flush("", idx + 1)

    if state["part"] == 0:
        # Tier probe saw rows but the walk yielded none; still leave a readable file so
        # a zero-trade day is distinguishable from a day that was never attempted.
        _write_parquet(pd.DataFrame(columns=TRADE_COLS), os.path.join(out, "part-0000.parquet"))
    open(done, "w").close()
    os.remove(state_path)
    logger.info("[tape] %s (%s): %d trades, %d parts",
                date, "+".join(p.split("/")[1] for p in tiers), state["kept"], state["part"])
    return date, state["kept"], state["scanned"]


def run_tape(start: str, end: str, workers: int = TAPE_DAY_WORKERS):
    """Download the exchange tape for every UTC day in [start, end]."""
    days = pd.date_range(start, end, freq="D", tz="UTC")
    todo = [d for d in days if not _day_done(_day_dir(d))]
    logger.info("[tape] %s..%s: %d days, %d already complete, %d workers",
                start, end, len(days), len(days) - len(todo), workers)
    if not todo:
        return

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_tape_day, d): d for d in todo}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                fut.result()
            except Exception:
                logger.exception("[tape] %s failed; re-run to resume from its checkpoint",
                                 f"{d:%Y-%m-%d}")
            done += 1
            rate = done / max(time.time() - t0, 1e-9)
            logger.info("[tape] %d/%d days done, %.1f day/h, eta %.1fh",
                        done, len(todo), rate * 3600, (len(todo) - done) / max(rate * 3600, 1e-9))


def tape_status(start: str | None = None, end: str | None = None):
    """Report per-day completeness of the local tape.

    Three separable failure modes, all of which a plain "does the directory exist"
    check would call healthy: a day never attempted (absent from the range), a day left
    half-written (no _COMPLETE), and a day whose parts no longer parse (corrupt).
    """
    rows = []
    for d in sorted(glob.glob(os.path.join(DATA_ROOT, "tape", "*"))):
        if not os.path.isdir(d):
            continue
        date = os.path.basename(d)
        if (start and date < start) or (end and date > end):
            continue
        parts = sorted(glob.glob(os.path.join(d, "part-*.parquet")))
        counts = [_parquet_rows(p) for p in parts]
        rows.append({"date": date,
                     "complete": os.path.exists(os.path.join(d, "_COMPLETE")),
                     "parts": len(parts),
                     "unreadable": sum(1 for c in counts if c < 0),
                     "trades": sum(c for c in counts if c >= 0)})
    t = pd.DataFrame(rows)
    if t.empty:
        logger.info("[status] no tape days on disk")
        return t

    missing = []
    if start and end:
        have = set(t.date)
        missing = [f"{d:%Y-%m-%d}" for d in pd.date_range(start, end, freq="D")
                   if f"{d:%Y-%m-%d}" not in have]

    logger.info("[status] %d days on disk, %d complete, %d incomplete, %d with unreadable parts, %s trades",
                len(t), int(t.complete.sum()), int((~t.complete).sum()),
                int((t.unreadable > 0).sum()), f"{t.trades.sum():,}")
    if missing:
        logger.warning("[status] %d days in range never collected: %s",
                       len(missing), ", ".join(missing[:10]) + (" ..." if len(missing) > 10 else ""))
    bad = t[~t.complete | (t.unreadable > 0) | (t.trades <= 0)]
    if not bad.empty:
        logger.warning("[status] %d days need a re-run (just re-invoke --tape; they are "
                       "no longer skipped):\n%s", len(bad), bad.to_string(index=False))
    if not missing and bad.empty:
        logger.info("[status] all days healthy")
    return t


# --------------------------------------------------------------------------- #
# Market metadata: one row per market, for the whole exchange
# --------------------------------------------------------------------------- #

def all_series() -> list[dict]:
    """Every series on the exchange, with lifetime volume."""
    s = _get("/series", {"include_volume": True}).get("series", [])
    logger.info("Exchange has %d series across %d categories",
                len(s), len({x.get("category") for x in s}))
    return s


def _markets_of_series(series_ticker: str) -> list[dict]:
    """Every market of a series, from both tiers, deduped on ticker.

    Both tiers are walked: /markets only serves post-cutoff markets and
    /historical/markets only pre-cutoff ones, and a sweep needs both.
    """
    out: dict[str, dict] = {}
    for path in ("/historical/markets", "/markets"):
        for m, _ in _paginate(path, {"series_ticker": series_ticker, "limit": 1000}, "markets"):
            row = {c: m.get(c) for c in MARKET_COLS}
            row["custom_strike"] = json.dumps(m.get("custom_strike")) if m.get("custom_strike") else None
            row["series_ticker"] = series_ticker
            out[m["ticker"]] = row
    return list(out.values())


def run_markets(series_filter: list[str] | None = None, min_volume: float = 0.0):
    """Store market metadata for every series (or a subset). ~1 request per 1000 markets."""
    _check_connectivity()
    out_dir = os.path.join(DATA_ROOT, "markets")
    os.makedirs(out_dir, exist_ok=True)

    series = all_series()
    if series_filter:
        wanted = {s.upper() for s in series_filter}
        series = [s for s in series if s["ticker"].upper() in wanted]
        skipped_mve = []
    else:
        # MVE (combo) series are instantiated per request and run to tens of millions of
        # markets each -- KXMVESPORTSMULTIGAMEEXTENDED alone has 33.5M -- which this
        # accumulate-then-write path cannot hold. kalshi_combo_collector.py already
        # stores them, so skip them here unless asked for by name.
        skipped_mve = [s["ticker"] for s in series if s["ticker"].startswith("KXMVE")]
        series = [s for s in series if not s["ticker"].startswith("KXMVE")]
    if skipped_mve:
        logger.info("[markets] skipping %d MVE series (see kalshi_combo_collector.py): %s",
                    len(skipped_mve), ", ".join(sorted(skipped_mve)[:4]) + " ...")
    if not series_filter and min_volume:
        series = [s for s in series
                  if float(s.get("volume_fp") or s.get("volume") or 0) >= min_volume]
    series.sort(key=lambda s: float(s.get("volume_fp") or s.get("volume") or 0), reverse=True)

    todo = [s for s in series if not _usable(os.path.join(out_dir, f"{s['ticker']}.parquet"))]
    logger.info("[markets] %d series selected, %d already stored, %d to fetch",
                len(series), len(series) - len(todo), len(todo))

    def _one(s: dict) -> tuple[str, int]:
        st = s["ticker"]
        rows = _markets_of_series(st)
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=MARKET_COLS + ["series_ticker"])
        _write_parquet(df, os.path.join(out_dir, f"{st}.parquet"))
        return st, len(rows)

    total = 0
    with ThreadPoolExecutor(max_workers=CONCURRENT_EVENT_WORKERS) as ex:
        futures = {ex.submit(_one, s): s for s in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                st, n = fut.result()
                total += n
            except Exception:
                logger.exception("[markets] %s failed", futures[fut]["ticker"])
                continue
            if i % 50 == 0 or i == len(todo):
                logger.info("[markets] %d/%d series, %d markets stored", i, len(todo), total)
    logger.info("[markets] done, %d markets", total)


# --------------------------------------------------------------------------- #
# Legacy per-event path
# --------------------------------------------------------------------------- #

def discover_top_series() -> dict[str, list[dict]]:
    """Return top N series per category, keyed by category name."""
    logger.info("Fetching all series with volume...")
    series = all_series()

    result: dict[str, list[dict]] = {}
    for cat in CATEGORIES:
        filtered = [s for s in series if s.get("category") == cat]
        # Kalshi migrated `volume` (int) to `volume_fp` (fixed-point string); keep the
        # old key as a fallback so this keeps ranking correctly across API tiers.
        filtered.sort(key=lambda s: float(s.get("volume_fp") or s.get("volume") or 0), reverse=True)
        result[cat] = filtered[:TOP_N]
        logger.info(
            "%s: selected %d series (top volume: %s)",
            cat, len(result[cat]),
            result[cat][0]["ticker"] if result[cat] else "none",
        )
    return result


def fetch_events(series_ticker: str, *, settled_only: bool = False) -> list[dict]:
    """Fetch full event objects for a series.

    min_close_ts / max_close_ts are accepted but ignored by /events (measured: a
    one-month window returns the same page as no window at all), so this enumerates
    the whole series and callers filter afterwards. Each event is annotated with
    `_query_status`, since /events does not include a status field on the event.
    """
    events: list[dict] = []
    statuses = ("settled",) if settled_only else ("settled", "open")
    for status in statuses:
        for e, _ in _paginate("/events", {"series_ticker": series_ticker,
                                          "status": status, "limit": 200}, "events"):
            e["_query_status"] = status
            events.append(e)
    return events


def fetch_markets(event_ticker: str, *, historical: bool = False) -> list[dict]:
    """Fetch all markets for an event.

    Paginated: an event can carry more markets than one page holds (KXINXU hourly
    ladders run 400), and reading only the first page silently halves the strikes.
    """
    path = "/historical/markets" if historical else "/markets"
    return [m for m, _ in _paginate(path, {"event_ticker": event_ticker, "limit": 1000}, "markets")]


def fetch_trades(ticker: str, close_time: str = "", volume: float | None = None) -> list[dict]:
    """Fetch a market's full tape from both tiers, unioned, verified against volume.

    Neither tier is complete on its own near the boundary: the live tape has a rolling
    retention window reaching ~6 days further back than /historical/cutoff claims, and
    it silently drops the OLDEST trades of markets that straddle it. So route to the
    likely tier first and consult the other whenever the summed count falls short of
    the market's volume.
    """
    paths = ["/markets/trades", "/historical/trades"]
    if _is_historical(close_time):
        paths.reverse()
    trades: dict[str, dict] = {}
    for path in paths:
        for t, _ in _paginate(path, {"ticker": ticker, "limit": TRADE_PAGE_LIMIT}, "trades"):
            trades[t["trade_id"]] = t
        got = sum(_norm_count(t.get("count"), t.get("count_fp")) or 0 for t in trades.values())
        if volume is not None and got >= volume:
            break
        if volume is None and trades:
            break
    return list(trades.values())


def _slug(category: str) -> str:
    """Directory-safe category name ('Climate and Weather' -> 'climate-and-weather')."""
    return re.sub(r"[^a-z0-9]+", "-", category.lower()).strip("-")


def _empty_marker(series_dir: str) -> str:
    return os.path.join(series_dir, "_no_trades.json")


def _load_empty(series_dir: str) -> set[str]:
    """Events already known to have zero trades.

    Without this an untraded event has no file, so every later run re-downloads its
    markets and trades forever. Untraded markets are the majority in some series.
    """
    try:
        return set(json.load(open(_empty_marker(series_dir))))
    except (OSError, ValueError):
        return set()


def _save_empty(series_dir: str, empty: set[str]):
    tmp = _empty_marker(series_dir) + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(sorted(empty), fh)
    os.replace(tmp, _empty_marker(series_dir))


def collect_series(series_ticker: str, series_title: str, category: str, out_dir: str,
                   min_close_ts: int, max_close_ts: int | None = None, *,
                   update_open: bool = False, settled_only: bool = False):
    """Collect all trades for a series, one parquet file per event."""
    series_dir = os.path.join(out_dir, series_ticker)
    os.makedirs(series_dir, exist_ok=True)

    # Validated, not merely listed: a zero-row or truncated event parquet must be
    # re-fetched instead of counting as collected forever.
    existing_events = {f.removesuffix(".parquet") for f in os.listdir(series_dir)
                       if f.endswith(".parquet")
                       and _usable(os.path.join(series_dir, f), allow_empty=False)}
    known_empty = _load_empty(series_dir)

    events = fetch_events(series_ticker, settled_only=settled_only)

    if update_open:
        # Skip already-downloaded events that came from the `settled` API query -- their
        # trades are final. Re-fetch open ones, which may have new ticks.
        events_to_process = [
            e for e in events
            if (e["event_ticker"] not in existing_events and e["event_ticker"] not in known_empty)
            or e.get("_query_status") == "open"
        ]
        n_reopened = sum(1 for e in events_to_process if e["event_ticker"] in existing_events)
        n_new = len(events_to_process) - n_reopened
        if not events_to_process:
            logger.info("  %s: nothing to update (%d events already collected)", series_ticker, len(existing_events))
            return
        logger.info(
            "  %s: %d events total, %d settled, %d new, %d open to re-fetch",
            series_ticker, len(events), len(existing_events) - n_reopened, n_new, n_reopened,
        )
    else:
        events_to_process = [e for e in events
                             if e["event_ticker"] not in existing_events
                             and e["event_ticker"] not in known_empty]
        if not events_to_process:
            logger.info("  %s: nothing new (%d events already collected, %d known empty)",
                        series_ticker, len(existing_events), len(known_empty))
            return
        logger.info(
            "  %s: %d events total, %d already collected, %d known empty, %d new",
            series_ticker, len(events), len(existing_events), len(known_empty), len(events_to_process),
        )

    empty_lock = threading.Lock()
    newly_empty: set[str] = set()

    def _process_event(event: dict) -> int:
        event_ticker = event["event_ticker"]
        event_title = event.get("title", "")
        event_sub_title = event.get("sub_title", "")

        # /events doesn't expose close_time on the event, so try live first and fall
        # back to historical if empty.
        try:
            markets = fetch_markets(event_ticker, historical=False)
            if not markets:
                markets = fetch_markets(event_ticker, historical=True)
        except Exception:
            logger.exception("    failed to fetch markets for %s", event_ticker)
            return 0

        # Client-side date filter: the API ignores min_close_ts/max_close_ts.
        def _in_range(m: dict) -> bool:
            ct = m.get("close_time") or ""
            if not ct:
                return True
            try:
                ts = pd.Timestamp(ct).timestamp()
            except ValueError:
                return True
            if ts < min_close_ts:
                return False
            return not (max_close_ts and ts > max_close_ts)

        markets = [m for m in markets if _in_range(m)]
        if not markets:
            with empty_lock:
                newly_empty.add(event_ticker)
            return 0

        def _fetch_market_trades(market: dict) -> tuple[dict, list[dict], bool]:
            try:
                vol = _norm_count(market.get("volume"), market.get("volume_fp"))
                trades = fetch_trades(market["ticker"], market.get("close_time", ""), vol)
                return market, trades, True
            except Exception:
                logger.exception("    failed to fetch trades for %s", market["ticker"])
                return market, [], False

        market_results: list[tuple[dict, list[dict], bool]] = []
        with ThreadPoolExecutor(max_workers=min(len(markets), CONCURRENT_MARKET_WORKERS)) as executor:
            futures = {executor.submit(_fetch_market_trades, m): m for m in markets}
            for future in as_completed(futures):
                market_results.append(future.result())

        if any(not ok for _, _, ok in market_results):
            logger.warning("    %s: skipping write -- some markets failed to fetch, will retry next run", event_ticker)
            return 0

        event_rows: list[dict] = []
        for m, trades, _ok in market_results:
            ticker = m["ticker"]
            for t in trades:
                event_rows.append({
                    "category": category,
                    "series_ticker": series_ticker,
                    "series_title": series_title,
                    "event_ticker": event_ticker,
                    "event_title": event_title,
                    "event_sub_title": event_sub_title,
                    "ticker": ticker,
                    "market_title": m.get("title", ""),
                    "market_type": m.get("market_type", ""),
                    "result": m.get("result"),
                    "expiration_value": m.get("expiration_value", ""),
                    "volume": _norm_count(m.get("volume"), m.get("volume_fp")) or 0,
                    "open_interest": _norm_count(m.get("open_interest"), m.get("open_interest_fp")) or 0,
                    "open_time": m.get("open_time", ""),
                    "close_time": m.get("close_time", ""),
                    "expected_expiration_time": m.get("expected_expiration_time", ""),
                    "settlement_ts": m.get("settlement_ts", ""),
                    "status": m.get("status", ""),
                    "rules_primary": m.get("rules_primary", ""),
                    "yes_sub_title": m.get("yes_sub_title", ""),
                    "no_sub_title": m.get("no_sub_title", ""),
                    "trade_id": t.get("trade_id"),
                    "created_time": t.get("created_time"),
                    "yes_price": _norm_price(t.get("yes_price"), t.get("yes_price_dollars")),
                    "no_price": _norm_price(t.get("no_price"), t.get("no_price_dollars")),
                    "count": _norm_count(t.get("count"), t.get("count_fp")),
                    "taker_side": t.get("taker_side"),
                })

        if not event_rows:
            with empty_lock:
                newly_empty.add(event_ticker)
            return 0

        df = pd.DataFrame(event_rows)
        _write_parquet(df, os.path.join(series_dir, f"{event_ticker}.parquet"))
        return len(df)

    total_trades = 0
    completed = 0
    progress_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=CONCURRENT_EVENT_WORKERS) as outer:
        futures = {outer.submit(_process_event, e): e for e in events_to_process}
        for future in as_completed(futures):
            try:
                n = future.result()
            except Exception:
                logger.exception("    event processing failed")
                n = 0
            with progress_lock:
                completed += 1
                total_trades += n
                done_now = completed
            if done_now == 1 or done_now % 10 == 0 or done_now == len(events_to_process):
                logger.info("    %s: %d/%d events processed, %d trades so far", series_ticker, done_now, len(events_to_process), total_trades)

    if newly_empty:
        _save_empty(series_dir, known_empty | newly_empty)
    logger.info("  %s: done, %d new trades written, %d events had none",
                series_ticker, total_trades, len(newly_empty))


def run():
    _check_connectivity()
    cutoff = datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)
    min_close_ts = int(cutoff.timestamp())
    logger.info("Collecting trades from last %d days (since %s)", LOOKBACK_DAYS, cutoff.date())

    top_series = discover_top_series()

    for cat in CATEGORIES:
        cat_dir = os.path.join(DATA_ROOT, _slug(cat))
        os.makedirs(cat_dir, exist_ok=True)
        series_list = top_series[cat]

        logger.info("=== %s (%d series) ===", cat, len(series_list))
        for j, s in enumerate(series_list):
            st = s["ticker"]
            title = s.get("title", "")
            logger.info("[%s %d/%d] %s - %s", cat, j + 1, len(series_list), st, title)
            try:
                collect_series(st, title, cat, cat_dir, min_close_ts)
            except Exception:
                logger.exception("Failed to collect %s", st)

    logger.info("Done.")


def _scan_existing_series(category: str) -> list[str]:
    """Series tickers already downloaded under a category directory."""
    cat_dir = os.path.join(DATA_ROOT, _slug(category))
    if not os.path.isdir(cat_dir):
        return []
    return sorted(d for d in os.listdir(cat_dir) if os.path.isdir(os.path.join(cat_dir, d)))


def _read_series_title(series_dir: str) -> str:
    """Read series_title from an existing parquet file in the series directory."""
    if os.path.isdir(series_dir):
        for f in os.listdir(series_dir):
            if f.endswith(".parquet"):
                try:
                    df = pd.read_parquet(os.path.join(series_dir, f),
                                         columns=["series_title"], engine="pyarrow")
                    if not df.empty:
                        return str(df.iloc[0]["series_title"])
                except Exception:
                    continue
    return ""


def _fetch_series_title(ticker: str) -> str:
    """Fetch series title from the Kalshi API for a not-yet-downloaded series."""
    try:
        data = _get(f"/series/{ticker}")
        return str(data.get("series", {}).get("title", "") or "")
    except Exception:
        return ""


def run_update(category: str, start_date: str | None = None, end_date: str | None = None,
               series_filter: list[str] | None = None):
    """Update mode: fetch new events/trades for already-downloaded series."""
    _check_connectivity()

    if start_date:
        min_close_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    else:
        min_close_ts = int((datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)).timestamp())

    max_close_ts = None
    settled_only = False
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=UTC)
        max_close_ts = int(end_dt.timestamp())
        # If the end date is in the past there are no open events worth re-fetching.
        if end_dt < datetime.now(UTC):
            settled_only = True

    logger.info("Update mode: category=%s, range=%s to %s, settled_only=%s",
                category, start_date or f"{LOOKBACK_DAYS}d ago", end_date or "now", settled_only)

    series_list = [s.upper() for s in series_filter] if series_filter else _scan_existing_series(category)
    if not series_list:
        logger.error("No existing data found for category '%s' in %s", category, DATA_ROOT)
        return

    cat_dir = os.path.join(DATA_ROOT, _slug(category))
    logger.info("=== Updating %s (%d series) ===", category, len(series_list))
    for j, st in enumerate(series_list):
        series_dir = os.path.join(cat_dir, st)
        title = _read_series_title(series_dir) or _fetch_series_title(st)
        logger.info("[%s %d/%d] %s - %s", category, j + 1, len(series_list), st, title)
        try:
            collect_series(st, title, category.capitalize(), cat_dir, min_close_ts, max_close_ts,
                           update_open=not settled_only, settled_only=settled_only)
        except Exception:
            logger.exception("Failed to update %s", st)

    logger.info("Update done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tape", nargs=2, metavar=("START", "END"),
                        help="Download the exchange-wide trade tape for a UTC date range")
    parser.add_argument("--tape-status", nargs="*", metavar="DATE",
                        help="Report local tape completeness; pass START END to also list days never collected")
    parser.add_argument("--markets", action="store_true", help="Download market metadata for every series")
    parser.add_argument("--workers", type=int, default=TAPE_DAY_WORKERS, help="Concurrent days for --tape")
    parser.add_argument("--rate", type=float, default=MAX_REQUESTS_PER_SEC,
                        help="Global requests/sec ceiling across all threads")
    parser.add_argument("--inflight", type=int, default=MAX_INFLIGHT_REQUESTS,
                        help="Max concurrent in-flight requests")
    parser.add_argument("--min-volume", type=float, default=0.0, help="Skip series below this lifetime volume (--markets)")
    parser.add_argument("--update", action="store_true", help="Update existing per-event data")
    parser.add_argument("--category", type=str, default=None, help="Category to update (required with --update)")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD, used with --update)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD, used with --update)")
    parser.add_argument("--series", type=str, default=None, help="Comma-separated series tickers")
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Output root (default: $KALSHI_DATA_DIR or ./data/kalshi next to this file)")
    args = parser.parse_args()
    if args.data_dir:
        DATA_ROOT = args.data_dir

    # A cursor walk is latency-bound at ~3 req/s, so raising --workers only helps if the
    # global ceilings move with it. Kalshi rate-limits per IP, so the right setting
    # depends on what else is hitting the API from this host.
    MAX_REQUESTS_PER_SEC = args.rate
    MAX_INFLIGHT_REQUESTS = args.inflight
    _inflight = threading.Semaphore(MAX_INFLIGHT_REQUESTS)
    logger.info("Rate config: %.0f req/s ceiling, %d in flight, %d day workers",
                MAX_REQUESTS_PER_SEC, MAX_INFLIGHT_REQUESTS, args.workers)

    series_filter = [s.strip() for s in args.series.split(",")] if args.series else None

    if args.tape_status is not None:
        lo, hi = (args.tape_status + [None, None])[:2]
        tape_status(lo, hi)
    elif args.tape:
        _check_connectivity()
        run_tape(args.tape[0], args.tape[1], args.workers)
    elif args.markets:
        run_markets(series_filter, args.min_volume)
    elif args.update:
        if not args.category:
            parser.error("--category is required with --update")
        run_update(args.category, args.start_date, args.end_date, series_filter)
    else:
        run()
