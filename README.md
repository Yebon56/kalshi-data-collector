# Kalshi trade collector

A script that downloads trade-level data from the public Kalshi REST API (`https://api.elections.kalshi.com/trade-api/v2`) and saves it as parquet. No API key is required.

## Installation

Requires Python 3.10 or later.

```bash
pip install -r requirements.txt
```

## Quick start

```bash
# Download NFL moneyline games that closed on 2026-01-18 (1-2 minutes)
python kalshi_collector.py --update --category sports \
  --series KXNFLGAME --start-date 2026-01-18 --end-date 2026-01-18
```

Output is written to `data/kalshi/sports/KXNFLGAME/<event_ticker>.parquet`.

## `--update` mode

Given one or more series, this mode walks through every event in each series, downloads its trades, and writes one parquet file per event. Despite the name, it is also the mode to use for a first download.

```
series ─▶ /events ─▶ event ─▶ /markets ─▶ market ─▶ /markets/trades
                                                    (+ /historical/trades)
```

1. For each series passed to `--series`, fetch all settled and open events from `/events`. If `--end-date` is in the past, only settled events are fetched.
2. Skip settled events that already have a valid file on disk. Open events are fetched again.
3. Fetch the event's markets and drop any market whose `close_time` falls outside `--start-date` to `--end-date`.
4. Download the full trade history of each remaining market. If the summed `count` falls short of the market's `volume`, read both the live and historical tiers and merge them.
5. Attach event and market metadata to every trade row and write the result to `<data-dir>/<category>/<series>/<event_ticker>.parquet`.

If a run is interrupted, running the same command again resumes from the events that are still missing.

### Options

| Option | Meaning | Default |
|---|---|---|
| `--category` | Name of the output subfolder (e.g. `crypto`, `sports`). Required | Error if omitted |
| `--series` | Series tickers to download, comma-separated | Every series already under `data/kalshi/<category>/` |
| `--start-date` | Lower bound on market `close_time` (YYYY-MM-DD, UTC) | 120 days before today |
| `--end-date` | Upper bound on market `close_time` (YYYY-MM-DD, through 23:59:59 UTC that day) | No limit |
| `--data-dir` | Output location | `$KALSHI_DATA_DIR`, or `data/kalshi` next to the script if unset |
| `--rate`, `--inflight` | Max requests per second, max concurrent requests | 18, 6 |

The date range applies to `close_time` in UTC. A game that ended late on Jan 17 US time can therefore fall under `--start-date 2026-01-18`.

### Finding series tickers

A series ticker appears in kalshi.com market URLs and as the prefix of every event ticker. For example, the event `KXNBAGAME-25DEC01ATLDET` belongs to the series `KXNBAGAME`. You can also list all series through the API:

```bash
curl -s "https://api.elections.kalshi.com/trade-api/v2/series?include_volume=true" | head -c 2000
```

### Run time and rate limits

Kalshi rate-limits requests per IP. Series with many events (for example, 15-minute crypto series) can take several hours.

- `429 ... retrying` warnings during a run are normal. The script waits and retries on its own.
- Do not run several processes from the same IP at once.
- Run long jobs inside `tmux` or `nohup`.

## Output schema (`--update`)

Each file holds one event, and each row is one trade. Market and event metadata are repeated on every row.

| Column | Description |
|---|---|
| `trade_id` | Unique trade ID |
| `created_time` | Trade time (UTC, ISO 8601) |
| `ticker` | Market ticker (e.g. `KXNFLGAME-26JAN18SFSEA-SEA`) |
| `yes_price`, `no_price` | Execution price in integer cents (1–99). `yes + no = 100` |
| `count` | Number of contracts traded |
| `taker_side` | Side the taker bought (`yes` / `no`) |
| `event_ticker`, `event_title`, `event_sub_title` | Event information |
| `series_ticker`, `series_title`, `category` | Series information. `category` is the value passed to `--category` |
| `market_title`, `market_type`, `yes_sub_title`, `no_sub_title`, `rules_primary` | Market information |
| `result` | Settlement result (`yes` / `no`, empty if not yet settled) |
| `expiration_value` | Settlement reference value (e.g. the index level at expiry) |
| `volume`, `open_interest` | Market's cumulative volume and open interest at collection time |
| `open_time`, `close_time`, `expected_expiration_time`, `settlement_ts`, `status` | Market schedule and status |

`_no_trades.json` in each series folder lists events with no trades at all, so that later runs do not query them again.

## Other modes

| Command | Description | Output |
|---|---|---|
| `--tape START END` | Exchange-wide trades by date. Only tickers, no metadata | `tape/<date>/part-*.parquet` |
| `--tape-status [START END]` | Check whether downloaded tape days are complete | (console) |
| `--markets [--series ...]` | Market metadata per series | `markets/<series>.parquet` |
| (no options) | Download the top-10 series by volume in each category for the last 120 days | `<category>/<series>/...` |

## API notes

The script already handles everything below, so you do not need to deal with it yourself. Keep it in mind if you call the API directly.

- `/events` and `/historical/markets` silently ignore `min_close_ts` and `max_close_ts`. Date filtering has to happen client-side after the data arrives.
- List endpoints must be paged through to the last cursor. Reading only the first page truncates the market list.
- Data is split into a live tier and a historical tier at `/historical/cutoff`. For markets near the boundary, reading only one tier can miss trades.
- Between February and March 2026, the API's numeric fields were replaced by string fields: `volume` → `volume_fp`, `count` → `count_fp`, `yes_price` → `yes_price_dollars`, and so on. The script reads both the old and new names and converts them to integers.

## Reproducing paper data

The Kalshi trade data for the two papers below can be downloaded with the following commands. Sample selection (period, minimum-trade filters, matching to regular-season games, etc.) happens in the analysis stage after collection.

**Lee, Lee & Lee (2026), *Cryptocurrency prediction markets through the derivatives lens*, IRFA 119, 105433**

```bash
python kalshi_collector.py --update --category crypto \
  --series KXBTCD,KXBTC15M,KXBTC,KXETHD,KXETH15M,KXETH \
  --start-date 2025-10-10 --end-date 2026-02-28
```

**Lee, Lee & Lee (2026), *Underreaction, salience, and hot-hand beliefs in sports prediction markets*, JBEF 52, 101259**

```bash
python kalshi_collector.py --update --category sports \
  --series KXNBAGAME,KXNFLGAME \
  --start-date 2025-09-01 --end-date 2026-04-30
```
