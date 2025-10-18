# Order Book & Trade Recording Guide

This document explains how to capture and inspect the real-time order book and
trade activity recorder that is built into the `search_ob_tui` terminal
application.

## Overview

Recording is a manual toggle that snapshots the active market’s order book and
recent trade feed while you browse in the TUI. When enabled (`r` key), every
incoming websocket update from the market stream is written to a dedicated
SQLite database. Pressing `r` again stops the session and renames the database
to include the market title and time span for quick identification.

The recorder is intentionally scoped to the market currently displayed in the
order book panel. If you change the selection while recording the new market’s
token IDs are included automatically.

## Quick Start

1. Launch the terminal UI:
   ```bash
   cargo run --bin search_ob_tui
   ```
2. Navigate to the market you want to record (arrow keys + `/` search).
3. Press `r` once. The trade panel shows a magenta status banner such as
   `Recording <market title> (press r to stop)`.
4. Continue navigating; snapshots and trades are captured automatically.
5. Press `r` again to stop. The banner is replaced with
   `Recording saved: recordings/<slug>_<start>-<end>.sqlite (<rows> rows)`.

All files are written under the repository-level `recordings/` directory. If
you delete the directory it will be recreated on the next recording.

## What Gets Stored

Two tables are created inside each SQLite database.

### `orderbook_snapshots`

| Column           | Type     | Description                                                |
|------------------|----------|------------------------------------------------------------|
| `id`             | INTEGER  | Autoincrement primary key.                                 |
| `asset_id`       | TEXT     | Polymarket CLOB token ID for the outcome.                 |
| `hashes`         | TEXT     | JSON array of event hashes that triggered the snapshot.   |
| `ws_timestamp`   | INTEGER  | Upstream timestamp from the websocket payload (ms).       |
| `local_timestamp`| INTEGER  | Recorder wall-clock timestamp when persisted (ms).        |
| `bids`           | TEXT     | JSON array of `[price, size]` levels after the update.    |
| `asks`           | TEXT     | JSON array of `[price, size]` levels after the update.    |

Snapshots are written for:

- Order book snapshots (`order_book` events).
- Depth deltas (`price_change` events). Hashes from every delta in a batch are
  accumulated so you can trace which websocket messages contributed to the
  state change.

### `trades`

| Column           | Type     | Description                                                |
|------------------|----------|------------------------------------------------------------|
| `id`             | INTEGER  | Autoincrement primary key.                                 |
| `asset_id`       | TEXT     | Token ID that executed.                                    |
| `outcome`        | TEXT     | Resolved outcome label at the time of recording.          |
| `size`           | REAL     | Quantity filled (Polymarket base units).                   |
| `price`          | REAL     | Price at which the trade executed.                        |
| `side`           | TEXT     | `BUY` or `SELL` from the taker’s perspective.             |
| `hash`           | TEXT     | Optional trade hash provided by the feed.                 |
| `ws_timestamp`   | INTEGER  | Upstream timestamp (ms).                                   |
| `local_timestamp`| INTEGER  | Recorder wall-clock timestamp (ms).                        |

Trades are ingested from `last_trade_price` websocket events. Outcome labels
come from the in-memory token metadata cache, so they track whichever market is
currently displayed in the UI.

## File Naming & Time Range

When a session stops, the temporary file is renamed to:

```
<slug-or-title>_<YYYYMMDD_HHMMSS>-<YYYYMMDD_HHMMSS>.sqlite
```

- The name uses the market slug when available, otherwise a sanitized version
  of the market title.
- The start/end timestamps correspond to the local wall-clock times of the
  first and last persisted rows. If no rows were written, the filename falls
  back to the recording start time.

## Inspecting the Data

You can explore a recording with any SQLite client. For quick inspection:

```bash
sqlite3 recordings/<file>.sqlite
```

Helpful queries:

- Count how many snapshots were captured per outcome:
  ```sql
  SELECT asset_id, COUNT(*) AS snapshots
  FROM orderbook_snapshots
  GROUP BY asset_id
  ORDER BY snapshots DESC;
  ```
- Reconstruct the most recent book state:
  ```sql
  SELECT asset_id,
         json_extract(bids, '$[0][0]') AS best_bid,
         json_extract(asks, '$[0][0]') AS best_ask
  FROM orderbook_snapshots
  WHERE id IN (
      SELECT MAX(id) FROM orderbook_snapshots GROUP BY asset_id
  );
  ```
- List trades with human-readable timestamps:
  ```sql
  SELECT asset_id,
         outcome,
         size,
         price,
         side,
         datetime(local_timestamp / 1000, 'unixepoch') AS local_time
  FROM trades
  ORDER BY id;
  ```

## Operational Notes

- **Resource usage:** Order book snapshots can be frequent during volatile
  markets. Monitor disk usage inside `recordings/` and prune files as needed.
- **Market selection:** Recording always targets the tokens for the market shown
  in the order book panel. Changing the selected market while recording expands
  the token list handled by the session.
- **Failure handling:** If the background writer encounters an error the
  recorder stops automatically and the UI shows a magenta warning. The temporary
  SQLite file is left in place for manual inspection.
- **Dependencies:** The recorder uses `rusqlite` with the bundled SQLite engine.
  No external database server is required.

## Code Map

- **Toggle & status:** `handle_key_event` (`tui/src/search_ob_tui.rs`) handles
  `r` presses and updates the trade panel banner.
- **Persistence:** `spawn_recording_worker` spins up the SQLite writer and lives
  alongside helper insert functions in the same file.
- **Event capture:** Market websocket processing dispatches snapshots and trades
  via `record_orderbook_snapshot` / `record_trade_event` when recording is
  active.

Use this guide as the reference point for further automation or analytics on
captured market data.
