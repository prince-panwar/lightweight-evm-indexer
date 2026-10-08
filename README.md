# lightweight-evm-indexer

[![CI](https://github.com/prince-panwar/lightweight-evm-indexer/actions/workflows/ci.yml/badge.svg)](https://github.com/prince-panwar/lightweight-evm-indexer/actions/workflows/ci.yml)

A small, production-grade engine for indexing EVM contract events into Postgres. It handles the parts that usually go wrong: cursors, oversized `eth_getLogs` ranges, reorgs, flaky RPC providers, exactly-once writes, and RPC cost.

It is about 2,300 lines of TypeScript, much of it comments explaining *why*, on top of [viem](https://viem.sh) and [pg](https://node-postgres.com). There is no framework and no code generation. Your protocol plugs in as one object: which events to index, which contracts to trust, and what to write.

> Extracted from a production indexer that serves a live DeFi protocol on five EVM chains. In production the websocket trigger and the learned `eth_getLogs` window cut RPC usage from **385 to ~25 calls per minute (−93%)**. Data was identical, and the chain stayed seconds behind.

---

## Contents

- [Measured results](#measured-results)
- [How it works](#how-it-works)
- [Design decisions](#design-decisions)
- [Quick start](#quick-start)
- [Writing a plugin](#writing-a-plugin)
- [Configuration](#configuration)
- [Operating it](#operating-it)
- [Testing](#testing)
- [Limitations](#limitations)

---

## Measured results

All numbers come from real runs, not estimates. Five chains (Base, Ink, Robinhood Chain, BNB Chain, Monad) through Alchemy.

### RPC calls per minute, five chains

| Engine version | Calls / min | vs. polling |
|---|---:|---:|
| Plain polling every 2 s (the usual design) | 385 | — |
| + websocket trigger, 30 s safety sweep | 56 | −85% |
| + `eth_getLogs` window learned from rejections, 60 s sweep (**production**) | **~25** | **−93%** |

Measurement method: the "before" and "after" engines ran side by side against the same chains for 10–11 minutes. Each counted every JSON-RPC call at a proxy, and each had its own database. Both produced identical data.

### Under failure (chaos A/B, 11 minutes, real chains)

The stress conditions:
- every websocket dropped every 150 s,
- one provider capped at a 20-block `eth_getLogs` window,
- both engines starting just behind the tip.

| | Calls / min | Errors | Websocket reconnects |
|---|---:|---:|---:|
| Previous engine | 91.2 | 0 | 25 |
| This engine | **46.9** | **0** | 25 |

The capped provider's window was learned from **one** rejection per query kind per 10-minute cycle, by reading the limit the provider states in its error message.

### Latency (event → committed row)

Latency is dominated by the confirmation depth you choose. The trigger itself adds about a second.

| Chain | Confirmations | Observed |
|---|---:|---:|
| Local anvil (e2e test) | 0 | ~0.8 s from sending the tx |
| Robinhood Chain | 30 (~3 s) | ~5 s |
| BNB Chain | 15 (~7 s) | ~9 s |
| Base | 4 (~8 s) | ~10.6 s |

### Throughput and heavy streams

- **Sustained load:** the same ingestion path sustained **60 events/s** with a 12 s worst-case lag on **1 vCPU / 2 GB**. All 17,884 of 17,884 events were reconciled against the chain.
- **Firehose:** indexing USDC transfers on Base (~60 events/s) through the example plugin produced 116,000 rows from a 1,000-block backfill in about 2 minutes. On this stream a websocket would cost **~3,500 notifications a minute**. The engine detects this and falls back to polling at **~130 calls a minute** (see [busy-chain guard](#8-a-busy-chain-polls-instead)).

---

## How it works

```mermaid
flowchart LR
    subgraph chain[EVM chain]
        RPC[(HTTP JSON-RPC)]
        WS[(eth_subscribe logs)]
    end

    WS -- "log in block N" --> TR["LogTrigger<br/>decode, trust, discover"]
    TR -- "pending: N" --> RU["Runner<br/>is this chain due?"]
    RU -- "settled / sweep / catch-up / poll" --> IX["Indexer.runOnce"]

    IX -- "safe head" --> RPC
    IX -- "reorg check:<br/>cursor hash vs chain" --> RPC
    IX --> RF["RangeFetcher<br/>1: topics from anyone + isTrusted<br/>2: common events by address"]
    RF -- "getLogsAdaptive<br/>(learned window)" --> RPC

    IX -- "one transaction" --> DB[("Postgres<br/>raw_event + plugin rows + cursor")]
```

One pass for one chain:

1. **Settled head.** Read head minus `confirmations`, or the chain's `finalized` tag. Distrust it when it lags or runs ahead, and never let it move backwards.
2. **Reorg check.** Near the tip, compare the hash stored with the cursor against the chain. If they differ, find the fork in the checkpoint window and rewind (see [reorgs](#3-reorgs-are-rewound-not-patched)).
3. **Next range.** Take the next block range above the cursor. Its size adapts: it grows after clean ranges and halves on any failure.
4. **Fetch in two phases.**
   - Phase 1 asks for your event signatures from anyone, then drops whatever `isTrusted` rejects.
   - Phase 2 fetches common events (ERC-20 `Transfer`) by address only.
5. **Timestamp.** Read one block time per distinct block, for kept events only.
6. **Commit.** Write the raw events, your projections and the cursor advance in **one transaction**, in batches that end on block boundaries.

The **Runner** decides when a chain gets a pass:

- **Without a live websocket**, every `pollMs` (classic polling).
- **With one**, only when there is a reason:
  - a notified block has had time to settle (`confirmations × blockTimeMs`),
  - the socket just reconnected,
  - the chain is still catching up,
  - or the periodic sweep is due.

An idle chain therefore costs a sweep every 60 s, about 5 calls a minute, instead of a pass every 2 s.

---

## Design decisions

These are the decisions that made the difference in production. Most of them were learned from an incident or a measurement.

### 1. The websocket is a trigger, not a data source

A notification only says "block N has one of your events". The events themselves are still read with `eth_getLogs` over the cursor's range. So:

- A dropped socket, a lost message or a provider hiccup can only **delay** an event until the next sweep. It can never lose one.
- There is no second write path to keep consistent. Websocket and polling ingestion are the same code.
- A sweep that ingests events the socket never announced logs a warning. That is your health signal.

### 2. Exactly-once from at-least-once

Every range may be retried. Correctness comes from two things:

- **Same-transaction cursor.** Raw events, plugin writes and the cursor advance commit together or not at all.
- **Replay guard.** `raw_event` is keyed by `(chain, blockHash, logIndex)`, and only events never seen before reach your handler.

Handlers therefore don't need to be idempotent. Batches end on block boundaries, because the cursor means "everything up to block N is applied". An oversized single block is committed whole rather than split.

### 3. Reorgs are rewound, not patched

- **Detection.** The cursor stores the hash of the block it stopped at, and the newest N commit points are kept as checkpoints.
- **Rewind.** If the chain no longer agrees, the engine walks the checkpoints to the newest block both forks share. It then deletes everything above it (raw events, your rows through `rewind()`, checkpoints), moves the cursor back and re-ingests. Replay is idempotent, so the result is what a full re-index would produce. The e2e suite does exactly this on anvil.
- **Mid-range guard.** A reorg landing *between* reading a range's logs and reading its end block's hash would store the new fork's hash next to the old fork's logs. A hash guard catches that and retries the range.

One idea was tried and **rejected**: checking for reorgs only every few minutes to save a block read per pass. Between two checks the cursor can move onto the new fork, after which the fork check matches and the orphaned rows stay forever. Saving 4 calls a minute isn't worth silent corruption.

### 4. Learn the provider's `eth_getLogs` window from rejections only

Providers cap `eth_getLogs` by block span or by response size, and each words the error differently.

The obvious approach, "remember the largest span that worked and pre-split to it", is a trap. If the first range after a restart happens to be 4 blocks, every later range is split into 4-block calls forever, because nothing wider is ever tried again. In production that meant 8 `eth_getLogs` per sweep instead of 2.

So the window is learned **only from a rejection**:

- **Stated limit:** parsed straight from the message when the provider states it ("up to a 2K block range"), so one rejection is enough.
- **Unstated limit:** bisected otherwise.
- **Same-call narrowing:** applied to the rest of the same call.
- **Per query kind:** topic queries and address queries are tracked separately.
- **Expiry:** forgotten 10 minutes after the last rejection, so a plan upgrade or a misread error heals by itself.
- **Throttles are separate:** "rate limit exceeded" means wait, and is never mistaken for "range too large", which would mean split. Splitting on a throttle doubles the request rate against a provider that just said stop.

### 5. Topic-only fetches need a trust boundary

Fetching by event signature (no address filter) costs one call however many contracts you watch, and it catches a contract created in the same range. But signatures aren't yours to reserve: a probe of a real chain found foreign contracts emitting the same events.

Every decoded event therefore passes your `isTrusted` check before it is stored, timestamped or even counted. Factory children are learned through `discover()` in block order, so a child's first event in the same range is trusted.

### 6. "Caught up" means the range reached the safe head

"Keep asking until nothing is new" never ends on a chain whose blocks come faster than one pass takes. A range that ends at the safe head is caught up, and the next tick picks up whatever came after.

### 7. Failures back off per chain

A failing chain is **skipped** until its backoff (1 s, doubling to 60 s) expires, while healthy chains keep their pace.

The earlier design capped the backoff at the poll interval. A provider outage then meant retrying every 2 s for as long as it lasted.

### 8. A busy chain polls instead

A trigger only pays off for sparse events. Providers bill every notification, and a firehose, such as a popular token's transfers, is never idle anyway.

Above `websocketMaxNotificationsPerMinute` (default 600) a chain drops its socket and polls for 30 minutes, then tries again. Measured on USDC/Base: ~3,500 → ~130 calls a minute.

### 9. One writer, warm standby

Leadership is a Postgres session advisory lock held on a dedicated connection. A second instance idles until the first one's connection ends.

Zero-downtime deploys are therefore: start the new instance, then stop the old one. A standby opens no websockets.

---

## Quick start

Requirements: Node 22+ and Docker (for Postgres).

```bash
git clone https://github.com/prince-panwar/lightweight-evm-indexer.git
cd lightweight-evm-indexer
npm ci
docker compose up -d postgres

# Index ERC-20 transfers for one or more tokens on any EVM chain:
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer \
RPC_URL=https://base-mainnet.g.alchemy.com/v2/<your-key> \
CHAIN_ID=8453 \
TOKENS=0x<token-address>,0x<another> \
npm run example:erc20
```

You'll see a short backfill, then the tip. The engine logs RPC usage every minute:

```text
INFO  [erc20-example] acquired indexer leadership
INFO  [erc20-example] chain 8453: websocket subscribed
INFO  [erc20-example] chain 8453: +412 transfer(s)
INFO  [erc20-example] chain 8453: 6 rpc calls in 60s (eth_getBlockByNumber 2, eth_getLogs 2, eth_blockNumber 1, ws_eth_chainId 1)
```

Balances are one query away:

```sql
SELECT holder, SUM(delta) AS balance FROM (
  SELECT to_address   AS holder,  value AS delta FROM erc20_transfer WHERE token = $1
  UNION ALL
  SELECT from_address AS holder, -value AS delta FROM erc20_transfer WHERE token = $1
) t GROUP BY holder ORDER BY balance DESC LIMIT 20;
```

---

## Writing a plugin

A plugin is everything protocol-specific. Here is a factory pattern: a factory creates pools, and each pool emits `Swap`. The package isn't on npm; install it from this repository (`npm install github:prince-panwar/lightweight-evm-indexer`) or vendor `src/`.

```ts
import { createIndexer, type IndexerPlugin } from 'lightweight-evm-indexer';
import { parseAbiItem, type Address } from 'viem';

const FACTORY = process.env.FACTORY!.toLowerCase();

const plugin: IndexerPlugin = {
  events: [
    parseAbiItem('event PoolCreated(address indexed pool, address token0, address token1)'),
    parseAbiItem('event Swap(address indexed sender, int256 amount0, int256 amount1)'),
  ],

  // Pools discovered earlier live in your own table; reload them on start.
  async loadWatched(chainId, db) {
    const { rows } = await db.query('SELECT address FROM pool WHERE chain_id = $1', [chainId]);
    return rows.map((r) => r.address as Address);
  },

  // The security boundary: PoolCreated only from the factory, Swap only from known pools.
  isTrusted: (e, ctx) => (e.eventName === 'PoolCreated' ? e.address === FACTORY : ctx.watched.has(e.address)),

  // A trusted PoolCreated introduces a pool; it is trusted from the next event on, same range included.
  discover: (e) => (e.eventName === 'PoolCreated' ? [e.args.pool as Address] : []),

  // Runs inside the commit transaction, in (block, logIndex) order, once per event.
  async handle(e, db) {
    if (e.eventName === 'PoolCreated') {
      await db.query('INSERT INTO pool (chain_id, address, created_block) VALUES ($1, $2, $3)', [
        e.chainId, e.args.pool, e.blockNumber.toString(),
      ]);
    } else {
      await db.query('INSERT INTO swap (chain_id, pool, block_number, amount0, amount1) VALUES ($1, $2, $3, $4, $5)', [
        e.chainId, e.address, e.blockNumber.toString(), e.args.amount0, e.args.amount1,
      ]);
    }
  },

  // Undo your rows above the fork; the engine handles its own tables and the cursor.
  async rewind(chainId, forkBlock, db) {
    await db.query('DELETE FROM swap WHERE chain_id = $1 AND block_number > $2', [chainId, forkBlock.toString()]);
    await db.query('DELETE FROM pool WHERE chain_id = $1 AND created_block > $2', [chainId, forkBlock.toString()]);
  },
};

const indexer = createIndexer({
  databaseUrl: process.env.DATABASE_URL!,
  plugin,
  chains: [
    {
      chainId: 8453,
      name: 'base',
      rpcUrls: [process.env.RPC_URL!],
      startBlock: 12_345_678n, // your factory's deploy block
      confirmations: 4,
      blockTimeMs: 2_000,
      websocket: true,
    },
  ],
});
await indexer.start();
```

| Hook | When it runs | Notes |
|---|---|---|
| `events` | — | ABI event items to index. |
| `addressScopedEvents` | — | Names too common to fetch by topic (e.g. `Transfer`); fetched by address from the watched set. |
| `subscriptionAddresses(chainId)` | Socket open | Optional fixed address list for the websocket filter (covers every event kind). Leave it out for factory-style discovery. |
| `loadWatched(chainId, db)` | Start, and after a reorg rewind | Addresses trusted beyond your static ones. |
| `isTrusted(event, ctx)` | Every decoded event and notification | Must be pure. Untrusted events are never stored. |
| `discover(event, ctx)` | Every trusted event | Returns addresses the event introduces. |
| `handle(event, db)` | Inside the commit transaction | Only for never-seen events, in canonical order. |
| `rewind(chainId, forkBlock, db)` | Inside the rewind transaction | Delete your rows above `forkBlock`. |

---

## Configuration

`createIndexer(options)`:

| Option | Default | Meaning |
|---|---|---|
| `databaseUrl` | — | Postgres connection string. The engine's tables live next to yours. |
| `chains` | — | One `ChainConfig` per chain (below). |
| `plugin` | — | Your `IndexerPlugin`. |
| `pollMs` | `2000` | Tick interval, and the poll interval for chains without a live websocket. |
| `sweepMs` | `60000` | With a live websocket: how often a chain is swept anyway. |
| `drainBudgetMs` | `15000` | How long one chain may drain before the next chain gets its turn. |
| `rangeSizing` | `{ start: 2000, max: 10000 }` | Blocks per range. Lower `start` for very busy event streams. |
| `reorgDepth` | `256` | Checkpoints kept per chain: how deep a reorg can be located. |
| `websocketMaxNotificationsPerMinute` | `600` | Above this, a chain polls instead (busy-chain guard). |
| `websocketBusyCooldownMs` | `1800000` | How long a busy chain polls before the socket is retried. |
| `usageLogMs` | `60000` | Per-chain RPC usage log interval (`0` disables it). |
| `leaderLockKey` | `7312001n` | Advisory-lock key; use a distinct one per indexer sharing a database. |
| `onCommitted(chainId, events)` | — | Called after each committed batch (e.g. to publish live updates). |
| `logger` | console | Any `{ debug, info, warn, error }` logger. |

`ChainConfig`:

| Field | Meaning |
|---|---|
| `chainId`, `name` | Identity. |
| `rpcUrls` | HTTP(S) endpoints, used as a fallback list. |
| `startBlock` | First block on a fresh database (your deploy block). |
| `confirmations` | Blocks behind head treated as settled. |
| `blockTimeMs` | Typical block interval, used to estimate when a notified block is settled. |
| `finality` | `{ mode: 'confirmations' }` (default) or `{ mode: 'finalized', maxLagBlocks }`. |
| `websocket`, `wsUrl` | Enable the trigger. The URL defaults to the RPC URL with `http(s)` → `ws(s)`. |

---

## Operating it

The log is the dashboard:

| Line | Meaning |
|---|---|
| `chain N: 6 rpc calls in 60s (eth_getLogs 2, …)` | Billable calls per chain and method, socket included. Judge any change by this. |
| `websocket subscribed` / `websocket down, polling meanwhile` | Trigger state. Polling is automatic while a socket is down. |
| `sweep ingested N event(s) the websocket did not announce` | The socket missed events; they were ingested anyway. |
| `provider log window narrowed to N blocks` | The provider rejected a wider `eth_getLogs`. Learned per query kind; expires after 10 min. |
| `over 600 notifications/min … polling` | The busy-chain guard engaged. |
| `backfilling, N blocks (M min) behind` | Recovery progress after a stop; silent at the tip. |
| `reorg detected, rewinding from A to B` | A reorg below the cursor was rewound. |

`indexer.lag(chainId)` returns how far the cursor trails the safe head, for health checks.

**Rollback:** set `websocket: false` and restart. The chain is polled exactly as before; nothing in the data path changes.

---

## Testing

```bash
npm test          # 51 unit tests, no network or database
npm run test:e2e  # anvil + Postgres: websocket-triggered ingestion and a real reorg rewind
```

The unit suite covers the adaptive `eth_getLogs`:
- contiguous in-order coverage,
- one-rejection learning,
- same-call narrowing,
- expiry,
- response-size caps,
- per-kind limits.

It also covers websocket reconnect, heartbeat and flood handling, and every scheduling rule in the runner.

The e2e suite (`docker compose up -d postgres`, Foundry's `anvil` on `PATH`, `DATABASE_URL` set) runs the real engine against a local chain:
- **Trigger only:** with the sweep pushed to 10 minutes, only the trigger can ingest, which takes ~0.8 s from send to commit.
- **Real reorg:** a snapshot, revert and replace at the same height must end with the orphaned transfer gone and its replacement ingested.

---

## Limitations

- **EVM only.** It relies on `eth_getLogs`, `eth_subscribe` and block hashes.
- **One writer per database.** One process sustained ~60 events/s on 1 vCPU. Beyond that, shard chains across processes, each with its own `leaderLockKey`, or use a dedicated indexing service.
- **Confirmation-based finality is probabilistic.** It's the only option on most L2s. Pick depths that match each chain's reorg history. The rewind covers anything within the checkpoint window.
- **Your plugin owns semantics.** The engine can't tell whether your projection should be cumulative or point-in-time across a rewind; `rewind()` decides.
