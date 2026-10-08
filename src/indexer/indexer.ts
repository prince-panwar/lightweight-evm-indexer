import type { Hex } from 'viem';
import type { ChainClient } from '../chain/chain-client.js';
import { withTransaction, type Pool, type PoolClient } from '../db/database.js';
import type { DecodedEvent, IndexerPlugin, Logger } from '../types.js';
import { batchByBlock } from './batch.js';
import type { Cursor, CursorStore } from './cursor-store.js';
import type { LogRange, RangeSizer } from './range-sizer.js';
import type { RangeFetcher } from './range-fetcher.js';
import type { ReorgHandler } from './reorg.js';

/**
 * The most events in one write transaction. Ranges are sized in BLOCKS, but
 * transactions in EVENTS, and after a long stop those stop being
 * proportional: bounding the second independently keeps recovery committable.
 */
export const MAX_EVENTS_PER_TX = 400;

export interface RangeSummary {
  fromBlock: bigint;
  toBlock: bigint;
  events: number;
  /** Events of the kinds the websocket subscription can announce. */
  triggerEvents: number;
  reorgedTo?: bigint;
  /**
   * The range ended at the safe head this pass read: the chain is caught up.
   * The runner stops here instead of asking again; "ask until nothing is new"
   * never ends on a chain whose blocks come faster than one pass takes.
   */
  atHead?: boolean;
}

export interface Lag {
  blocks: bigint;
  seconds: number;
}

export class Indexer {
  constructor(
    private readonly pool: Pool,
    private readonly chain: ChainClient,
    private readonly cursor: CursorStore,
    private readonly sizer: RangeSizer,
    private readonly fetcher: RangeFetcher,
    private readonly reorg: ReorgHandler,
    private readonly plugin: IndexerPlugin,
    private readonly reorgDepth: number,
    private readonly log: Logger,
    private readonly announced: ReadonlySet<string>,
    private readonly onCommitted?: (chainId: number, events: DecodedEvent[]) => void,
  ) {}

  /**
   * Ingest at most one range. Null when already caught up to the safe head.
   * A reorg short-circuits: the rewind moves the cursor and the next call
   * resumes from the fork rather than reasoning about a straddling range.
   */
  async runOnce(chainId: number): Promise<RangeSummary | null> {
    const safeHead = await this.chain.safeHead(chainId);
    const cursor = await this.cursor.get(chainId);

    // Within the reorg window of the safe head, check the cursor's block is
    // still canonical. Far below it the tip is settled by definition, and a
    // probe per range would cost a round trip per range of a long backfill.
    if (cursor && this.nearTip(cursor, safeHead)) {
      const forkBlock = await this.reorg.handle(chainId, cursor);
      if (forkBlock !== null) {
        return { fromBlock: forkBlock, toBlock: forkBlock, events: 0, triggerEvents: 0, reorgedTo: forkBlock };
      }
    }

    const range = await this.sizer.nextRange(
      chainId,
      cursor?.lastBlock ?? null,
      this.chain.chain(chainId).startBlock,
      safeHead,
    );
    if (!range) return null;

    // EVERY failure below narrows the range, not just the fetch: a write
    // transaction too big for its deadline fails the same way an oversized
    // eth_getLogs does, and must not be rebuilt byte-for-byte next tick.
    try {
      const summary = await this.ingest(chainId, range);
      return { ...summary, atHead: range.toBlock >= safeHead };
    } catch (err) {
      await this.sizer.shrink(chainId);
      throw err;
    }
  }

  private nearTip(cursor: Cursor, safeHead: bigint): boolean {
    return safeHead <= cursor.lastBlock + BigInt(this.reorgDepth);
  }

  private async ingest(chainId: number, range: LogRange): Promise<RangeSummary> {
    const events = await this.fetcher.fetchRange(chainId, range.fromBlock, range.toBlock);

    for (const batch of batchByBlock(events, range, MAX_EVENTS_PER_TX)) {
      const toHash = await this.chain.getBlockHash(chainId, batch.toBlock);

      // The logs were fetched BEFORE this hash. A reorg landing between the
      // two reads would store the new fork's hash next to the old fork's
      // logs, and the next fork check (which compares hashes) would never
      // rewind. A log at the cursor block carries its block's hash, so a
      // disagreement proves the reads saw different forks: abort and refetch.
      const stale = batch.events.find(
        (e) => e.blockNumber === batch.toBlock && e.blockHash.toLowerCase() !== toHash.toLowerCase(),
      );
      if (stale) {
        throw new Error(
          `chain ${chainId}: block ${batch.toBlock} changed between reading logs and its hash; reorg mid-range, retrying`,
        );
      }
      await this.commit(chainId, batch.events, batch.toBlock, toHash);
    }

    await this.sizer.recordSuccess(chainId, range);
    if (events.length > 0) this.log.debug(`chain ${chainId}: ${range.fromBlock}-${range.toBlock}, ${events.length} events`);

    return {
      fromBlock: range.fromBlock,
      toBlock: range.toBlock,
      events: events.length,
      triggerEvents: events.filter((e) => this.announced.has(e.eventName)).length,
    };
  }

  /**
   * ONE TRANSACTION: raw events, every projection, and the cursor advance
   * commit together or not at all. Only events never seen before reach the
   * plugin (raw_event is the replay guard for every handler at once).
   */
  private async commit(chainId: number, events: DecodedEvent[], toBlock: bigint, toHash: Hex): Promise<void> {
    const fresh = await withTransaction(this.pool, async (db) => {
      const unseen = await this.unseen(db, chainId, events);
      await this.insertRawEvents(db, unseen);
      for (const event of unseen) await this.plugin.handle(event, db);
      await this.cursor.advance(db, chainId, toBlock, toHash);
      await this.cursor.trimCheckpoints(db, chainId, this.reorgDepth);
      return unseen;
    });
    // After the commit, never inside it: a hint sent from inside would name
    // rows nobody can read yet, and a rolled-back batch rows that never exist.
    if (fresh.length > 0) this.onCommitted?.(chainId, fresh);
  }

  private async unseen(db: PoolClient, chainId: number, events: DecodedEvent[]): Promise<DecodedEvent[]> {
    if (events.length === 0) return [];
    let min = events[0]!.blockNumber;
    let max = min;
    for (const e of events) {
      if (e.blockNumber < min) min = e.blockNumber;
      if (e.blockNumber > max) max = e.blockNumber;
    }
    const { rows } = await db.query<{ block_hash: string; log_index: number }>(
      'SELECT block_hash, log_index FROM raw_event WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3',
      [chainId, min.toString(), max.toString()],
    );
    const seen = new Set(rows.map((r) => `${r.block_hash}:${r.log_index}`));
    return events.filter((e) => !seen.has(`${e.blockHash}:${e.logIndex}`));
  }

  private async insertRawEvents(db: PoolClient, events: DecodedEvent[]): Promise<void> {
    if (events.length === 0) return;
    const values: unknown[] = [];
    const rows = events.map((e, i) => {
      values.push(
        e.chainId,
        e.blockNumber.toString(),
        e.blockHash,
        e.logIndex,
        e.txHash,
        e.txIndex,
        e.address,
        e.eventName,
        JSON.stringify(e.args),
        e.blockTime,
      );
      const o = i * 10;
      return `($${o + 1},$${o + 2},$${o + 3},$${o + 4},$${o + 5},$${o + 6},$${o + 7},$${o + 8},$${o + 9},$${o + 10})`;
    });
    await db.query(
      `INSERT INTO raw_event (chain_id, block_number, block_hash, log_index, tx_hash, tx_index, address, event_name, args, block_time)
       VALUES ${rows.join(',')} ON CONFLICT DO NOTHING`,
      values,
    );
  }

  /**
   * How far ingestion is behind the SAFE head (the furthest the cursor can
   * ever get): measured against the raw head, a caught-up chain would always
   * read `confirmations` blocks behind.
   */
  async lag(chainId: number): Promise<Lag> {
    const [head, cursor] = await Promise.all([this.chain.safeHead(chainId), this.cursor.get(chainId)]);
    if (!cursor) return { blocks: head, seconds: Number.POSITIVE_INFINITY };
    const blocks = head > cursor.lastBlock ? head - cursor.lastBlock : 0n;
    if (blocks === 0n) return { blocks, seconds: 0 };
    const at = (await this.chain.blockTimes(chainId, [cursor.lastBlock])).get(cursor.lastBlock);
    return { blocks, seconds: at ? Math.max(0, Math.round((Date.now() - at.getTime()) / 1000)) : 0 };
  }
}
