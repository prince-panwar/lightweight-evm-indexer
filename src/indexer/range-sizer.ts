import type { Pool } from '../db/database.js';
import type { Logger } from '../types.js';

export interface LogRange {
  fromBlock: bigint;
  toBlock: bigint;
}

export interface RangeSizing {
  /** Blocks per range on a fresh chain. */
  start: number;
  /** Upper bound the size may grow to. */
  max: number;
}

/** Clean ranges in a row before the size grows. */
const GROWTH_STREAK = 3;
const GROWTH_FACTOR = 1.5;
const MIN_RANGE = 1;

/**
 * How many blocks the next range covers.
 *
 * At the tip a range is a handful of blocks and none of this matters. After a
 * long stop it is what decides whether recovery takes minutes or never
 * finishes: grow slowly after a streak of clean ranges, halve at once on any
 * failure, and persist the size so a restart resumes where the provider was
 * last comfortable rather than at the start.
 */
export class RangeSizer {
  private readonly cleanRuns = new Map<number, number>();

  constructor(
    private readonly pool: Pool,
    private readonly sizing: RangeSizing,
    private readonly log: Logger,
  ) {}

  async rangeSize(chainId: number): Promise<number> {
    const { rows } = await this.pool.query<{ range_size: number }>(
      'SELECT range_size FROM range_progress WHERE chain_id = $1',
      [chainId],
    );
    return rows[0]?.range_size ?? this.sizing.start;
  }

  /** The next range above `cursorBlock` (or from `startBlock`), clamped to the safe head; null when caught up. */
  async nextRange(chainId: number, cursorBlock: bigint | null, startBlock: bigint, safeHead: bigint): Promise<LogRange | null> {
    const fromBlock = cursorBlock !== null ? cursorBlock + 1n : startBlock;
    if (fromBlock > safeHead) return null;
    const size = BigInt(await this.rangeSize(chainId));
    const toBlock = fromBlock + size - 1n;
    return { fromBlock, toBlock: toBlock > safeHead ? safeHead : toBlock };
  }

  /**
   * A clean range: grow after a streak. `+ 1` is load-bearing: floor(n * 1.5)
   * is a fixed point for n < 2, so without it a range halved down to 1 block
   * would stay there forever.
   */
  async recordSuccess(chainId: number, range: LogRange): Promise<void> {
    const streak = (this.cleanRuns.get(chainId) ?? 0) + 1;
    let size = await this.rangeSize(chainId);
    if (streak >= GROWTH_STREAK) {
      size = Math.min(Math.max(size + 1, Math.ceil(size * GROWTH_FACTOR)), this.sizing.max);
      this.cleanRuns.set(chainId, 0);
    } else {
      this.cleanRuns.set(chainId, streak);
    }
    await this.save(chainId, size, range.toBlock);
  }

  /** Any failure halves the range (never below MIN_RANGE): the retry is not the attempt that just failed. */
  async shrink(chainId: number): Promise<number> {
    const size = Math.max(MIN_RANGE, Math.floor((await this.rangeSize(chainId)) / 2));
    this.cleanRuns.set(chainId, 0);
    await this.save(chainId, size, null);
    this.log.warn(`chain ${chainId}: range size shrunk to ${size}`);
    return size;
  }

  private async save(chainId: number, size: number, doneThrough: bigint | null): Promise<void> {
    await this.pool.query(
      `INSERT INTO range_progress (chain_id, range_size, done_through) VALUES ($1, $2, $3)
       ON CONFLICT (chain_id) DO UPDATE SET range_size = $2,
         done_through = COALESCE($3, range_progress.done_through)`,
      [chainId, size, doneThrough?.toString() ?? null],
    );
  }
}
