import type { Hex } from 'viem';
import type { Pool, PoolClient } from '../db/database.js';

export interface Cursor {
  lastBlock: bigint;
  lastBlockHash: Hex;
}

/**
 * The per-chain cursor and its checkpoint window.
 *
 * Writes take a TRANSACTION CLIENT on purpose: the cursor must move in the
 * same transaction as the range's writes. Split them and a crash in between
 * either loses the range (cursor advanced, writes rolled back) or replays it
 * into a cursor that already passed it. Committing both together, plus the
 * raw-event replay guard, turns at-least-once delivery into an exactly-once
 * effect without a distributed transaction.
 */
export class CursorStore {
  constructor(private readonly pool: Pool) {}

  async get(chainId: number): Promise<Cursor | null> {
    const { rows } = await this.pool.query<{ last_block: string; last_block_hash: string }>(
      'SELECT last_block, last_block_hash FROM indexer_cursor WHERE chain_id = $1',
      [chainId],
    );
    const row = rows[0];
    return row ? { lastBlock: BigInt(row.last_block), lastBlockHash: row.last_block_hash as Hex } : null;
  }

  /** Move the cursor and record the block's hash as a checkpoint (upsert: a reorg replays heights). */
  async advance(db: PoolClient, chainId: number, block: bigint, hash: Hex): Promise<void> {
    await db.query(
      `INSERT INTO indexer_cursor (chain_id, last_block, last_block_hash, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (chain_id) DO UPDATE SET last_block = $2, last_block_hash = $3, updated_at = now()`,
      [chainId, block.toString(), hash],
    );
    await db.query(
      `INSERT INTO block_checkpoint (chain_id, block_number, block_hash) VALUES ($1, $2, $3)
       ON CONFLICT (chain_id, block_number) DO UPDATE SET block_hash = $3`,
      [chainId, block.toString(), hash],
    );
  }

  /** Keep only the newest `keep` checkpoints: the window bounds how deep a reorg can be found. */
  async trimCheckpoints(db: PoolClient, chainId: number, keep: number): Promise<void> {
    await db.query(
      `DELETE FROM block_checkpoint WHERE chain_id = $1 AND block_number < (
         SELECT block_number FROM block_checkpoint WHERE chain_id = $1
         ORDER BY block_number DESC OFFSET $2 LIMIT 1)`,
      [chainId, Math.max(0, keep - 1)],
    );
  }

  /** Checkpoints strictly below `block`, newest first. */
  async checkpointsBelow(chainId: number, block: bigint, limit: number): Promise<{ blockNumber: bigint; blockHash: Hex }[]> {
    const { rows } = await this.pool.query<{ block_number: string; block_hash: string }>(
      `SELECT block_number, block_hash FROM block_checkpoint
       WHERE chain_id = $1 AND block_number < $2 ORDER BY block_number DESC LIMIT $3`,
      [chainId, block.toString(), limit],
    );
    return rows.map((r) => ({ blockNumber: BigInt(r.block_number), blockHash: r.block_hash as Hex }));
  }

  /** Drop everything above a fork point; the fork block itself survives. */
  async deleteCheckpointsAbove(db: PoolClient, chainId: number, block: bigint): Promise<void> {
    await db.query('DELETE FROM block_checkpoint WHERE chain_id = $1 AND block_number > $2', [
      chainId,
      block.toString(),
    ]);
  }
}
