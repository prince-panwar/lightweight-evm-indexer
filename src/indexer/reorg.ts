import type { ChainClient } from '../chain/chain-client.js';
import { withTransaction, type Pool } from '../db/database.js';
import type { IndexerPlugin, Logger } from '../types.js';
import type { Cursor, CursorStore } from './cursor-store.js';
import type { WatchSet } from './watch-set.js';

/**
 * Finds the last block both forks agree on and rewinds everything above it.
 *
 * The cursor stores the hash of the block it stopped at. If the chain now
 * has a different block there, a reorg reached below the cursor: walk the
 * checkpoint window (newest first) for the newest block whose stored hash
 * still matches, delete everything above it (raw events, the plugin's rows,
 * checkpoints), move the cursor back, and let ingestion replay from there.
 * Replay is idempotent, so the result is the same state a full re-index
 * would produce.
 */
export class ReorgHandler {
  constructor(
    private readonly pool: Pool,
    private readonly chain: ChainClient,
    private readonly cursor: CursorStore,
    private readonly watched: WatchSet,
    private readonly plugin: IndexerPlugin,
    private readonly reorgDepth: number,
    private readonly log: Logger,
  ) {}

  /** The fork block, or null when the cursor's block is still canonical. */
  async detectFork(chainId: number, cursor: Cursor): Promise<bigint | null> {
    const tipHash = await this.chain.getBlockHash(chainId, cursor.lastBlock);
    if (tipHash === cursor.lastBlockHash) return null;

    for (const checkpoint of await this.cursor.checkpointsBelow(chainId, cursor.lastBlock, this.reorgDepth)) {
      if ((await this.chain.getBlockHash(chainId, checkpoint.blockNumber)) === checkpoint.blockHash) {
        return checkpoint.blockNumber;
      }
    }

    // NO KNOWN FORK POINT: deeper than the window, or the window is empty (a
    // restored database). Throwing here would stop the chain dead, forever,
    // while the gap grows. Rewinding to the floor of the window and
    // re-ingesting is not a guess: replay is idempotent, so it produces the
    // rows a full replay from that height would.
    const start = this.chain.chain(chainId).startBlock - 1n;
    const window = cursor.lastBlock > BigInt(this.reorgDepth) ? cursor.lastBlock - BigInt(this.reorgDepth) : 0n;
    const floor = window > start ? window : start;
    this.log.error(
      `chain ${chainId}: reorg deeper than the ${this.reorgDepth}-checkpoint window below ${cursor.lastBlock}, ` +
        `or the window is empty; rewinding to ${floor} and re-ingesting`,
    );
    return floor;
  }

  /** Rewind if the cursor's block was reorged away. Returns the fork block, or null if nothing to do. */
  async handle(chainId: number, cursor: Cursor): Promise<bigint | null> {
    const forkBlock = await this.detectFork(chainId, cursor);
    if (forkBlock === null) return null;

    this.log.warn(`chain ${chainId}: reorg detected, rewinding from ${cursor.lastBlock} to ${forkBlock}`);
    const forkHash = await this.chain.getBlockHash(chainId, forkBlock);

    await withTransaction(this.pool, async (db) => {
      await db.query('DELETE FROM raw_event WHERE chain_id = $1 AND block_number > $2', [
        chainId,
        forkBlock.toString(),
      ]);
      await this.plugin.rewind(chainId, forkBlock, db);
      await this.cursor.deleteCheckpointsAbove(db, chainId, forkBlock);
      await this.cursor.advance(db, chainId, forkBlock, forkHash);
    });

    // Contracts discovered on the orphaned fork must stop being trusted.
    await this.watched.load(chainId);
    return forkBlock;
  }
}
