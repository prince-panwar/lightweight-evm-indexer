import { ChainClient, type TransportFactory } from './chain/chain-client.js';
import { createPool, migrate, type Pool } from './db/database.js';
import { CursorStore } from './indexer/cursor-store.js';
import { Decoder } from './indexer/decoder.js';
import { Indexer, type Lag } from './indexer/indexer.js';
import { LeaderLock } from './indexer/leader.js';
import { RangeFetcher } from './indexer/range-fetcher.js';
import { RangeSizer, type RangeSizing } from './indexer/range-sizer.js';
import { ReorgHandler } from './indexer/reorg.js';
import { WatchSet } from './indexer/watch-set.js';
import { consoleLogger } from './logger.js';
import { Runner, type RunnerTiming } from './runner.js';
import type { SocketFactory } from './trigger/log-stream.js';
import { LogTrigger } from './trigger/log-trigger.js';
import type { ChainConfig, DecodedEvent, IndexerPlugin, Logger } from './types.js';

export interface IndexerOptions {
  /** Postgres connection string (the engine's tables live next to your plugin's). */
  databaseUrl: string;
  chains: ChainConfig[];
  plugin: IndexerPlugin;
  /** Default 2000 ms. */
  pollMs?: number;
  /** Default 60000 ms. Only matters for chains with a live websocket. */
  sweepMs?: number;
  /** Default 15000 ms. */
  drainBudgetMs?: number;
  /** Default 60000 ms; 0 disables the per-minute RPC usage log. */
  usageLogMs?: number;
  /** Blocks per range on a fresh chain, and the cap. Default { start: 2000, max: 10000 }. */
  rangeSizing?: RangeSizing;
  /** Checkpoints kept per chain: how deep a reorg can be located. Default 256. */
  reorgDepth?: number;
  /** Postgres advisory-lock key for single-writer leadership. Default 7_312_001. */
  leaderLockKey?: bigint;
  /** Run the engine's schema migration on start. Default true. */
  migrate?: boolean;
  logger?: Logger;
  /** Called after each committed batch with the newly ingested events (e.g. publish live updates). */
  onCommitted?: (chainId: number, events: DecodedEvent[]) => void;
  /** Test seams. */
  transport?: TransportFactory;
  socketFactory?: SocketFactory;
}

export interface RunningIndexer {
  start(): Promise<void>;
  stop(): Promise<void>;
  /** How far a chain's cursor trails its safe head. */
  lag(chainId: number): Promise<Lag>;
  readonly pool: Pool;
}

/** Wire the engine from config and a plugin. Nothing runs until `start()`. */
export function createIndexer(options: IndexerOptions): RunningIndexer {
  const log = options.logger ?? consoleLogger('indexer');
  const chains = new Map(options.chains.map((c) => [c.chainId, c]));
  const reorgDepth = options.reorgDepth ?? 256;

  const pool = createPool(options.databaseUrl);
  const chain = new ChainClient(chains, { transport: options.transport, logger: log });
  const cursor = new CursorStore(pool);
  const sizer = new RangeSizer(pool, options.rangeSizing ?? { start: 2_000, max: 10_000 }, log);
  const decoder = new Decoder(options.plugin.events);
  const watched = new WatchSet(pool, options.plugin);
  const fetcher = new RangeFetcher(chain, decoder, watched, options.plugin);
  const reorg = new ReorgHandler(pool, chain, cursor, watched, options.plugin, reorgDepth, log);
  const indexer = new Indexer(pool, chain, cursor, sizer, fetcher, reorg, options.plugin, reorgDepth, log, options.onCommitted);
  const leader = new LeaderLock(options.databaseUrl, options.leaderLockKey ?? 7_312_001n, log);
  const trigger = new LogTrigger(
    chains,
    decoder,
    watched,
    options.plugin,
    fetcher.triggerTopics(),
    chain.usage,
    log,
    options.socketFactory,
  );
  const timing: RunnerTiming = {
    pollMs: options.pollMs ?? 2_000,
    sweepMs: options.sweepMs ?? 60_000,
    drainBudgetMs: options.drainBudgetMs ?? 15_000,
    usageLogMs: options.usageLogMs ?? 60_000,
  };
  const runner = new Runner(chains, leader, indexer, watched, trigger, chain, timing, log);

  return {
    pool,
    async start() {
      if (options.migrate !== false) await migrate(pool);
      runner.start();
    },
    async stop() {
      await runner.stop();
      await pool.end();
    },
    lag: (chainId) => indexer.lag(chainId),
  };
}
