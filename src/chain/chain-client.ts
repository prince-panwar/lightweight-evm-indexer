import {
  createPublicClient,
  fallback,
  formatLog,
  http,
  numberToHex,
  type Hex,
  type Log,
  type PublicClient,
  type RpcLog,
  type Transport,
} from 'viem';
import type { ChainConfig, Logger } from '../types.js';
import { consoleLogger } from '../logger.js';
import { RpcUsage } from './rpc-usage.js';
import { flattenError, RANGE_TOO_LARGE, RATE_LIMITED, statedLogSpan, TRANSIENT } from './errors.js';

/** Bounded so a recovery range cannot fire hundreds of block reads at once (that is how throttling starts). */
const BLOCK_READ_CONCURRENCY = 8;

/** Attempts for a rate-limited or transient call, and the base of their backoff. */
const RETRY_ATTEMPTS = 5;
const RETRY_BASE_MS = 400;
const RETRY_MAX_MS = 8_000;

/** Block timestamps kept in memory (blocks are immutable once settled). */
const BLOCK_TIME_CACHE = 5_000;

/**
 * A learned eth_getLogs window is forgotten this long after the provider last
 * rejected a range. Re-learning a real cap costs one rejection when the
 * provider states its window (Alchemy does), a short bisection otherwise; a
 * window learned from a misread error heals by itself instead of splitting
 * every later range forever.
 */
export const LOG_LIMIT_TTL_MS = 10 * 60_000;

export interface GetLogsOptions {
  fromBlock: bigint;
  toBlock: bigint;
  address?: Hex[];
  topics?: (Hex | Hex[] | null)[];
}

export type TransportFactory = (chain: ChainConfig, usage: RpcUsage) => Transport;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Read-only chain access for the indexer: settled head, block hashes and
 * times, and an eth_getLogs that adapts to whatever the provider allows.
 * Holds no keys and creates no wallet client.
 */
export class ChainClient {
  readonly usage = new RpcUsage();
  private readonly clients = new Map<number, PublicClient>();
  private readonly blockTimeCache = new Map<string, Date>();
  private readonly lastSafeHead = new Map<number, bigint>();
  private readonly distrusted = new Set<number>();
  /**
   * Per chain AND query kind (topic-filtered vs address-filtered: their
   * response sizes differ by orders of magnitude): the widest span to send in
   * one call, known ONLY once the provider rejected a wider one, and when.
   */
  private readonly logLimit = new Map<string, { width: bigint; since: number }>();
  private readonly transportFactory: TransportFactory;
  private readonly log: Logger;

  constructor(
    private readonly chains: Map<number, ChainConfig>,
    options: { transport?: TransportFactory; logger?: Logger } = {},
  ) {
    this.transportFactory = options.transport ?? ChainClient.defaultTransport;
    this.log = options.logger ?? consoleLogger('ChainClient');
  }

  /** A fallback across every endpoint, each counting what it sends. */
  private static defaultTransport(chain: ChainConfig, usage: RpcUsage): Transport {
    return fallback(
      chain.rpcUrls.map((url) =>
        http(url, {
          retryCount: 2,
          retryDelay: 250,
          timeout: 15_000,
          batch: true,
          onFetchRequest: (_request, init) => usage.recordBody(chain.chainId, init.body),
        }),
      ),
      { retryCount: 1 },
    );
  }

  chain(chainId: number): ChainConfig {
    const chain = this.chains.get(chainId);
    if (!chain) throw new Error(`unknown chain id ${chainId}`);
    return chain;
  }

  client(chainId: number): PublicClient {
    const cached = this.clients.get(chainId);
    if (cached) return cached;
    const client = createPublicClient({
      transport: this.transportFactory(this.chain(chainId), this.usage),
    }) as PublicClient;
    this.clients.set(chainId, client);
    return client;
  }

  async headBlock(chainId: number): Promise<bigint> {
    return this.withRetry(chainId, 'getBlockNumber', () => this.client(chainId).getBlockNumber({ cacheTime: 0 }));
  }

  /**
   * The highest block considered settled enough to ingest.
   *
   * NEVER RETURNS A LOWER VALUE THAN IT ALREADY HAS for a chain. Fallback
   * transports rotate endpoints, and one endpoint can serve a head (or a
   * frozen `finalized` pointer) hundreds of blocks behind another; without
   * this floor the cursor would re-ingest or skip between ticks.
   */
  async safeHead(chainId: number): Promise<bigint> {
    const head = await this.headBlock(chainId);
    const candidate = await this.settledHead(chainId, head);
    const floor = this.lastSafeHead.get(chainId) ?? 0n;
    const result = candidate > floor ? candidate : floor;
    this.lastSafeHead.set(chainId, result);
    return result;
  }

  private byConfirmations(chainId: number, head: bigint): bigint {
    const depth = BigInt(this.chain(chainId).confirmations);
    return head > depth ? head - depth : 0n;
  }

  private async settledHead(chainId: number, head: bigint): Promise<bigint> {
    const finality = this.chain(chainId).finality ?? { mode: 'confirmations' };
    if (finality.mode === 'confirmations') return this.byConfirmations(chainId, head);

    let finalized: bigint;
    try {
      const block = await this.client(chainId).getBlock({ blockTag: 'finalized' });
      finalized = block.number ?? 0n;
    } catch (err) {
      this.warnDistrust(chainId, `finalized unreadable: ${flattenError(err)}`);
      return this.byConfirmations(chainId, head);
    }
    if (finalized > head) {
      this.warnDistrust(chainId, `finalized ${finalized} is ahead of head ${head}`);
      return this.byConfirmations(chainId, head);
    }
    if (head - finalized > BigInt(finality.maxLagBlocks)) {
      this.warnDistrust(chainId, `finalized ${finalized} lags head ${head} by ${head - finalized} blocks`);
      return this.byConfirmations(chainId, head);
    }
    this.distrusted.delete(chainId);
    return finalized;
  }

  private warnDistrust(chainId: number, reason: string): void {
    if (this.distrusted.has(chainId)) return;
    this.distrusted.add(chainId);
    this.log.warn(`chain ${chainId}: distrusting the finalized tag, using confirmations - ${reason}`);
  }

  async getBlockHash(chainId: number, blockNumber: bigint): Promise<Hex> {
    const block = await this.withRetry(chainId, `getBlock ${blockNumber}`, () =>
      this.client(chainId).getBlock({ blockNumber }),
    );
    if (!block.hash) throw new Error(`chain ${chainId}: block ${blockNumber} has no hash yet`);
    return block.hash;
  }

  /**
   * Block timestamps, deduplicated, memoized and read with bounded
   * concurrency. Every log in a block shares its time, so this costs one read
   * per distinct block, not per log.
   */
  async blockTimes(chainId: number, blockNumbers: bigint[]): Promise<Map<bigint, Date>> {
    const out = new Map<bigint, Date>();
    const missing: bigint[] = [];
    for (const n of new Set(blockNumbers)) {
      const hit = this.blockTimeCache.get(`${chainId}:${n}`);
      if (hit) out.set(n, hit);
      else missing.push(n);
    }

    let next = 0;
    const worker = async () => {
      while (next < missing.length) {
        const n = missing[next++]!;
        const block = await this.withRetry(chainId, `getBlock ${n}`, () =>
          this.client(chainId).getBlock({ blockNumber: n }),
        );
        const at = new Date(Number(block.timestamp) * 1000);
        this.rememberBlockTime(chainId, n, at);
        out.set(n, at);
      }
    };
    await Promise.all(Array.from({ length: Math.min(BLOCK_READ_CONCURRENCY, missing.length) }, worker));
    return out;
  }

  private rememberBlockTime(chainId: number, blockNumber: bigint, at: Date): void {
    this.blockTimeCache.set(`${chainId}:${blockNumber}`, at);
    if (this.blockTimeCache.size > BLOCK_TIME_CACHE) {
      const oldest = this.blockTimeCache.keys().next().value;
      if (oldest !== undefined) this.blockTimeCache.delete(oldest);
    }
  }

  /**
   * Retry the calls worth retrying (throttles, dropped sockets), and only
   * those, with jittered exponential backoff. An oversized-range error is NOT
   * retried here: repeating it cannot succeed, and getLogsAdaptive answers it
   * by splitting instead.
   */
  async withRetry<T>(chainId: number, what: string, fn: () => Promise<T>): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt < RETRY_ATTEMPTS; attempt++) {
      try {
        return await fn();
      } catch (err) {
        const message = flattenError(err);
        if (!RATE_LIMITED.test(message) && !TRANSIENT.test(message)) throw err;
        lastError = err;
        if (attempt === RETRY_ATTEMPTS - 1) break;
        const wait = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** attempt);
        await sleep(wait / 2 + Math.random() * (wait / 2));
        this.log.debug(`chain ${chainId}: retrying ${what} after ${message}`);
      }
    }
    throw lastError;
  }

  /**
   * `eth_getLogs` that adapts to a provider's size limit instead of failing
   * the range, and REMEMBERS the limit instead of rediscovering it per call.
   *
   * THE LIMIT IS LEARNED FROM REJECTIONS ONLY. Remembering "the largest span
   * that ever succeeded" instead is a trap: pre-splitting means nothing wider
   * is ever tried again, so a first range that happened to be 4 blocks caps
   * every later one at 4 blocks for the life of the process. Without a
   * rejection there is no limit and a range goes out whole.
   *
   * A rejection that states the window ("up to a 2K block range") sets the
   * limit in one step. One that does not is bisected, and the narrowed width
   * applies to the rest of the same call. A learned limit expires
   * LOG_LIMIT_TTL_MS after the last rejection, so a wider allowance (or a
   * limit learned from a misread error) is picked up again without a restart.
   */
  async getLogsAdaptive(chainId: number, opts: GetLogsOptions): Promise<Log[]> {
    const key = `${chainId}:${opts.address && opts.address.length > 0 ? 'address' : 'topics'}`;
    const client = this.client(chainId);

    // A raw request rather than `client.getLogs`: viem's action rebuilds the
    // topic list from an `event`, and an OR-list of topic0 values is exactly
    // what the topic phase needs to send as-is.
    const call = (from: bigint, to: bigint): Promise<Log[]> =>
      this.withRetry(chainId, `getLogs ${from}-${to}`, async () => {
        const logs = (await client.request({
          method: 'eth_getLogs',
          params: [
            {
              ...(opts.address && opts.address.length > 0 ? { address: opts.address } : {}),
              ...(opts.topics ? { topics: opts.topics } : {}),
              fromBlock: numberToHex(from),
              toBlock: numberToHex(to),
            },
          ],
        } as never)) as RpcLog[];
        return logs.map((log) => formatLog(log));
      });

    const bisect = async (from: bigint, to: bigint): Promise<Log[]> => {
      try {
        return await call(from, to);
      } catch (err) {
        const message = flattenError(err);
        if (RATE_LIMITED.test(message) || !RANGE_TOO_LARGE.test(message)) throw err;
        if (from === to) {
          throw new Error(`provider cannot return logs for a single block (${from}) on chain ${chainId}: ${message}`);
        }

        const span = to - from + 1n;
        const stated = statedLogSpan(message);
        if (stated !== null && stated < span) {
          this.narrowLogSpan(chainId, key, stated);
          this.log.warn(`chain ${chainId}: ${from}-${to} over the provider's stated ${stated}-block window; splitting`);
          return split(from, to);
        }

        // Halve, then re-split at the (possibly narrower) learned width, so
        // every chunk after this one goes out at the narrowest width known.
        this.narrowLogSpan(chainId, key, span / 2n);
        this.log.warn(`chain ${chainId}: bisecting ${from}-${to}: ${message}`);
        return split(from, to);
      }
    };

    // Re-reads the width for EVERY chunk: a rejection part-way through
    // narrows the rest of this call too, not just the next one.
    const split = async (fromBlock: bigint, toBlock: bigint): Promise<Log[]> => {
      const logs: Log[] = [];
      let from = fromBlock;
      while (from <= toBlock) {
        const width = this.currentLogWidth(key);
        const to = width === undefined || from + width - 1n > toBlock ? toBlock : from + width - 1n;
        logs.push(...(await bisect(from, to)));
        from = to + 1n;
      }
      return logs;
    };

    return split(opts.fromBlock, opts.toBlock);
  }

  private currentLogWidth(key: string): bigint | undefined {
    const limit = this.logLimit.get(key);
    if (!limit) return undefined;
    if (Date.now() - limit.since >= LOG_LIMIT_TTL_MS) {
      this.logLimit.delete(key);
      return undefined;
    }
    return limit.width;
  }

  /** Narrow immediately: a rejection is definitive about what does not work. */
  private narrowLogSpan(chainId: number, key: string, span: bigint): void {
    const capped = span < 1n ? 1n : span;
    const limit = this.logLimit.get(key);
    if (!limit || capped < limit.width) {
      this.logLimit.set(key, { width: capped, since: Date.now() });
      this.log.warn(`chain ${chainId}: provider log window narrowed to ${capped} blocks (${key.split(':')[1]} queries)`);
    } else {
      limit.since = Date.now();
    }
  }
}
