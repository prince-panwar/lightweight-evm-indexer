import type { ChainClient } from './chain/chain-client.js';
import type { Indexer } from './indexer/indexer.js';
import type { LeaderLock } from './indexer/leader.js';
import type { WatchSet } from './indexer/watch-set.js';
import type { LogTrigger, TriggerMode } from './trigger/log-trigger.js';
import type { ChainConfig, Logger } from './types.js';
import { errorMessage } from './logger.js';

export interface RunnerTiming {
  /** Tick interval, and the poll interval for chains without a live websocket. */
  pollMs: number;
  /** With a live websocket: how often a chain is swept anyway (safety net + moves the cursor over empty blocks). */
  sweepMs: number;
  /** How long one chain may drain before the tick moves on to the next. */
  drainBudgetMs: number;
  /** How often per-chain RPC usage is logged (0 = never). */
  usageLogMs: number;
}

/** Why a chain gets a pass this tick; only a plain 'sweep' can reveal missed notifications. */
type DueReason = 'poll' | 'behind' | 'catch-up' | 'sweep' | 'pending';

const BACKOFF_BASE_MS = 1_000;
const BACKOFF_MAX_MS = 60_000;
/** Slack on top of confirmations x block time before the first pass for a notified block. */
const SETTLE_MARGIN_MS = 500;
/** First try for a notified block on a chain whose safe head is its finalized tag. */
const FINALIZED_SETTLE_MS = 3_000;
const PROGRESS_LOG_MS = 30_000;

/**
 * Drives ingestion: a self-scheduling timeout (never a fixed interval, so a
 * slow tick can never overlap the next), one chain at a time, each in its own
 * try so one sick RPC cannot stop the others.
 */
export class Runner {
  private timer: NodeJS.Timeout | null = null;
  private stopping = false;
  private readonly prepared = new Set<number>();
  private readonly failures = new Map<number, number>();
  /** A failing chain is SKIPPED until its backoff expires; the tick itself keeps its pace for healthy chains. */
  private readonly notBefore = new Map<number, number>();
  private readonly lastPassAt = new Map<number, number>();
  /** Chains whose last drain ran out of budget before reaching the safe head. */
  private readonly behind = new Set<number>();
  private readonly lastProgressLogAt = new Map<number, number>();
  private lastUsageLogAt = 0;
  private running: Promise<void> | null = null;

  constructor(
    private readonly chains: Map<number, ChainConfig>,
    private readonly leader: LeaderLock,
    private readonly indexer: Indexer,
    private readonly watched: WatchSet,
    private readonly trigger: LogTrigger,
    private readonly chain: ChainClient,
    private readonly timing: RunnerTiming,
    private readonly log: Logger,
  ) {}

  start(): void {
    this.stopping = false;
    this.schedule(0);
  }

  /** Stop scheduling, wait for the tick in flight, close sockets and leadership. */
  async stop(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    await this.running;
    this.trigger.stopAll();
    await this.leader.release();
  }

  private schedule(delayMs: number): void {
    if (this.stopping) return;
    this.timer = setTimeout(() => {
      this.running = this.tick().finally(() => {
        this.running = null;
      });
    }, delayMs);
  }

  /** @internal exposed for tests */
  async tick(): Promise<void> {
    if (this.stopping) return;
    let delay = this.timing.pollMs;

    try {
      if (!(await this.leader.acquire())) {
        // A standby (or a demoted leader) pays for no notifications.
        this.trigger.stopAll();
        return this.schedule(this.timing.pollMs);
      }

      for (const [chainId, chain] of this.chains) {
        if (this.stopping) break;
        if (Date.now() < (this.notBefore.get(chainId) ?? 0)) continue;

        try {
          await this.prepare(chainId);
          const mode = this.trigger.mode(chainId);
          const reason = this.dueReason(chainId, chain, mode);
          if (!reason) continue;

          const pendingBefore = this.trigger.pending(chainId);
          const noticesBefore = this.trigger.notices(chainId);
          const result = await this.drain(chainId);
          this.lastPassAt.set(chainId, Date.now());
          if (result.lastToBlock !== null) this.trigger.ingested(chainId, result.lastToBlock);

          if (result.caughtUp) {
            this.behind.delete(chainId);
          } else {
            // Ran out of budget mid-backfill: come straight back.
            this.behind.add(chainId);
            delay = 0;
          }

          // The websocket's health signal: a plain sweep (nothing pending, no
          // notification meanwhile) that still found trigger events means the
          // socket missed them. They are ingested either way.
          if (
            reason === 'sweep' &&
            pendingBefore === null &&
            this.trigger.notices(chainId) === noticesBefore &&
            result.triggerEvents > 0
          ) {
            this.log.warn(`chain ${chainId}: sweep ingested ${result.triggerEvents} event(s) the websocket did not announce`);
          }

          this.failures.delete(chainId);
          this.notBefore.delete(chainId);
        } catch (err) {
          this.notBefore.set(chainId, Date.now() + this.recordFailure(chainId, err));
        }
      }
    } catch (err) {
      this.log.error(`indexer tick failed: ${errorMessage(err)}`, err);
    }

    this.maybeLogUsage();
    this.schedule(delay);
  }

  /**
   * Without a live websocket: always (plain polling). With one, only with a
   * reason, so an idle chain costs nothing between sweeps.
   */
  private dueReason(chainId: number, chain: ChainConfig, mode: TriggerMode): DueReason | null {
    if (mode !== 'live') return 'poll';
    if (this.behind.has(chainId)) return 'behind';
    if (this.trigger.takeCatchUp(chainId)) return 'catch-up';

    const now = Date.now();
    const last = this.lastPassAt.get(chainId) ?? 0;
    if (now - last >= this.timing.sweepMs) return 'sweep';

    const pending = this.trigger.pending(chainId);
    if (!pending) return null;
    const settleMs =
      chain.finality?.mode === 'finalized'
        ? FINALIZED_SETTLE_MS
        : chain.confirmations * chain.blockTimeMs + SETTLE_MARGIN_MS;
    return now >= pending.since + settleMs && now - last >= this.timing.pollMs ? 'pending' : null;
  }

  /**
   * Ingest one chain until caught up or out of budget. Caught up = nothing to
   * ingest, or the last range reached the safe head; never "ask again until
   * nothing is new", which on a fast chain never ends.
   */
  private async drain(chainId: number) {
    const deadline = Date.now() + this.timing.drainBudgetMs;
    let ranges = 0;
    let caughtUp = false;
    let lastToBlock: bigint | null = null;
    let triggerEvents = 0;

    while (!this.stopping && Date.now() < deadline) {
      const summary = await this.indexer.runOnce(chainId);
      if (summary === null) {
        caughtUp = true;
        break;
      }
      ranges++;
      if (summary.reorgedTo === undefined) {
        lastToBlock = summary.toBlock;
        triggerEvents += summary.triggerEvents;
      }
      if (summary.atHead) {
        caughtUp = true;
        break;
      }
    }

    if (ranges > 0 && !caughtUp) await this.reportProgress(chainId);
    return { caughtUp, lastToBlock, triggerEvents };
  }

  /** Rehydrate the watched set once per chain; a failure is retried next tick. */
  private async prepare(chainId: number): Promise<void> {
    if (this.prepared.has(chainId)) return;
    await this.watched.load(chainId);
    this.prepared.add(chainId);
  }

  /** A backfill is quiet by design, so say how far behind it is, at most every 30s. */
  private async reportProgress(chainId: number): Promise<void> {
    const now = Date.now();
    if (now - (this.lastProgressLogAt.get(chainId) ?? 0) < PROGRESS_LOG_MS) return;
    this.lastProgressLogAt.set(chainId, now);
    try {
      const { blocks, seconds } = await this.indexer.lag(chainId);
      if (blocks > 0n) this.log.info(`chain ${chainId}: backfilling, ${blocks} blocks (${Math.round(seconds / 60)} min) behind`);
    } catch {
      // Reporting must never be the thing that fails a tick.
    }
  }

  private recordFailure(chainId: number, err: unknown): number {
    const count = (this.failures.get(chainId) ?? 0) + 1;
    this.failures.set(chainId, count);
    const backoff = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** Math.min(count - 1, 6));
    this.log.error(`chain ${chainId}: pass failed (${count} in a row, retrying in ${backoff}ms): ${errorMessage(err)}`, err);
    return backoff;
  }

  /** Once per interval: RPC calls per chain and method. The number any polling change must be judged by. */
  private maybeLogUsage(): void {
    if (this.timing.usageLogMs <= 0) return;
    const now = Date.now();
    if (now - this.lastUsageLogAt < this.timing.usageLogMs) return;
    const seconds = this.lastUsageLogAt ? Math.round((now - this.lastUsageLogAt) / 1000) : 0;
    this.lastUsageLogAt = now;
    const usage = this.chain.usage.take();
    if (!seconds) return; // the first call only opens the window
    for (const [chainId, byMethod] of usage) {
      const billable = [...byMethod.entries()].filter(([m]) => m !== 'ws_ignored');
      const total = billable.reduce((a, [, n]) => a + n, 0);
      const detail = [...byMethod.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([m, n]) => `${m} ${n}`)
        .join(', ');
      this.log.info(`chain ${chainId}: ${total} rpc calls in ${seconds}s (${detail})`);
    }
  }
}
