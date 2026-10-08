import { afterEach, describe, expect, it, vi } from 'vitest';
import { RpcUsage } from '../src/chain/rpc-usage.js';
import type { RangeSummary } from '../src/indexer/indexer.js';
import { Runner } from '../src/runner.js';
import type { ChainConfig, Logger } from '../src/types.js';

const A = 1;
const B = 2;

const chainCfg = (chainId: number, extra: Partial<ChainConfig> = {}): ChainConfig => ({
  chainId,
  name: `chain-${chainId}`,
  rpcUrls: ['https://rpc.example'],
  startBlock: 0n,
  confirmations: 4,
  blockTimeMs: 1_000,
  ...extra,
});

const atHead = (toBlock: bigint, triggerEvents = 0): RangeSummary => ({
  fromBlock: toBlock,
  toBlock,
  events: triggerEvents,
  triggerEvents,
  atHead: true,
});

interface TriggerFake {
  mode: () => 'off' | 'live' | 'down';
  pending: () => { max: bigint; since: number } | null;
  ingested: (c: number, b: bigint) => void;
  notices: () => number;
  takeCatchUp: () => boolean;
  stopAll: () => void;
}

function build(o: { runOnce?: (chainId: number) => Promise<RangeSummary | null>; chains?: ChainConfig[]; trigger?: Partial<TriggerFake>; acquire?: () => Promise<boolean> } = {}) {
  const runOnce = vi.fn(o.runOnce ?? (async () => null));
  const lag = vi.fn(async () => ({ blocks: 0n, seconds: 0 }));
  const warn = vi.fn();
  const log: Logger = { debug() {}, info() {}, warn, error() {} };
  const trigger: TriggerFake = {
    mode: () => 'off',
    pending: () => null,
    ingested: vi.fn(),
    notices: () => 0,
    takeCatchUp: () => false,
    stopAll: vi.fn(),
    ...o.trigger,
  };
  const chains = new Map((o.chains ?? [chainCfg(A)]).map((c) => [c.chainId, c]));
  const runner = new Runner(
    chains,
    { acquire: o.acquire ?? (async () => true), release: async () => {} } as never,
    { runOnce, lag } as never,
    { load: vi.fn(async () => {}) } as never,
    trigger as never,
    { usage: new RpcUsage() } as never,
    { pollMs: 2_000, sweepMs: 60_000, drainBudgetMs: 15_000, usageLogMs: 0 },
    log,
  );
  const delays: number[] = [];
  vi.spyOn(runner as unknown as { schedule: (ms: number) => void }, 'schedule').mockImplementation((ms) => void delays.push(ms));
  const callsFor = (id: number) => runOnce.mock.calls.filter((c) => c[0] === id).length;
  return { runner, runOnce, lag, warn, trigger, delays, callsFor };
}

describe('Runner', () => {
  afterEach(() => vi.useRealTimers());

  it('stops at the safe head instead of asking again (no head-chasing)', async () => {
    const { runner, runOnce, lag, delays } = build({ runOnce: async () => atHead(10n) });
    await runner.tick();
    expect(runOnce).toHaveBeenCalledTimes(1);
    expect(delays).toEqual([2_000]);
    expect(lag).not.toHaveBeenCalled(); // nothing to report at the tip
  });

  it('comes straight back while a backfill is behind', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const runOnce = vi.fn(async (): Promise<RangeSummary> => {
      vi.advanceTimersByTime(4_000);
      return { fromBlock: 1n, toBlock: 2n, events: 0, triggerEvents: 0, atHead: false };
    });
    const { runner, delays } = build({ runOnce });
    await runner.tick();
    expect(delays).toEqual([0]);
    expect(runOnce.mock.calls.length).toBeGreaterThan(1);
  });

  it('keeps draining through a reorg rewind', async () => {
    const runOnce = vi
      .fn()
      .mockResolvedValueOnce({ fromBlock: 5n, toBlock: 5n, events: 0, triggerEvents: 0, reorgedTo: 5n })
      .mockResolvedValueOnce(atHead(11n));
    const { runner } = build({ runOnce });
    await runner.tick();
    expect(runOnce).toHaveBeenCalledTimes(2);
  });

  it('backs a failing chain off per chain while the healthy one keeps its pace', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const { runner, callsFor, delays } = build({
      chains: [chainCfg(A), chainCfg(B)],
      runOnce: async (id) => {
        if (id === A) throw new Error('provider exploded');
        return null;
      },
    });
    await runner.tick(); // A fails: 1s
    vi.advanceTimersByTime(500);
    await runner.tick(); // skipped
    expect(callsFor(A)).toBe(1);
    vi.advanceTimersByTime(600);
    await runner.tick(); // retried, fails: 2s
    vi.advanceTimersByTime(1_500);
    await runner.tick(); // skipped
    expect(callsFor(A)).toBe(2);
    expect(callsFor(B)).toBe(4);
    expect(delays).toEqual([2_000, 2_000, 2_000, 2_000]);
  });

  it('closes sockets and does nothing while another instance holds leadership', async () => {
    const { runner, runOnce, trigger } = build({ acquire: async () => false });
    await runner.tick();
    expect(runOnce).not.toHaveBeenCalled();
    expect(trigger.stopAll).toHaveBeenCalled();
  });

  describe('with a live websocket', () => {
    const live = (extra: Partial<TriggerFake> = {}) => ({ mode: () => 'live' as const, ...extra });

    it('makes no pass on an idle chain between sweeps', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { runner, runOnce } = build({ runOnce: async () => atHead(10n), trigger: live() });
      await runner.tick(); // first sweep
      for (let i = 0; i < 25; i++) {
        vi.advanceTimersByTime(2_000);
        await runner.tick();
      }
      expect(runOnce).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(10_000);
      await runner.tick(); // 60s since the last pass
      expect(runOnce).toHaveBeenCalledTimes(2);
    });

    it('ingests a notified block once confirmations x block time have passed', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      let pending: { max: bigint; since: number } | null = null;
      const { runner, runOnce, trigger } = build({ runOnce: async () => atHead(20n, 1), trigger: live({ pending: () => pending }) });
      await runner.tick();
      pending = { max: 20n, since: Date.now() };
      vi.advanceTimersByTime(2_000);
      await runner.tick(); // 4 x 1s + 0.5s not yet
      expect(runOnce).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(3_000);
      await runner.tick();
      expect(runOnce).toHaveBeenCalledTimes(2);
      expect(trigger.ingested).toHaveBeenLastCalledWith(A, 20n);
    });

    it('polls every tick while the socket is down', async () => {
      const { runner, runOnce } = build({ runOnce: async () => atHead(5n), trigger: { mode: () => 'down' } });
      await runner.tick();
      await runner.tick();
      await runner.tick();
      expect(runOnce).toHaveBeenCalledTimes(3);
    });

    it('sweeps right after a reconnect', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      let reconnected = false;
      const { runner, runOnce } = build({
        runOnce: async () => atHead(5n),
        trigger: live({ takeCatchUp: () => reconnected && !(reconnected = false) }),
      });
      await runner.tick();
      reconnected = true;
      vi.advanceTimersByTime(2_000);
      await runner.tick();
      expect(runOnce).toHaveBeenCalledTimes(2);
    });

    it('warns when a plain sweep finds events the socket never announced', async () => {
      const { runner, warn } = build({ runOnce: async () => atHead(9n, 2), trigger: live() });
      await runner.tick();
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('2 event(s) the websocket did not announce'));
    });

    it('does not warn when the events were announced', async () => {
      const { runner, warn } = build({
        runOnce: async () => atHead(9n, 2),
        trigger: live({ pending: () => ({ max: 9n, since: 0 }) }),
      });
      await runner.tick();
      expect(warn).not.toHaveBeenCalled();
    });

    it('tries a notified block after 3s on a finalized-mode chain, ignoring its fallback depth', async () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      let pending: { max: bigint; since: number } | null = null;
      const { runner, runOnce } = build({
        chains: [chainCfg(A, { confirmations: 128, blockTimeMs: 2_000, finality: { mode: 'finalized', maxLagBlocks: 256 } })],
        runOnce: async () => atHead(20n, 1),
        trigger: live({ pending: () => pending }),
      });
      await runner.tick();
      pending = { max: 20n, since: Date.now() };
      vi.advanceTimersByTime(3_100);
      await runner.tick();
      expect(runOnce).toHaveBeenCalledTimes(2);
    });
  });
});
