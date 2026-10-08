import { afterEach, describe, expect, it, vi } from 'vitest';
import { custom } from 'viem';
import { ChainClient, LOG_LIMIT_TTL_MS } from '../src/chain/chain-client.js';
import type { ChainConfig, Logger } from '../src/types.js';

const CHAIN = 8453;
const quiet: Logger = { debug() {}, info() {}, warn() {}, error() {} };

interface Call {
  from: bigint;
  width: bigint;
  address: boolean;
  ok: boolean;
}

/** A provider whose eth_getLogs answer is decided by `reject(width, from, hasAddress)`. */
function provider(reject: (width: bigint, from: bigint, hasAddress: boolean) => string | null, chainOverrides: Partial<ChainConfig> = {}) {
  const calls: Call[] = [];
  let head = 1_000n;
  const chain: ChainConfig = {
    chainId: CHAIN,
    name: 'test',
    rpcUrls: ['http://mock'],
    startBlock: 0n,
    confirmations: 4,
    blockTimeMs: 2_000,
    ...chainOverrides,
  };
  const client = new ChainClient(new Map([[CHAIN, chain]]), {
    logger: quiet,
    transport: () =>
      custom(
        {
          async request({ method, params }: { method: string; params: unknown }) {
            if (method === 'eth_blockNumber') return `0x${head.toString(16)}`;
            if (method !== 'eth_getLogs') throw new Error(`unexpected ${method}`);
            const p = (params as Record<string, string | string[]>[])[0]!;
            const from = BigInt(p.fromBlock as string);
            const width = BigInt(p.toBlock as string) - from + 1n;
            const err = reject(width, from, Boolean(p.address));
            calls.push({ from, width, address: Boolean(p.address), ok: err === null });
            if (err) throw new Error(err);
            return [];
          },
        },
        { retryCount: 0 },
      ),
  });
  return { client, calls, setHead: (h: bigint) => (head = h) };
}

/** Every block exactly once, in order, as contiguous successful calls. */
function expectContiguous(calls: Call[], from: bigint, to: bigint) {
  let next = from;
  for (const c of calls.filter((x) => x.ok)) {
    expect(c.from).toBe(next);
    next = c.from + c.width;
  }
  expect(next).toBe(to + 1n);
}

const TOPIC = '0xaa00000000000000000000000000000000000000000000000000000000000001' as const;

describe('getLogsAdaptive', () => {
  afterEach(() => vi.useRealTimers());

  it('sends a range whole when no limit is known', async () => {
    const { client, calls } = provider(() => null);
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 999n });
    expect(calls.map((c) => c.width)).toEqual([1000n]);
  });

  it('does not let a small first range cap later ranges', async () => {
    const { client, calls } = provider(() => null);
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 100n, toBlock: 103n }); // 4 blocks
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 104n, toBlock: 133n }); // 30 blocks
    expect(calls.map((c) => c.width)).toEqual([30n]);
  });

  it('needs ONE rejection when the provider states its window', async () => {
    const { client, calls } = provider((w) =>
      w > 10n ? 'Log response size exceeded. You can make eth_getLogs requests with up to a 10 block range' : null,
    );
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 1999n });
    expect(calls.filter((c) => !c.ok)).toHaveLength(1);
    expectContiguous(calls, 0n, 1999n);
  });

  it('bisects an unstated limit and covers the range contiguously, in order', async () => {
    const { client, calls } = provider((w, from) => (w > 7n && from % 3n === 0n ? 'block range is too wide' : null));
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 100n, toBlock: 333n });
    expectContiguous(calls, 100n, 333n);
  });

  it('remembers a rejection: later ranges are pre-split with no further rejections', async () => {
    const { client, calls } = provider((w) => (w > 100n ? 'query returned more than 10000 results' : null));
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 399n });
    const rejections = calls.filter((c) => !c.ok).length;
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 400n, toBlock: 799n });
    expect(calls.every((c) => c.ok)).toBe(true);
    expect(rejections).toBeGreaterThan(0);
    expectContiguous(calls, 400n, 799n);
  });

  it('applies a narrowing to the rest of the same call', async () => {
    let cap = 100n;
    const { client, calls } = provider((w) => (w > cap ? 'query returned more than 10000 results' : null));
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 199n }); // learns 100
    cap = 25n;
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 1_000n, toBlock: 1_399n });
    const firstOk = calls.findIndex((c) => c.ok);
    expect(calls.slice(firstOk).filter((c) => !c.ok)).toHaveLength(0);
    expectContiguous(calls, 1_000n, 1_399n);
  });

  it('does not let a quiet wider success undo a narrowing (response-size caps)', async () => {
    const { client, calls } = provider((w, from) => (from < 50n && w > 50n ? 'query returned more than 10000 results' : null));
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 99n });
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 99n });
    expect(calls.map((c) => c.width)).toEqual([50n, 50n]);
  });

  it('keeps separate limits for topic and address queries', async () => {
    const { client, calls } = provider((w, _from, hasAddress) =>
      !hasAddress && w > 10n ? 'You can make eth_getLogs requests with up to a 10 block range' : null,
    );
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 99n });
    await client.getLogsAdaptive(CHAIN, { address: ['0x0000000000000000000000000000000000000001'], fromBlock: 0n, toBlock: 99n });
    expect(calls.filter((c) => c.address).map((c) => c.width)).toEqual([100n]);
  });

  it('forgets a learned limit after LOG_LIMIT_TTL_MS', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    let capped = true;
    const { client, calls } = provider((w) => (capped && w > 50n ? 'block range is too wide' : null));
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 99n }); // learns 50
    capped = false;

    vi.advanceTimersByTime(LOG_LIMIT_TTL_MS - 1);
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 1_000n, toBlock: 1_099n });
    expect(calls.map((c) => c.width)).toEqual([50n, 50n]);

    vi.advanceTimersByTime(2);
    calls.length = 0;
    await client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 2_000n, toBlock: 2_099n });
    expect(calls.map((c) => c.width)).toEqual([100n]);
  });

  it('refuses (throws) when a single block is too large', async () => {
    const { client } = provider(() => 'query returned more than 10000 results');
    await expect(client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 5n, toBlock: 6n })).rejects.toThrow(/single block/);
  });

  it('propagates an unrelated error instead of bisecting on it', async () => {
    const { client, calls } = provider(() => 'invalid API key');
    await expect(client.getLogsAdaptive(CHAIN, { topics: [[TOPIC]], fromBlock: 0n, toBlock: 99n })).rejects.toThrow(/invalid API key/);
    expect(calls).toHaveLength(1);
  });
});

describe('safeHead', () => {
  it('is head minus confirmations, and never moves backwards', async () => {
    const { client, setHead } = provider(() => null);
    setHead(1_000n);
    expect(await client.safeHead(CHAIN)).toBe(996n);
    setHead(990n); // a lagging endpoint behind a fallback transport
    expect(await client.safeHead(CHAIN)).toBe(996n);
    setHead(1_010n);
    expect(await client.safeHead(CHAIN)).toBe(1_006n);
  });
});
