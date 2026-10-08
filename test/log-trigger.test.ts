import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { encodeAbiParameters, encodeEventTopics, parseAbiItem, type Address } from 'viem';
import { RpcUsage } from '../src/chain/rpc-usage.js';
import { Decoder } from '../src/indexer/decoder.js';
import { WatchSet } from '../src/indexer/watch-set.js';
import { LogTrigger, socketUrl } from '../src/trigger/log-trigger.js';
import type { ChainConfig, IndexerPlugin, Logger } from '../src/types.js';
import { FakeSocket } from './fake-socket.js';

const quiet: Logger = { debug() {}, info() {}, warn() {}, error() {} };
const CHAIN = 1;
const FACTORY = '0xfac0000000000000000000000000000000000001';
const CHILD = '0xc000000000000000000000000000000000000002';
const STRANGER = '0xbad0000000000000000000000000000000000bad';

const created = parseAbiItem('event Created(address indexed child)');
const ping = parseAbiItem('event Ping(uint256 value)');

/** A factory-style plugin: Created from the factory introduces a child; Ping is trusted from known children. */
const plugin: IndexerPlugin = {
  events: [created, ping],
  async loadWatched() {
    return [];
  },
  isTrusted: (e, ctx) => (e.eventName === 'Created' ? e.address === FACTORY : ctx.watched.has(e.address)),
  discover: (e) => (e.eventName === 'Created' ? [e.args.child as Address] : []),
  async handle() {},
  async rewind() {},
};

const rpcLog = (address: string, block: number, topics: readonly string[], data: string) => ({
  address,
  blockNumber: `0x${block.toString(16)}`,
  blockHash: `0x${'11'.repeat(32)}`,
  transactionHash: `0x${'22'.repeat(32)}`,
  transactionIndex: '0x0',
  logIndex: '0x0',
  topics,
  data,
  removed: false,
});
const createdLog = (from: string, block: number) =>
  rpcLog(from, block, encodeEventTopics({ abi: [created], args: { child: CHILD } }) as string[], '0x');
const pingLog = (from: string, block: number) =>
  rpcLog(from, block, encodeEventTopics({ abi: [ping] }) as string[], encodeAbiParameters([{ type: 'uint256' }], [7n]));

function setup(opts: { websocket?: boolean; max?: number } = {}) {
  const chain: ChainConfig = {
    chainId: CHAIN,
    name: 'test',
    rpcUrls: ['https://rpc.example/v2/key'],
    startBlock: 0n,
    confirmations: 0,
    blockTimeMs: 1_000,
    websocket: opts.websocket ?? true,
  };
  const decoder = new Decoder(plugin.events);
  const watched = new WatchSet({} as never, plugin);
  const sockets: FakeSocket[] = [];
  const usage = new RpcUsage();
  const trigger = new LogTrigger(
    new Map([[CHAIN, chain]]),
    decoder,
    watched,
    plugin,
    decoder.selectors(),
    usage,
    quiet,
    (url) => {
      const s = new FakeSocket(url);
      sockets.push(s);
      return s;
    },
    { maxNotificationsPerMinute: opts.max ?? 600, busyCooldownMs: 30 * 60_000 },
  );
  return { trigger, sockets, usage, watched };
}

describe('LogTrigger', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('is off for a chain without a websocket', () => {
    const { trigger, sockets } = setup({ websocket: false });
    expect(trigger.mode(CHAIN)).toBe('off');
    expect(sockets).toHaveLength(0);
  });

  it('connects on the RPC URL with the scheme swapped; down until subscribed, then live with a catch-up', () => {
    const { trigger, sockets } = setup();
    expect(trigger.mode(CHAIN)).toBe('down');
    expect(sockets[0]!.url).toBe('wss://rpc.example/v2/key');
    sockets[0]!.subscribe();
    expect(trigger.mode(CHAIN)).toBe('live');
    expect(trigger.takeCatchUp(CHAIN)).toBe(true);
    expect(trigger.takeCatchUp(CHAIN)).toBe(false);
    trigger.stopAll();
  });

  it('ignores look-alike events from contracts the plugin does not trust', () => {
    const { trigger, sockets, usage } = setup();
    trigger.mode(CHAIN);
    sockets[0]!.subscribe();
    sockets[0]!.notify(pingLog(STRANGER, 50));
    sockets[0]!.notify(createdLog(STRANGER, 50));
    expect(trigger.pending(CHAIN)).toBeNull();
    expect(usage.take().get(CHAIN)?.get('ws_ignored')).toBe(2);
    trigger.stopAll();
  });

  it("trusts a brand-new child the factory announced before ingestion has learned it", () => {
    const { trigger, sockets } = setup();
    trigger.mode(CHAIN);
    sockets[0]!.subscribe();
    sockets[0]!.notify(pingLog(CHILD, 60));
    expect(trigger.pending(CHAIN)).toBeNull(); // not known yet
    sockets[0]!.notify(createdLog(FACTORY, 61));
    sockets[0]!.notify(pingLog(CHILD, 62));
    expect(trigger.pending(CHAIN)?.max).toBe(62n);
    expect(trigger.notices(CHAIN)).toBe(2);
    trigger.stopAll();
  });

  it('clears pending only once ingestion has passed the highest announced block', () => {
    const { trigger, sockets, watched } = setup();
    watched.add(CHAIN, CHILD);
    trigger.mode(CHAIN);
    sockets[0]!.subscribe();
    sockets[0]!.notify(pingLog(CHILD, 100));
    sockets[0]!.notify(pingLog(CHILD, 105));
    trigger.ingested(CHAIN, 104n);
    expect(trigger.pending(CHAIN)?.max).toBe(105n);
    trigger.ingested(CHAIN, 105n);
    expect(trigger.pending(CHAIN)).toBeNull();
    trigger.stopAll();
  });

  it('drops a flooded socket and polls, then tries the socket again after the cooldown', async () => {
    const { trigger, sockets, watched } = setup({ max: 5 });
    watched.add(CHAIN, CHILD);
    trigger.mode(CHAIN);
    sockets[0]!.subscribe();
    for (let i = 0; i < 6; i++) sockets[0]!.notify(pingLog(CHILD, 200 + i));
    await Promise.resolve(); // the stop is queued as a microtask
    expect(sockets[0]!.closed).toBe(true);
    expect(trigger.mode(CHAIN)).toBe('off');

    vi.advanceTimersByTime(30 * 60_000);
    expect(trigger.mode(CHAIN)).toBe('down');
    expect(sockets).toHaveLength(2);
    trigger.stopAll();
  });
});

describe('socketUrl', () => {
  it('swaps the scheme for http(s) and refuses anything else', () => {
    expect(socketUrl('https://a.example/v2/k')).toBe('wss://a.example/v2/k');
    expect(socketUrl('http://127.0.0.1:8545')).toBe('ws://127.0.0.1:8545');
    expect(socketUrl('ipc:///tmp/geth.ipc')).toBeNull();
    expect(socketUrl(undefined)).toBeNull();
  });
});
