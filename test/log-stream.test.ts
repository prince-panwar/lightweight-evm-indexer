import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  HEARTBEAT_MS,
  HEARTBEAT_TIMEOUT_MS,
  LogStream,
  RECONNECT_BASE_MS,
  SUBSCRIBE_TIMEOUT_MS,
} from '../src/trigger/log-stream.js';
import { FakeSocket } from './fake-socket.js';

function setup(addresses?: `0x${string}`[]) {
  const sockets: FakeSocket[] = [];
  const blocks: bigint[] = [];
  const states: boolean[] = [];
  const usage: string[] = [];
  const stream = new LogStream({
    chainId: 1,
    url: 'wss://example/v2/key',
    topics: ['0xaa', '0xbb'],
    addresses,
    onLog: (l) => blocks.push(BigInt(l.blockNumber)),
    onState: (live) => states.push(live),
    onUsage: (k) => usage.push(k),
    log: { warn: () => {} },
    socketFactory: (url) => {
      const s = new FakeSocket(url);
      sockets.push(s);
      return s;
    },
  });
  return { stream, sockets, blocks, states, usage };
}

describe('LogStream', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('subscribes to the topics on open, and is live only once confirmed', () => {
    const { stream, sockets, states } = setup();
    stream.start();
    sockets[0]!.open();
    expect(sockets[0]!.sent[0]).toMatchObject({ method: 'eth_subscribe', params: ['logs', { topics: [['0xaa', '0xbb']] }] });
    expect(stream.isLive()).toBe(false);
    sockets[0]!.reply({ jsonrpc: '2.0', id: 1, result: '0xsub' });
    expect(stream.isLive()).toBe(true);
    expect(states).toEqual([true]);
    stream.stop();
  });

  it('adds an address filter when given one', () => {
    const { stream, sockets } = setup(['0x0000000000000000000000000000000000000001']);
    stream.start();
    sockets[0]!.open();
    expect(sockets[0]!.sent[0]!.params).toEqual([
      'logs',
      { address: ['0x0000000000000000000000000000000000000001'], topics: [['0xaa', '0xbb']] },
    ]);
    stream.stop();
  });

  it('reports each notified log', () => {
    const { stream, sockets, blocks } = setup();
    stream.start();
    sockets[0]!.subscribe();
    sockets[0]!.notify({ address: '0x01', blockNumber: '0x10', topics: [], data: '0x' });
    expect(blocks).toEqual([16n]);
    stream.stop();
  });

  it('ignores malformed messages instead of throwing out of the socket handler', () => {
    const { stream, sockets, blocks } = setup();
    stream.start();
    sockets[0]!.subscribe();
    expect(() => sockets[0]!.onmessage?.({ data: 'not json' })).not.toThrow();
    expect(() => sockets[0]!.notify({ blockNumber: 'zz' })).not.toThrow();
    expect(blocks).toEqual([]);
    expect(stream.isLive()).toBe(true);
    stream.stop();
  });

  it('reconnects with backoff after a drop, and re-subscribes', () => {
    const { stream, sockets, states } = setup();
    stream.start();
    sockets[0]!.subscribe();
    sockets[0]!.drop();
    expect(states).toEqual([true, false]);
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(2);
    sockets[1]!.subscribe();
    expect(states).toEqual([true, false, true]);
    stream.stop();
  });

  it('keeps backing off an endpoint that accepts and drops at once', () => {
    const { stream, sockets } = setup();
    stream.start();
    sockets[0]!.subscribe();
    sockets[0]!.drop(); // 1s
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    sockets[1]!.subscribe();
    sockets[1]!.drop(); // subscribing alone does not reset the backoff: 2s now
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(2);
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(3);
    stream.stop();
  });

  it('abandons and retries a connection that never subscribes', () => {
    const { stream, sockets } = setup();
    stream.start();
    sockets[0]!.open();
    vi.advanceTimersByTime(SUBSCRIBE_TIMEOUT_MS);
    expect(sockets[0]!.closed).toBe(true);
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(2);
    stream.stop();
  });

  it('treats a refused subscription as down and retries', () => {
    const { stream, sockets } = setup();
    stream.start();
    sockets[0]!.open();
    sockets[0]!.reply({ jsonrpc: '2.0', id: 1, error: { message: 'method not supported' } });
    expect(stream.isLive()).toBe(false);
    vi.advanceTimersByTime(RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(2);
    stream.stop();
  });

  it('reconnects when a heartbeat goes unanswered (a silently dead socket)', () => {
    const { stream, sockets, usage } = setup();
    stream.start();
    sockets[0]!.subscribe();
    vi.advanceTimersByTime(HEARTBEAT_MS);
    expect(sockets[0]!.sent.at(-1)!.method).toBe('eth_chainId');
    expect(usage).toContain('eth_chainId');
    vi.advanceTimersByTime(HEARTBEAT_TIMEOUT_MS);
    expect(stream.isLive()).toBe(false);
    expect(sockets[0]!.closed).toBe(true);
    stream.stop();
  });

  it('stays up while heartbeats are answered', () => {
    const { stream, sockets } = setup();
    stream.start();
    sockets[0]!.subscribe();
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(HEARTBEAT_MS);
      const beat = sockets[0]!.sent.at(-1)!;
      sockets[0]!.reply({ jsonrpc: '2.0', id: beat.id, result: '0x1' });
    }
    vi.advanceTimersByTime(HEARTBEAT_TIMEOUT_MS);
    expect(stream.isLive()).toBe(true);
    expect(sockets).toHaveLength(1);
    stream.stop();
  });

  it('does not reconnect after stop()', () => {
    const { stream, sockets } = setup();
    stream.start();
    sockets[0]!.subscribe();
    stream.stop();
    vi.advanceTimersByTime(10 * RECONNECT_BASE_MS);
    expect(sockets).toHaveLength(1);
  });
});
