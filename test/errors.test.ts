import { describe, expect, it } from 'vitest';
import { flattenError, RANGE_TOO_LARGE, RATE_LIMITED, statedLogSpan } from '../src/chain/errors.js';
import { RpcUsage } from '../src/chain/rpc-usage.js';

describe('statedLogSpan', () => {
  it('reads the window a provider states in its rejection', () => {
    expect(statedLogSpan('Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range')).toBe(2000n);
    expect(statedLogSpan('You can make eth_getLogs requests with up to a 500 block range')).toBe(500n);
    expect(statedLogSpan('requests with up to 10,000 block range')).toBe(10_000n);
    expect(statedLogSpan('up to an 800 block range')).toBe(800n);
  });

  it('returns null when no usable window is stated', () => {
    expect(statedLogSpan('query returned more than 10000 results')).toBeNull();
    expect(statedLogSpan('up to a 0 block range')).toBeNull();
    expect(statedLogSpan('block range extends beyond current head')).toBeNull();
    expect(statedLogSpan(`up to a ${'9'.repeat(400)} block range`)).toBeNull();
  });
});

describe('error classification', () => {
  it('never reads a throttle as an oversized range', () => {
    const throttle = 'rate limit exceeded, please retry';
    expect(RATE_LIMITED.test(throttle)).toBe(true);
  });

  it('recognises common provider phrasings of an oversized range', () => {
    for (const m of [
      'query returned more than 10000 results',
      'Log response size exceeded',
      'block range is too wide',
      'HTTP response body exceeded the size limit',
    ]) {
      expect(RANGE_TOO_LARGE.test(m), m).toBe(true);
    }
  });

  it('flattens nested viem errors, details and causes included', () => {
    const inner = Object.assign(new Error('inner'), { details: 'query returned more than 10000 results' });
    const outer = Object.assign(new Error('An unknown RPC error occurred.'), { cause: inner });
    expect(RANGE_TOO_LARGE.test(flattenError(outer))).toBe(true);
  });
});

describe('RpcUsage', () => {
  it('counts every call in a batch, per chain and method, and resets on take', () => {
    const usage = new RpcUsage();
    usage.recordBody(
      8453,
      JSON.stringify([
        { jsonrpc: '2.0', id: 1, method: 'eth_getLogs' },
        { jsonrpc: '2.0', id: 2, method: 'eth_getBlockByNumber' },
      ]),
    );
    usage.recordBody(8453, JSON.stringify({ jsonrpc: '2.0', id: 3, method: 'eth_getLogs' }));
    usage.recordBody(1, 'not json');

    const taken = usage.take();
    expect(Object.fromEntries(taken.get(8453)!)).toEqual({ eth_getLogs: 2, eth_getBlockByNumber: 1 });
    expect(Object.fromEntries(taken.get(1)!)).toEqual({ unknown: 1 });
    expect(usage.take().size).toBe(0);
  });
});
