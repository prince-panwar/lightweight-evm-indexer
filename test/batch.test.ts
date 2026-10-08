import { describe, expect, it } from 'vitest';
import { batchByBlock } from '../src/indexer/batch.js';
import type { DecodedEvent } from '../src/types.js';

const ev = (block: bigint, logIndex: number): DecodedEvent => ({
  chainId: 1,
  blockNumber: block,
  blockHash: `0x${block.toString(16).padStart(64, '0')}`,
  blockTime: new Date(0),
  logIndex,
  txHash: '0x01',
  txIndex: 0,
  address: '0x0000000000000000000000000000000000000001',
  eventName: 'E',
  args: {},
});

describe('batchByBlock', () => {
  const range = { fromBlock: 10n, toBlock: 20n };

  it('keeps a small range as one batch ending at the range end', () => {
    const batches = batchByBlock([ev(11n, 0), ev(12n, 0)], range, 400);
    expect(batches).toHaveLength(1);
    expect(batches[0]!.toBlock).toBe(20n);
  });

  it('only ever cuts on a block boundary, and the last batch carries the range end', () => {
    const events = [ev(11n, 0), ev(11n, 1), ev(12n, 0), ev(12n, 1), ev(13n, 0)];
    const batches = batchByBlock(events, range, 2);
    expect(batches.map((b) => b.toBlock)).toEqual([11n, 12n, 20n]);
    expect(batches.flatMap((b) => b.events)).toEqual(events);
  });

  it('emits an oversized single block whole rather than splitting it', () => {
    const events = [ev(11n, 0), ev(11n, 1), ev(11n, 2), ev(12n, 0)];
    const batches = batchByBlock(events, range, 2);
    expect(batches[0]!.events).toHaveLength(3);
    expect(batches[0]!.toBlock).toBe(11n);
  });
});
