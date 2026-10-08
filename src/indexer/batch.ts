import type { DecodedEvent } from '../types.js';
import type { LogRange } from './range-sizer.js';

export interface EventBatch {
  events: DecodedEvent[];
  toBlock: bigint;
}

/**
 * Split a range's events into commit-sized batches that each end ON A BLOCK
 * BOUNDARY.
 *
 * The cursor means "every event up to and including block N is applied", so
 * a batch that stopped mid-block would commit a cursor claiming more than it
 * wrote, and the rest of that block would never be ingested. A single block
 * holding more than `limit` events is therefore emitted whole and over the
 * limit: indivisible, and a slightly large batch still commits where a split
 * one would lose data.
 */
export function batchByBlock(events: DecodedEvent[], range: LogRange, limit: number): EventBatch[] {
  if (events.length <= limit) return [{ events, toBlock: range.toBlock }];

  const batches: EventBatch[] = [];
  let current: DecodedEvent[] = [];
  for (let i = 0; i < events.length; i++) {
    current.push(events[i]!);
    const last = i === events.length - 1;
    const blockEnds = last || events[i + 1]!.blockNumber !== events[i]!.blockNumber;
    if (!blockEnds) continue;
    if (current.length >= limit || last) {
      // The final batch carries the range to its declared end, so blocks after
      // the last event are still marked done.
      batches.push({ events: current, toBlock: last ? range.toBlock : events[i]!.blockNumber });
      current = [];
    }
  }
  return batches;
}
