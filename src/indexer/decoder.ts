import { parseEventLogs, toEventSelector, type AbiEvent, type Address, type Hex, type Log } from 'viem';
import type { DecodedEvent, UntimedEvent } from '../types.js';

/** Stored args are strings: bigints and nested values survive JSON without loss. */
function normalize(value: unknown): string {
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'string') return value.startsWith('0x') && value.length === 42 ? value.toLowerCase() : value;
  if (typeof value === 'boolean' || typeof value === 'number') return String(value);
  return JSON.stringify(value, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
}

const byBlockThenIndex = (a: UntimedEvent, b: UntimedEvent) =>
  a.blockNumber === b.blockNumber ? a.logIndex - b.logIndex : a.blockNumber < b.blockNumber ? -1 : 1;

/** Canonical order: (blockNumber, logIndex). Two events in one block have a definite order only logIndex records. */
export function inOrder<T extends UntimedEvent>(events: T[]): T[] {
  return [...events].sort(byBlockThenIndex);
}

export class Decoder {
  constructor(private readonly events: AbiEvent[]) {}

  /** topic0 selectors for the named events (all events when `names` is omitted). */
  selectors(names?: ReadonlySet<string>): Hex[] {
    return this.events.filter((e) => !names || names.has(e.name)).map((e) => toEventSelector(e));
  }

  /**
   * Decode WITHOUT block times, in canonical order, so a caller can decide
   * which events it keeps before paying for their timestamps (a block read
   * each). `strict: false` drops logs matching no known event instead of
   * throwing.
   */
  decodeUntimed(chainId: number, logs: Log[]): UntimedEvent[] {
    const parsed = parseEventLogs({ abi: this.events, logs, strict: false });
    return inOrder(
      parsed.map((p) => {
        const args: Record<string, string> = {};
        for (const [key, value] of Object.entries((p.args ?? {}) as Record<string, unknown>)) {
          args[key] = normalize(value);
        }
        return {
          chainId,
          blockNumber: p.blockNumber as bigint,
          blockHash: p.blockHash as Hex,
          logIndex: p.logIndex as number,
          txHash: p.transactionHash as Hex,
          txIndex: p.transactionIndex as number,
          address: (p.address as string).toLowerCase() as Address,
          eventName: p.eventName as string,
          args,
        };
      }),
    );
  }

  /** Attach each event's block time; order is preserved. A missing time is an error, never a 1970 default. */
  withTimes(chainId: number, events: UntimedEvent[], times: Map<bigint, Date>): DecodedEvent[] {
    return events.map((e) => {
      const blockTime = times.get(e.blockNumber);
      if (!blockTime) throw new Error(`no block time for block ${e.blockNumber} on chain ${chainId}`);
      return { ...e, blockTime };
    });
  }
}
