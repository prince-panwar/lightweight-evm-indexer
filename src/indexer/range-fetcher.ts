import type { Hex, Log } from 'viem';
import type { ChainClient } from '../chain/chain-client.js';
import type { DecodedEvent, IndexerPlugin, UntimedEvent } from '../types.js';
import { Decoder, inOrder } from './decoder.js';
import type { WatchSet } from './watch-set.js';

/** Providers vary in what they accept; 500 addresses per call is a safe cap. */
export const ADDRESS_CHUNK = 500;

/**
 * Fetch one range in TWO PHASES.
 *
 * Phase 1 names event SIGNATURES and no addresses, so one call returns every
 * protocol event in the range however many contracts emit them, including
 * a contract created in that very range. Signatures are not ours to reserve,
 * so every decoded log goes through the plugin's `isTrusted` (a probe of a
 * real chain found foreign contracts emitting the same signatures).
 *
 * Phase 2 is for signatures every contract emits (ERC-20 `Transfer`): a
 * topic-only query returns the whole chain's traffic, so those are fetched
 * by address from the watched set, after phase 1 has had the chance to
 * discover new addresses.
 *
 * Trusted BEFORE timed: a block time is an RPC read, and a look-alike log is
 * thrown away anyway. One timestamp read per distinct block, kept events only.
 */
export class RangeFetcher {
  private readonly topicSelectors: Hex[];
  private readonly scopedSelectors: Hex[];

  constructor(
    private readonly chain: ChainClient,
    private readonly decoder: Decoder,
    private readonly watched: WatchSet,
    private readonly plugin: IndexerPlugin,
  ) {
    const scoped = new Set(plugin.addressScopedEvents ?? []);
    const all = new Set(plugin.events.map((e) => e.name));
    this.scopedSelectors = decoder.selectors(scoped);
    this.topicSelectors = decoder.selectors(new Set([...all].filter((n) => !scoped.has(n))));
  }

  /**
   * What a websocket subscription listens for, and which event names it can
   * therefore announce: every event when the plugin gives a fixed address
   * list to filter by, otherwise the phase-1 (topic-filtered) events only.
   */
  subscription(): { topics: Hex[]; eventNames: Set<string> } {
    const scoped = new Set(this.plugin.addressScopedEvents ?? []);
    const names = this.plugin.events.map((e) => e.name);
    if (this.plugin.subscriptionAddresses) {
      return { topics: [...this.topicSelectors, ...this.scopedSelectors], eventNames: new Set(names) };
    }
    return { topics: this.topicSelectors, eventNames: new Set(names.filter((n) => !scoped.has(n))) };
  }

  async fetchRange(chainId: number, fromBlock: bigint, toBlock: bigint): Promise<DecodedEvent[]> {
    const kept: UntimedEvent[] = [];

    // ---- phase 1: protocol signatures, from anyone, trusted in order
    if (this.topicSelectors.length > 0) {
      const logs = await this.chain.getLogsAdaptive(chainId, {
        topics: [this.topicSelectors],
        fromBlock,
        toBlock,
      });
      kept.push(...this.trust(chainId, this.decoder.decodeUntimed(chainId, logs)));
    }

    // ---- phase 2: common signatures, by address (addresses found above included)
    if (this.scopedSelectors.length > 0) {
      const addresses = this.watched.list(chainId);
      const logs: Log[] = [];
      for (let i = 0; i < addresses.length; i += ADDRESS_CHUNK) {
        logs.push(
          ...(await this.chain.getLogsAdaptive(chainId, {
            address: addresses.slice(i, i + ADDRESS_CHUNK),
            topics: [this.scopedSelectors],
            fromBlock,
            toBlock,
          })),
        );
      }
      kept.push(...this.trust(chainId, this.decoder.decodeUntimed(chainId, logs)));
    }

    if (kept.length === 0) return [];
    const ordered = inOrder(kept);
    const times = await this.chain.blockTimes(
      chainId,
      ordered.map((e) => e.blockNumber),
    );
    return this.decoder.withTimes(chainId, ordered, times);
  }

  /** Keep trusted events; let each one introduce addresses before the next is judged. */
  private trust(chainId: number, events: UntimedEvent[]): UntimedEvent[] {
    const out: UntimedEvent[] = [];
    for (const event of events) {
      const ctx = { chainId, watched: this.watched.set(chainId) };
      if (!this.plugin.isTrusted(event, ctx)) continue;
      out.push(event);
      for (const address of this.plugin.discover?.(event, ctx) ?? []) this.watched.add(chainId, address);
    }
    return out;
  }
}
