import type { Address } from 'viem';
import type { Pool } from '../db/database.js';
import type { IndexerPlugin } from '../types.js';

const EMPTY: ReadonlySet<string> = new Set();

/**
 * Per chain, the addresses the plugin trusts beyond its static ones: known at
 * start (`loadWatched`) or discovered from trusted events (`discover`).
 * Lowercase throughout: mixed-case keys are silent cache misses.
 */
export class WatchSet {
  private readonly sets = new Map<number, Set<string>>();

  constructor(
    private readonly pool: Pool,
    private readonly plugin: IndexerPlugin,
  ) {}

  /** (Re)load from the plugin's own view of the world, replacing what is held. */
  async load(chainId: number): Promise<void> {
    const client = await this.pool.connect();
    try {
      const addresses = await this.plugin.loadWatched(chainId, client);
      this.sets.set(chainId, new Set(addresses.map((a) => a.toLowerCase())));
    } finally {
      client.release();
    }
  }

  add(chainId: number, address: Address): void {
    let set = this.sets.get(chainId);
    if (!set) this.sets.set(chainId, (set = new Set()));
    set.add(address.toLowerCase());
  }

  set(chainId: number): ReadonlySet<string> {
    return this.sets.get(chainId) ?? EMPTY;
  }

  list(chainId: number): Address[] {
    return [...this.set(chainId)] as Address[];
  }
}
