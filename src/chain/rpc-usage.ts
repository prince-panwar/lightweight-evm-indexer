/**
 * Billable JSON-RPC calls per chain and method, counted at the transport.
 *
 * Per CALL, not per HTTP request: a batch is one round trip, but providers
 * bill every call inside it, and that is the number any change to polling
 * has to be judged by.
 */
export class RpcUsage {
  private readonly counts = new Map<number, Map<string, number>>();

  record(chainId: number, method: string, n = 1): void {
    let byMethod = this.counts.get(chainId);
    if (!byMethod) this.counts.set(chainId, (byMethod = new Map()));
    byMethod.set(method, (byMethod.get(method) ?? 0) + n);
  }

  /** Count every call in a JSON-RPC request body (single or batch). Never throws. */
  recordBody(chainId: number, body: unknown): void {
    let methods: string[] = ['unknown'];
    try {
      const parsed = JSON.parse(String(body)) as { method?: string } | { method?: string }[];
      methods = (Array.isArray(parsed) ? parsed : [parsed]).map((c) => c.method ?? 'unknown');
    } catch {
      // Accounting must never break a read.
    }
    for (const m of methods) this.record(chainId, m);
  }

  /** Counts since the last call, per chain and method; resets them. */
  take(): Map<number, Map<string, number>> {
    const out = new Map(this.counts);
    this.counts.clear();
    return out;
  }
}
