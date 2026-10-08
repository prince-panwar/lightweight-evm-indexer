/**
 * Provider phrasings that mean "you are sending requests too fast".
 *
 * Checked BEFORE the range-size list, and that order matters: "rate limit
 * exceeded" contains phrases a size check would also match, and answering a
 * throttle by bisecting doubles the request count against an endpoint that
 * just said it was over budget. Narrowing is the wrong response to this
 * class; waiting is the right one.
 */
export const RATE_LIMITED =
  /rate limit|ratelimit|too many requests|429|request limit|capacity exceeded|throttl|quota|compute units|cu per second/i;

/**
 * Provider phrasings that all mean "narrow the range and try again".
 *
 * A phrase list rather than an error-code table: every provider words this
 * differently and none use a standard code. A missing phrase degrades to a
 * visible hard failure; a phrase that is too broad would bisect on an auth
 * error, which is not visible.
 */
export const RANGE_TOO_LARGE =
  /too many results|more than \d+ results|response size|response body exceeded|query timeout|block range|range is too|log limit|result set|exceed maximum|limited to|too large|too wide|ranges over|not supported|max(?:imum)? (?:block|range)/i;

/** Transport-level faults worth one more attempt rather than a whole poll cycle. */
export const TRANSIENT =
  /timeout|timed out|econnreset|econnrefused|etimedout|epipe|socket hang up|network|fetch failed|502|503|504|bad gateway|service unavailable|gateway timeout/i;

/** viem wraps RPC errors several layers deep; match against the whole chain. */
export function flattenError(err: unknown): string {
  const parts: string[] = [];
  let current: unknown = err;

  for (let depth = 0; current instanceof Error && depth < 8; depth++) {
    parts.push(current.message);
    const details = (current as { details?: unknown }).details;
    if (typeof details === 'string') parts.push(details);
    const shortMessage = (current as { shortMessage?: unknown }).shortMessage;
    if (typeof shortMessage === 'string') parts.push(shortMessage);
    current = (current as { cause?: unknown }).cause;
  }

  if (parts.length === 0) parts.push(String(err));
  return parts.join(' | ');
}

/**
 * The block window a provider states in its rejection, e.g. "You can make
 * eth_getLogs requests with up to a 2K block range" or "up to a 500 block
 * range". Null when the message names none (or names something absurd).
 */
export function statedLogSpan(message: string): bigint | null {
  const m = /up to (?:an? )?(\d[\d,]*(?:\.\d+)?)\s*(k)?\s*block range/i.exec(message);
  if (!m) return null;
  const n = Math.floor(Number(m[1]!.replace(/,/g, '')) * (m[2] ? 1000 : 1));
  return Number.isFinite(n) && n >= 1 ? BigInt(n) : null;
}
