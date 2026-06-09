/**
 * Shared SSRF protection and HTTP utilities for web-fetch and web-search tools.
 */

import * as http from 'http';
import * as https from 'https';
import * as dns from 'dns';
import { URL } from 'url';

export const BLOCKED_HOSTNAMES = new Set([
  'localhost', '127.0.0.1', '::1', '0.0.0.0',
  'metadata.google.internal', 'metadata.google.com',
  'instance-data', 'metadata',
]);

export function isPrivateIP(ip: string): boolean {
  // Strip IPv4-mapped IPv6 prefix and re-check
  if (ip.startsWith('::ffff:')) {
    return isPrivateIP(ip.slice(7));
  }
  // IPv6 wildcard
  if (ip === '::' || ip === '0.0.0.0') return true;
  // Carrier-grade NAT
  if (/^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./.test(ip)) return true;
  // IPv4 private ranges
  if (/^10\./.test(ip)) return true;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true;
  if (/^192\.168\./.test(ip)) return true;
  // Loopback
  if (/^127\./.test(ip)) return true;
  // Link-local
  if (/^169\.254\./.test(ip)) return true;
  // AWS/GCP/Azure metadata
  if (ip === '169.254.169.254') return true;
  // IPv6 private
  if (ip === '::1' || ip.startsWith('fe80:') || ip.startsWith('fc00:') || ip.startsWith('fd')) return true;
  return false;
}

export function isBlockedRedirectTarget(url: string): boolean {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    if (BLOCKED_HOSTNAMES.has(hostname)) return true;
    // Block obvious private IP patterns in the hostname itself
    if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|127\.|0\.0\.0\.0|169\.254\.)/.test(hostname)) return true;
    if (hostname === '::1' || hostname.startsWith('[::')) return true;
    return false;
  } catch { return true; }
}

export async function validateUrlSafety(url: string): Promise<{ error: string } | { resolvedIP: string }> {
  const parsed = new URL(url);
  const hostname = parsed.hostname.toLowerCase();

  if (BLOCKED_HOSTNAMES.has(hostname)) {
    return { error: `Blocked: "${hostname}" is a private/metadata hostname` };
  }

  // Resolve hostname and check for private IPs — return the resolved IP
  // so callers can pin it to prevent DNS rebinding (TOCTOU)
  try {
    const addresses = await new Promise<dns.LookupAddress[]>((resolve, reject) => {
      dns.lookup(hostname, { all: true }, (err, addrs) => {
        if (err) reject(err);
        else resolve(addrs);
      });
    });
    for (const addr of addresses) {
      if (isPrivateIP(addr.address)) {
        return { error: `Blocked: "${hostname}" resolves to private IP ${addr.address}` };
      }
    }
    return { resolvedIP: addresses[0]?.address || hostname };
  } catch {
    return { error: `Blocked: DNS resolution failed for "${hostname}" — cannot verify safety` };
  }
}

/**
 * Strip HTML tags and return clean text.
 *
 * Removes <script>, <style>, and <head> blocks entirely (including their
 * content), then strips the remaining tags, collapses whitespace runs and
 * trims every line.
 */
export function stripHtml(html: string): string {
  let text = html;

  // Remove script blocks
  text = text.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '');

  // Remove style blocks
  text = text.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '');

  // Remove head block
  text = text.replace(/<head\b[^>]*>[\s\S]*?<\/head>/gi, '');

  // Remove HTML comments
  text = text.replace(/<!--[\s\S]*?-->/g, '');

  // Convert <br>, <p>, <div>, <li>, heading tags to newlines before stripping
  text = text.replace(/<br\s*\/?>/gi, '\n');
  text = text.replace(/<\/(p|div|h[1-6]|li|tr|blockquote)>/gi, '\n');
  text = text.replace(/<(p|div|h[1-6]|li|tr|blockquote)\b[^>]*>/gi, '\n');

  // Strip remaining tags
  text = text.replace(/<[^>]+>/g, '');

  // Decode common HTML entities
  text = text.replace(/&amp;/g, '&');
  text = text.replace(/&lt;/g, '<');
  text = text.replace(/&gt;/g, '>');
  text = text.replace(/&quot;/g, '"');
  text = text.replace(/&#39;/g, "'");
  text = text.replace(/&nbsp;/g, ' ');

  // Collapse runs of whitespace on each line, then trim each line
  text = text
    .split('\n')
    .map((line) => line.replace(/[ \t]+/g, ' ').trim())
    .join('\n');

  // Collapse 3+ consecutive blank lines into 2
  text = text.replace(/\n{3,}/g, '\n\n');

  return text.trim();
}

const USER_AGENT =
  'Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0';

/**
 * Perform an HTTP(S) GET following redirects up to `maxRedirects` hops.
 * Returns the final response body as a string along with status metadata.
 */
export function httpGet(
  url: string,
  signal: AbortSignal,
  options?: {
    maxRedirects?: number;
    timeoutMs?: number;
    includeContentType?: boolean;
    resolvedIP?: string;
  },
): Promise<{ body: string; statusCode: number; contentType: string }> {
  const maxRedirects = options?.maxRedirects ?? 5;
  const timeoutMs = options?.timeoutMs ?? 30000;

  return httpGetInternal(url, signal, maxRedirects, timeoutMs, options?.resolvedIP);
}

function httpGetInternal(
  url: string,
  signal: AbortSignal,
  redirectsLeft: number,
  timeoutMs: number,
  resolvedIP?: string,
): Promise<{ body: string; statusCode: number; contentType: string }> {
  const MAX_RESPONSE_LENGTH = 50000;

  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error('Aborted'));
      return;
    }

    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      reject(new Error(`Invalid URL: ${url}`));
      return;
    }

    const requester = parsed.protocol === 'https:' ? https : http;

    const lookupOverride: http.RequestOptions['lookup'] = resolvedIP
      ? (_hostname, _opts, cb) => { (cb as (err: null, address: string, family: number) => void)(null, resolvedIP, resolvedIP.includes(':') ? 6 : 4); }
      : undefined;

    const req = requester.get(
      url,
      {
        headers: {
          'User-Agent': USER_AGENT,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
          'Accept-Encoding': 'identity',
          'DNT': '1',
          'Connection': 'keep-alive',
          'Upgrade-Insecure-Requests': '1',
        },
        timeout: timeoutMs,
        ...(lookupOverride ? { lookup: lookupOverride } : {}),
      },
      (res) => {
        const statusCode = res.statusCode ?? 0;

        // Handle redirects
        if (
          [301, 302, 303, 307, 308].includes(statusCode) &&
          res.headers.location
        ) {
          if (redirectsLeft <= 0) {
            reject(new Error('Too many redirects'));
            return;
          }
          // Resolve relative redirects against the original URL
          const redirectUrl = new URL(res.headers.location, url).toString();
          if (isBlockedRedirectTarget(redirectUrl)) {
            reject(new Error(`Blocked: redirect to private/internal URL "${redirectUrl}"`));
            return;
          }
          res.resume(); // Drain the response body
          validateUrlSafety(redirectUrl).then((ssrfResult) => {
            if ('error' in ssrfResult) {
              reject(new Error(ssrfResult.error));
              return;
            }
            httpGetInternal(redirectUrl, signal, redirectsLeft - 1, timeoutMs, ssrfResult.resolvedIP).then(resolve, reject);
          }, reject);
          return;
        }

        const chunks: Buffer[] = [];
        let totalBytes = 0;
        res.on('data', (chunk: Buffer) => {
          totalBytes += chunk.length;
          if (totalBytes > MAX_RESPONSE_LENGTH * 2) {
            res.destroy();
            resolve({ body: Buffer.concat(chunks).toString('utf-8'), statusCode, contentType: res.headers['content-type'] ?? '' });
            return;
          }
          chunks.push(chunk);
        });
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf-8');
          const contentType = res.headers['content-type'] ?? '';
          resolve({ body, statusCode, contentType });
        });
        res.on('error', reject);
      },
    );

    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Request timed out after ${timeoutMs}ms`));
    });

    req.on('error', (err) => {
      reject(err);
    });

    // Respect abort signal
    const onAbort = () => {
      req.destroy();
      reject(new Error('Aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });

    // Clean up the abort listener when the request finishes
    req.on('close', () => {
      signal.removeEventListener('abort', onAbort);
    });
  });
}
