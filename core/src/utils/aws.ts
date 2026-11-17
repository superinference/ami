/**
 * AWS short-term (STS) credential validation — the boundary typeguard AMI uses to
 * REJECT a malformed / empty AWS session-credential set before an amazon-bedrock
 * client is constructed from it.
 *
 * When Bedrock is driven by temporary credentials produced by `aws sts
 * assume-role` / `aws sts get-session-token` (the standard short-term / SSO /
 * assume-role path), those credentials are exported into the process environment
 * as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`. The
 * values are EXTERNALLY SOURCED — a CLI JSON blob, an SSO broker, a CI secret
 * store — and can be partially populated, truncated, or empty (e.g. a broken
 * `aws sts` wrapper that emits `{"Credentials":{"AccessKeyId":"", ...}}`). Feeding
 * such a blob straight into the AWS/Bedrock client surfaces later as an opaque 403
 * with no hint that the credential material itself was malformed.
 *
 * {@link isValidAwsStsOutput} is a faithful port of Claude Code's typeguard
 * (utils/aws.ts): it asserts the STS output is an object carrying a `Credentials`
 * object whose `AccessKeyId` / `SecretAccessKey` / `SessionToken` are present,
 * string-typed, and non-empty. {@link assertValidStsEnvCredentials} reconstructs
 * that STS-output shape from the standard env vars and fails fast — with an
 * actionable message naming the offending fields — when the shape is invalid, so
 * the failure is attributed at the credential boundary rather than deep in the
 * provider.
 *
 * This is a boundary GUARD, distinct from (and complementary to) the purely
 * informational `awsShortTermCredentialHint` in common/src/provider-config.ts,
 * which only reports presence; this module validates the credential SHAPE.
 */

import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join as pathJoin } from 'node:path';

/** AWS short-term credentials, as returned by STS under the `Credentials` key. */
export interface AwsCredentials {
  AccessKeyId: string;
  SecretAccessKey: string;
  SessionToken: string;
  /** ISO-8601 expiry; informational only (not validated). */
  Expiration?: string;
}

/** Output shape of `aws sts assume-role` / `aws sts get-session-token`. */
export interface AwsStsOutput {
  Credentials: AwsCredentials;
}

/**
 * Typeguard validating that `value` is a well-formed AWS STS output: an object
 * with a `Credentials` object whose `AccessKeyId`, `SecretAccessKey`, and
 * `SessionToken` are all present, string-typed, and non-empty. Mirrors Claude
 * Code's `isValidAwsStsOutput` (utils/aws.ts) — including the explicit
 * empty-string rejection (`.length > 0`) — so malformed/partial credential blobs
 * are caught before the secret material is trusted.
 */
export function isValidAwsStsOutput(value: unknown): value is AwsStsOutput {
  if (!value || typeof value !== 'object') return false;
  const output = value as Record<string, unknown>;
  const credentials = output.Credentials;
  if (!credentials || typeof credentials !== 'object') return false;
  const c = credentials as Record<string, unknown>;
  return (
    typeof c.AccessKeyId === 'string' &&
    typeof c.SecretAccessKey === 'string' &&
    typeof c.SessionToken === 'string' &&
    c.AccessKeyId.length > 0 &&
    c.SecretAccessKey.length > 0 &&
    c.SessionToken.length > 0
  );
}

/**
 * Reconstruct an {@link AwsStsOutput} candidate from the standard AWS env vars,
 * or `null` when `AWS_SESSION_TOKEN` is absent — i.e. the setup is NOT using
 * short-term (STS) credentials, so there is no STS blob to validate (long-term
 * `AKIA…` keys and bearer-token auth take other paths). A present session token
 * is the authoritative signal that temporary STS credentials are in play.
 */
export function readStsEnvCredentials(
  env: NodeJS.ProcessEnv = process.env,
): AwsStsOutput | null {
  const SessionToken = (env.AWS_SESSION_TOKEN ?? '').trim();
  if (!SessionToken) return null;
  return {
    Credentials: {
      AccessKeyId: (env.AWS_ACCESS_KEY_ID ?? '').trim(),
      SecretAccessKey: (env.AWS_SECRET_ACCESS_KEY ?? '').trim(),
      SessionToken,
    },
  };
}

// ---------------------------------------------------------------------------
// AWS shared-credentials (~/.aws/credentials) INI source + cache invalidation.
//
// Claude Code reads long-term/SSO credentials through the AWS SDK credential
// provider chain and exposes `clearAwsIniCache()` — a wrapper over
// `fromIni({ ignoreCache: true })` — so a user who rotates or re-authenticates
// mid-session has the edited `~/.aws/credentials` picked up WITHOUT a restart
// (utils/aws.ts:L57-74). AMI previously resolved Bedrock auth from a STATIC env
// `AWS_ACCESS_KEY_ID` only, so an ini-only setup never authenticated and a
// credential edit could never be re-read.
//
// This adds a dependency-free INI reader for the standard credentials file with
// the same cache-invalidation knob: a parsed profile table is memoized per file
// path (reads are cheap on the hot path) and {@link clearAwsIniCache} / the
// `ignoreCache` option force a fresh read so rotated credentials are honored.
// ---------------------------------------------------------------------------

/** Long-term/SSO credential fields read from a `~/.aws/credentials` profile. */
export interface AwsIniCredentials {
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
}

/**
 * Env knob mirroring AWS SDK `fromIni({ ignoreCache: true })`: when truthy, every
 * {@link readAwsIniCredentials} call bypasses the in-process cache and re-reads
 * the credentials file, so edits are always picked up (at the cost of a disk read
 * per resolution). Default OFF — the cache is used and {@link clearAwsIniCache}
 * is the explicit invalidation hook.
 */
export const AWS_INI_IGNORE_CACHE_ENV = 'AMI_AWS_INI_IGNORE_CACHE';

/**
 * Parsed-profile cache keyed by resolved credentials-file path. `null` means the
 * cache is empty/invalidated. Mirrors the AWS SDK's global ini cache that
 * `clearAwsIniCache` clears.
 */
let awsIniCache: Map<string, Record<string, AwsIniCredentials>> | null = null;

/**
 * Invalidate the shared-credentials INI cache so the next
 * {@link readAwsIniCredentials} re-reads `~/.aws/credentials` from disk. Mirrors
 * Claude Code's `clearAwsIniCache()` (utils/aws.ts) — call it after the user
 * re-authenticates or rotates credentials mid-session.
 */
export function clearAwsIniCache(): void {
  awsIniCache = null;
}

/**
 * Resolve the path to the AWS shared-credentials file: `AWS_SHARED_CREDENTIALS_FILE`
 * when set (the SDK override), otherwise `~/.aws/credentials`.
 */
export function resolveAwsCredentialsFilePath(
  env: NodeJS.ProcessEnv = process.env,
): string {
  const override = (env.AWS_SHARED_CREDENTIALS_FILE ?? '').trim();
  return override || pathJoin(homedir(), '.aws', 'credentials');
}

/**
 * Minimal, dependency-free parser for the AWS credentials INI format. Handles
 * `[profile]` and `[profile name]` section headers, `key = value` pairs, and
 * `#`/`;` line comments; only the three credential keys are retained. Unknown
 * keys and malformed lines are ignored (lenient, like the SDK's ini reader).
 */
function parseAwsIniCredentials(text: string): Record<string, AwsIniCredentials> {
  const out: Record<string, AwsIniCredentials> = {};
  let current: string | null = null;
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.replace(/[;#].*$/, '').trim();
    if (!line) continue;
    const section = /^\[(.+)\]$/.exec(line);
    if (section) {
      // `[profile dev]` (config-file style) and `[dev]` both name profile `dev`.
      current = section[1].trim().replace(/^profile\s+/, '');
      if (!out[current]) out[current] = {};
      continue;
    }
    if (!current) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim().toLowerCase();
    const value = line.slice(eq + 1).trim();
    if (key === 'aws_access_key_id') out[current].accessKeyId = value;
    else if (key === 'aws_secret_access_key') out[current].secretAccessKey = value;
    else if (key === 'aws_session_token') out[current].sessionToken = value;
  }
  return out;
}

/**
 * Read a profile's credentials from the AWS shared-credentials file, memoized per
 * file path. The profile is `opts.profile`, else `AWS_PROFILE`, else `default`.
 *
 * `opts.ignoreCache` (or the {@link AWS_INI_IGNORE_CACHE_ENV} env knob) forces a
 * fresh disk read, mirroring `fromIni({ ignoreCache: true })`. A missing or
 * unreadable file yields an empty object (`{}`) rather than throwing, so callers
 * can treat "no ini credentials" and "no such profile" uniformly. The empty
 * object is cached too, so a missing file is not re-stat'd on every request.
 */
export function readAwsIniCredentials(
  opts: { profile?: string; ignoreCache?: boolean; env?: NodeJS.ProcessEnv } = {},
): AwsIniCredentials {
  const env = opts.env ?? process.env;
  const ignoreCache = opts.ignoreCache ?? isAwsIniIgnoreCache(env);
  const profile = (opts.profile || env.AWS_PROFILE || 'default').trim();
  const filePath = resolveAwsCredentialsFilePath(env);
  if (!awsIniCache) awsIniCache = new Map();
  if (ignoreCache) awsIniCache.delete(filePath);
  let parsed = awsIniCache.get(filePath);
  if (!parsed) {
    try {
      parsed = parseAwsIniCredentials(readFileSync(filePath, 'utf8'));
    } catch {
      parsed = {};
    }
    awsIniCache.set(filePath, parsed);
  }
  return parsed[profile] ?? {};
}

/** True when {@link AWS_INI_IGNORE_CACHE_ENV} is set to a truthy value. */
function isAwsIniIgnoreCache(env: NodeJS.ProcessEnv): boolean {
  const v = (env[AWS_INI_IGNORE_CACHE_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/** Raised when env-sourced short-term AWS credentials are structurally invalid. */
export class AwsStsCredentialError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AwsStsCredentialError';
  }
}

/**
 * Fail fast when the environment carries short-term AWS credentials
 * (`AWS_SESSION_TOKEN` present) whose shape is invalid — a missing or empty
 * `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. No-op when no session token is
 * set (not an STS setup) or when the credential triple is well-formed. The thrown
 * message names exactly which fields are empty so the operator can fix the
 * credential source instead of debugging a downstream 403.
 */
export function assertValidStsEnvCredentials(
  env: NodeJS.ProcessEnv = process.env,
): void {
  const candidate = readStsEnvCredentials(env);
  if (candidate === null) return;
  const creds = candidate.Credentials;
  if (isValidAwsStsOutput(candidate)) return;
  const missing: string[] = [];
  if (!creds.AccessKeyId) missing.push('AWS_ACCESS_KEY_ID');
  if (!creds.SecretAccessKey) missing.push('AWS_SECRET_ACCESS_KEY');
  throw new AwsStsCredentialError(
    `AWS_SESSION_TOKEN is set (short-term STS credentials) but ${missing.join(' and ')} ` +
      `${missing.length > 1 ? 'are' : 'is'} empty or missing. ` +
      'Re-export the full credential triple from your STS/SSO source before using amazon-bedrock.',
  );
}
