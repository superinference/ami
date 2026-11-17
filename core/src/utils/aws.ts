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
