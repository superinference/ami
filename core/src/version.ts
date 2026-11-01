/**
 * Single source of truth for the AMI product version and client identity.
 *
 * MUST equal every package.json version in the monorepo — enforced by
 * test/version-sync.test.ts, which fails CI if this string drifts from the
 * canonical manifest. Import AMI_VERSION / AMI_CLIENT_INFO everywhere instead of
 * hardcoding a version string (the old `'1.0.0'` MCP clientInfo bug) or
 * re-reading package.json from library code, so a release bump only touches the
 * manifests + this constant and nothing silently falls out of sync.
 */
export const AMI_VERSION = '0.7.4';

/** The client name AMI advertises to MCP servers and HTTP peers. */
export const AMI_CLIENT_NAME = 'ami';

/** clientInfo sent in the MCP `initialize` handshake (name + real version). */
export const AMI_CLIENT_INFO = { name: AMI_CLIENT_NAME, version: AMI_VERSION } as const;
