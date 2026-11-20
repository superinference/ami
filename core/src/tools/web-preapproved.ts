/**
 * Built-in preapproved trusted-domain config knob for web_fetch (parity with
 * the reference's WebFetchTool/preapproved.ts).
 *
 * For legal and security reasons web_fetch normally only fetches domains the
 * caller provided (per-call allowed_domains, or an interactive approval). We make
 * an exception for a curated set of code/documentation hosts: a fetch to one of
 * these trusted doc sites is treated as preapproved, which (a) relaxes the
 * secondary-model copyright/quote guardrails (code and doc excerpts are fine) and
 * (b) skips the lossy small-model summarization so the verbatim docs are returned.
 *
 * SECURITY: this list is ONLY for web_fetch (read-only GET). It is deliberately
 * NOT a general network allow-list — arbitrary access (POST, uploads) to some of
 * these hosts could enable exfiltration, so sandbox/network rules never inherit it.
 */

export const PREAPPROVED_HOSTS: ReadonlySet<string> = new Set([
  // Anthropic / agent tooling
  'platform.claude.com',
  'code.claude.com',
  'modelcontextprotocol.io',
  'github.com/anthropics',
  'agentskills.io',

  // Top programming languages
  'docs.python.org',
  'en.cppreference.com',
  'docs.oracle.com',
  'learn.microsoft.com',
  'developer.mozilla.org',
  'go.dev',
  'pkg.go.dev',
  'www.php.net',
  'docs.swift.org',
  'kotlinlang.org',
  'ruby-doc.org',
  'doc.rust-lang.org',
  'www.typescriptlang.org',

  // Web & JavaScript frameworks/libraries
  'react.dev',
  'angular.io',
  'vuejs.org',
  'nextjs.org',
  'expressjs.com',
  'nodejs.org',
  'bun.sh',
  'jquery.com',
  'getbootstrap.com',
  'tailwindcss.com',
  'd3js.org',
  'threejs.org',
  'redux.js.org',
  'webpack.js.org',
  'jestjs.io',
  'reactrouter.com',

  // Python frameworks & libraries
  'docs.djangoproject.com',
  'flask.palletsprojects.com',
  'fastapi.tiangolo.com',
  'pandas.pydata.org',
  'numpy.org',
  'www.tensorflow.org',
  'pytorch.org',
  'scikit-learn.org',
  'matplotlib.org',
  'requests.readthedocs.io',
  'jupyter.org',

  // PHP frameworks
  'laravel.com',
  'symfony.com',
  'wordpress.org',

  // Java frameworks & libraries
  'docs.spring.io',
  'hibernate.org',
  'tomcat.apache.org',
  'gradle.org',
  'maven.apache.org',

  // .NET & C# frameworks
  'asp.net',
  'dotnet.microsoft.com',
  'nuget.org',
  'blazor.net',

  // Mobile development
  'reactnative.dev',
  'docs.flutter.dev',
  'developer.apple.com',
  'developer.android.com',

  // Data science & machine learning
  'keras.io',
  'spark.apache.org',
  'huggingface.co',
  'www.kaggle.com',

  // Databases
  'www.mongodb.com',
  'redis.io',
  'www.postgresql.org',
  'dev.mysql.com',
  'www.sqlite.org',
  'graphql.org',
  'prisma.io',

  // Cloud & DevOps
  'docs.aws.amazon.com',
  'cloud.google.com',
  'kubernetes.io',
  'www.docker.com',
  'www.terraform.io',
  'www.ansible.com',
  'vercel.com/docs',
  'docs.netlify.com',
  'devcenter.heroku.com',

  // Testing & monitoring
  'cypress.io',
  'selenium.dev',

  // Game development
  'docs.unity.com',
  'docs.unrealengine.com',

  // Other essential tools
  'git-scm.com',
  'nginx.org',
  'httpd.apache.org',
]);

// Split once at module load so lookups are O(1) Set.has() for the common
// hostname-only case, falling back to a small per-host path-prefix list for the
// handful of path-scoped entries (e.g. "github.com/anthropics").
const { HOSTNAME_ONLY, PATH_PREFIXES } = (() => {
  const hosts = new Set<string>();
  const paths = new Map<string, string[]>();
  for (const entry of PREAPPROVED_HOSTS) {
    const slash = entry.indexOf('/');
    if (slash === -1) {
      hosts.add(entry);
    } else {
      const host = entry.slice(0, slash);
      const prefix = entry.slice(slash);
      const existing = paths.get(host);
      if (existing) existing.push(prefix);
      else paths.set(host, [prefix]);
    }
  }
  return { HOSTNAME_ONLY: hosts, PATH_PREFIXES: paths };
})();

/**
 * True iff `hostname` (+ `pathname` for path-scoped entries) is on the curated
 * preapproved code/docs list. Path matching enforces segment boundaries:
 * "/anthropics" must NOT match "/anthropics-evil/malware" — only an exact match
 * or a "/" immediately after the prefix is allowed.
 */
export function isPreapprovedHost(hostname: string, pathname: string): boolean {
  const host = hostname.toLowerCase();
  if (HOSTNAME_ONLY.has(host)) return true;
  const prefixes = PATH_PREFIXES.get(host);
  if (prefixes) {
    for (const p of prefixes) {
      if (pathname === p || pathname.startsWith(p + '/')) return true;
    }
  }
  return false;
}

/**
 * Match a per-call `allowed_domains` / `blocked_domains` entry against a request's
 * host (+ pathname). Two entry forms are supported (parity with the reference's
 * preapproved path-scoped allowlist, generalized to the caller-supplied rule list):
 *
 *  - **host-only** (`example.com`) — matches the host exactly OR any subdomain of
 *    it (`host === d || host.endsWith('.' + d)`), the original AMI behavior.
 *  - **path-scoped** (`github.com/anthropics`) — matches the host (exact or
 *    subdomain) AND a path PREFIX, with SEGMENT-BOUNDARY enforcement: the pathname
 *    must equal the prefix or be followed by '/', so `github.com/anthropics` matches
 *    `/anthropics` and `/anthropics/repo` but NOT `/anthropics-evil/malware`
 *    (prefix-spoofing guard, mirroring isPreapprovedHost L154-166). This lets a
 *    caller scope approval to a sub-path of a shared/multi-tenant host instead of
 *    the whole host.
 *
 * ADDITIVE: entries with no '/' behave exactly as the previous host-only check, so
 * existing callers are unaffected.
 */
export function matchDomainRule(host: string, pathname: string, entry: string): boolean {
  const h = host.toLowerCase();
  const slash = entry.indexOf('/');
  if (slash === -1) {
    const d = entry.toLowerCase();
    return h === d || h.endsWith('.' + d);
  }
  const ruleHost = entry.slice(0, slash).toLowerCase();
  const prefix = entry.slice(slash); // keeps the leading '/'
  if (!(h === ruleHost || h.endsWith('.' + ruleHost))) return false;
  return pathname === prefix || pathname.startsWith(prefix + '/');
}

/**
 * The raw-markdown passthrough gate for web_fetch: decide whether a fetched
 * response from a preapproved trusted-docs host should be returned VERBATIM,
 * skipping the lossy secondary-model summarization pass. Mirrors the reference's
 * WebFetchTool branch (`isPreapprovedUrl(url) && content.length <
 * MAX_MARKDOWN_LENGTH`): raw passthrough requires BOTH
 *   - the host (+ path, via {@link isPreapprovedHost}'s segment-boundary rule) is
 *     on the curated preapproved code/docs allowlist, AND
 *   - the content is strictly under the markdown length ceiling.
 *
 * Owning this decision in the preapproval module (rather than inline in
 * web-fetch.ts) keeps a single source of truth and makes the gate independently
 * unit-testable. `isPreapprovedHost` is an O(1) Set lookup, so callers may also
 * query host-preapproval separately (for the relaxed summary guidelines) without
 * a meaningful cost.
 *
 * ADDITIVE / SUPERSET NOTE: the reference restricts the raw path to `text/markdown`
 * responses; AMI applies it to any textual content on a preapproved host (HTML is
 * already converted to markdown before this gate), which better serves the intent
 * of not lossy-paraphrasing authoritative reference docs. The content-type is
 * therefore decided by the caller, not re-checked here.
 */
export function shouldReturnRawMarkdown(
  hostname: string,
  pathname: string,
  contentLength: number,
  maxMarkdownLength: number,
): boolean {
  return isPreapprovedHost(hostname, pathname) && contentLength < maxMarkdownLength;
}
