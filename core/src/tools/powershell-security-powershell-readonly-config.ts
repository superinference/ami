// Split from powershell-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { resolveToCanonical, stripModulePrefix } from './powershell-parser';
import type { FileOperationType } from '../permissions';
import type { PowerShellReadOnlyConfig } from './powershell-security-power-shell-read-only-config';

/**
 * PowerShell read-only cmdlet allowlist keyed by canonical (lowercase) cmdlet
 * name → its {@link PowerShellReadOnlyConfig}. Single source of truth for both
 * membership ({@link POWERSHELL_READONLY_CMDLETS}) and the per-cmdlet safe-flag
 * gate in {@link isReadOnlyPowerShellCommand}.
 */
export const POWERSHELL_READONLY_CONFIG: Record<string, PowerShellReadOnlyConfig> = {
  // Filesystem / item inspection (read).
  'get-childitem': {
    safeFlags: [
      '-path', '-literalpath', '-filter', '-include', '-exclude', '-recurse',
      '-depth', '-name', '-force', '-attributes', '-directory', '-file',
      '-hidden', '-readonly', '-system',
    ],
  },
  'get-content': {
    safeFlags: [
      '-path', '-literalpath', '-totalcount', '-head', '-tail', '-raw',
      '-encoding', '-delimiter', '-readcount',
    ],
  },
  'get-item': { safeFlags: ['-path', '-literalpath', '-force', '-stream'] },
  'get-itemproperty': { safeFlags: ['-path', '-literalpath', '-name'] },
  'get-itempropertyvalue': { safeFlags: ['-path', '-literalpath', '-name'] },
  'get-acl': {
    safeFlags: ['-path', '-literalpath', '-audit', '-filter', '-include', '-exclude'],
  },
  'get-filehash': {
    safeFlags: ['-path', '-literalpath', '-algorithm', '-inputstream'],
  },
  // Navigation (cwd change only — inert; compound write/multi-cd caught earlier).
  'get-location': { safeFlags: ['-psprovider', '-psdrive', '-stack', '-stackname'] },
  'set-location': { safeFlags: ['-path', '-literalpath', '-passthru', '-stackname'] },
  'push-location': { safeFlags: ['-path', '-literalpath', '-passthru', '-stackname'] },
  'pop-location': { safeFlags: ['-passthru', '-stackname'] },
  // Text searching / filtering (read).
  'select-string': {
    safeFlags: [
      '-path', '-literalpath', '-pattern', '-inputobject', '-simplematch',
      '-casesensitive', '-quiet', '-list', '-notmatch', '-allmatches',
      '-encoding', '-context', '-raw', '-noemphasis',
    ],
  },
  'compare-object': {
    safeFlags: [
      '-referenceobject', '-differenceobject', '-property', '-syncwindow',
      '-casesensitive', '-culture', '-excludedifferent', '-includeequal', '-passthru',
    ],
  },
  // Object inspection / pipeline shaping (output only). Selection/ordering flags
  // are harmless; dangerous arg VALUES are caught by PS_READONLY_DISQUALIFY_RE.
  'select-object': { allowAllFlags: true },
  'sort-object': { allowAllFlags: true },
  'group-object': { allowAllFlags: true },
  'where-object': { allowAllFlags: true },
  'measure-object': { allowAllFlags: true },
  'get-member': {
    safeFlags: ['-inputobject', '-membertype', '-name', '-static', '-view', '-force'],
  },
  'get-unique': { safeFlags: ['-inputobject', '-asstring', '-caseinsensitive', '-ontype'] },
  'get-random': {
    safeFlags: ['-inputobject', '-minimum', '-maximum', '-count', '-setseed', '-shuffle'],
  },
  // Formatters / output (pipeline → host/string, no write).
  'format-table': { allowAllFlags: true },
  'format-list': { allowAllFlags: true },
  'format-wide': { allowAllFlags: true },
  'format-custom': { allowAllFlags: true },
  'format-hex': {
    safeFlags: ['-path', '-literalpath', '-inputobject', '-encoding', '-count', '-offset'],
  },
  'out-string': { allowAllFlags: true },
  'out-host': { allowAllFlags: true },
  'write-output': { safeFlags: ['-inputobject', '-noenumerate'] },
  'write-host': {
    safeFlags: ['-object', '-nonewline', '-separator', '-foregroundcolor', '-backgroundcolor'],
  },
  // Converters (pipeline → object/string, no file write).
  'convertfrom-csv': { safeFlags: ['-inputobject', '-delimiter', '-header', '-useculture'] },
  'convertto-csv': {
    safeFlags: ['-inputobject', '-delimiter', '-notypeinformation', '-noheader', '-usequotes'],
  },
  'convertfrom-json': { safeFlags: ['-inputobject', '-depth', '-ashashtable', '-noenumerate'] },
  'convertto-json': {
    safeFlags: ['-inputobject', '-depth', '-compress', '-enumsasstrings', '-asarray'],
  },
  'convertto-html': {
    safeFlags: [
      '-inputobject', '-property', '-head', '-title', '-body', '-pre', '-post',
      '-as', '-fragment',
    ],
  },
  'convertto-xml': { safeFlags: ['-inputobject', '-depth', '-as', '-notypeinformation'] },
  // Path utilities (read / compute). -Resolve omitted (touches the filesystem).
  'join-path': { safeFlags: ['-path', '-childpath', '-additionalchildpath'] },
  'join-string': {
    safeFlags: [
      '-inputobject', '-property', '-separator', '-outputprefix', '-outputsuffix',
      '-singlequote', '-doublequote', '-formatstring',
    ],
  },
  'split-path': {
    safeFlags: [
      '-path', '-literalpath', '-qualifier', '-noqualifier', '-parent', '-leaf',
      '-leafbase', '-extension', '-isabsolute',
    ],
  },
  'resolve-path': { safeFlags: ['-path', '-literalpath', '-relative'] },
  'convert-path': { safeFlags: ['-path', '-literalpath'] },
  'test-path': {
    safeFlags: [
      '-path', '-literalpath', '-pathtype', '-filter', '-include', '-exclude',
      '-isvalid', '-newerthan', '-olderthan',
    ],
  },
  // System / environment info (read).
  'get-date': { safeFlags: ['-date', '-format', '-uformat', '-displayhint', '-asutc'] },
  'get-alias': { safeFlags: ['-name', '-definition', '-scope', '-exclude'] },
  // NOTE (intentional AMI superset divergence): the reference removed Get-Help from its
  // allowlist (module-autoload hazard via pipelined -Name). AMI keeps it
  // read-only (allowAllFlags) to preserve the long-standing `Get-Help` auto-allow
  // perk; the autoload attack still requires a $-bearing form rejected by
  // PS_READONLY_DISQUALIFY_RE.
  'get-help': { allowAllFlags: true },
  'get-history': { safeFlags: ['-id', '-count'] },
  'get-host': { allowAllFlags: true },
  'get-culture': { allowAllFlags: true },
  'get-uiculture': { allowAllFlags: true },
  'get-timezone': { safeFlags: ['-name', '-id', '-listavailable'] },
  'get-uptime': { allowAllFlags: true },
  'get-computerinfo': { allowAllFlags: true },
  'get-hotfix': { safeFlags: ['-id', '-description'] },
  'get-process': {
    safeFlags: ['-name', '-id', '-module', '-fileversioninfo', '-includeusername'],
  },
  'get-service': {
    safeFlags: [
      '-name', '-displayname', '-dependentservices', '-requiredservices',
      '-include', '-exclude',
    ],
  },
  'get-module': {
    safeFlags: ['-name', '-listavailable', '-all', '-fullyqualifiedname', '-psedition'],
  },
  'get-psdrive': { safeFlags: ['-name', '-psprovider', '-scope'] },
  'get-psprovider': { safeFlags: ['-psprovider'] },
  'get-cimclass': {
    safeFlags: ['-classname', '-namespace', '-methodname', '-propertyname', '-qualifiername'],
  },
  // Network info (read). -ComputerName / -CimSession (remote) deliberately absent.
  'get-netadapter': {
    safeFlags: ['-name', '-interfacedescription', '-interfaceindex', '-physical'],
  },
  'get-netipaddress': {
    safeFlags: ['-interfaceindex', '-interfacealias', '-addressfamily', '-type'],
  },
  'get-netipconfiguration': {
    safeFlags: ['-interfaceindex', '-interfacealias', '-detailed', '-all'],
  },
  'get-netroute': {
    safeFlags: ['-interfaceindex', '-interfacealias', '-addressfamily', '-destinationprefix'],
  },
  'get-dnsclient': { safeFlags: ['-interfaceindex', '-interfacealias'] },
  'get-dnsclientcache': {
    safeFlags: ['-entry', '-name', '-type', '-status', '-section', '-data'],
  },
  // Event log (read). -FilterXml/-FilterHashtable (XXE → SSRF) deliberately
  // absent; -FilterXPath (plain pattern) stays.
  'get-eventlog': {
    safeFlags: [
      '-logname', '-newest', '-after', '-before', '-entrytype', '-index',
      '-instanceid', '-message', '-source', '-username', '-asbaseobject', '-list',
    ],
  },
  'get-winevent': {
    safeFlags: [
      '-logname', '-listlog', '-listprovider', '-providername', '-path',
      '-maxevents', '-filterxpath', '-force', '-oldest',
    ],
  },
  // Timing (inert).
  'start-sleep': { safeFlags: ['-seconds', '-milliseconds', '-duration'] },
};

// Constructs that make a command un-analyzable as a pure read, so their presence
// fails the read-only classification closed (never auto-allow): variables /
// subexpressions / env leaks (`$`), script blocks (`{ }`), grouping /
// subexpressions (`(`), splatting / arrays / hashtables (`@`), the call operator
// (`&`), backtick escapes, and output redirection (`>` — a file write). Mirrors
// readOnlyValidation.ts's fail-closed posture (argLeaksValue + the
// SAFE_PATH_ELEMENT_TYPES whitelist) without needing the native AST: rejecting
// these wholesale is strictly more conservative than the per-arg element-type
// whitelist and keeps this a cheap synchronous check for the classifier.
export const PS_READONLY_DISQUALIFY_RE = /[$`{}()@&>]/;

/**
 * Canonicalize the FIRST (command-name) token of a shell command / permission
 * rule pattern to its canonical PowerShell cmdlet name for cross-alias rule
 * matching, leaving the remainder intact apart from normalizing the name↔args
 * separator to a single space.
 *
 * This is the engine behind cross-alias permission matching on Windows (where
 * AMI's bash tool runs PowerShell): a deny rule written for `Remove-Item` must
 * also block `rm` / `del` / `ri`, and a rule for `rm` must block `Remove-Item`,
 * because they name the same cmdlet. Mirrors the reference's PowerShell
 * `filterRulesByContentsMatchingInput` canonical substitution.
 *
 * `stripModule` encodes the deny-over-match / allow-no-over-match asymmetry:
 *  - INPUT commands and DENY/ASK rules pass `true` — a module-qualified name
 *    (`Microsoft.PowerShell.Management\Remove-Item`) is reduced to its bare
 *    cmdlet so an obfuscating module prefix cannot hide a denied command
 *    (fail-safe over-match is acceptable for deny/ask).
 *  - ALLOW rules pass `false` — the module prefix is KEPT so an allow rule
 *    scoped to `ModuleA\Get-Thing` can NEVER over-match `ModuleB\Get-Thing`
 *    (allow must never widen; fail-closed).
 *
 * The separator is normalized because prefix rule matching appends a literal
 * space (`prefix + ' '`); without it a tab-separated `rm\t./x` canonicalizes to
 * `remove-item\t./x` and misses a `Remove-Item` prefix rule while other paths
 * still match it (a deny-rule bypass). The returned name is lowercase
 * ({@link resolveToCanonical}); callers must compare case-insensitively.
 */
export function canonicalizePowerShellCommandName(
  input: string,
  stripModule: boolean,
): string {
  const raw = input.replace(/^\s+/, '');
  const firstToken = raw.split(/\s+/)[0] ?? '';
  if (firstToken.length === 0) return input;
  const nameForResolve = stripModule ? stripModulePrefix(firstToken) : firstToken;
  const canonical = resolveToCanonical(nameForResolve);
  const rest = raw.slice(firstToken.length).replace(/^\s+/, ' ');
  return canonical + rest;
}

// ---------------------------------------------------------------------------
// Per-cmdlet path-parameter configuration + path extraction.
//
// Mirrors the reference's PowerShellTool/pathValidation.ts CMDLET_PATH_CONFIG +
// extractPathsFromCommand. Each config entry declares, per cmdlet, which
// parameters carry filesystem paths (validated), which are switches (consume no
// value), and which are value-taking non-path params (consume a value, not a
// path). The SECURITY MODEL is fail-safe: ANY -Param the config does not
// recognize sets `hasUnvalidatablePathArg` so the caller forces an ask — this
// ends the "a missing switch swallows the positional path" whack-a-mole.
//
// Three OPTIONAL per-cmdlet knobs tune extraction (each with an explicit default
// so an omitted knob behaves identically to the reference):
//   - leafOnlyPathParams (default: none) — a param (e.g. New-Item -Name) that
//     PowerShell resolves RELATIVE TO another param, not cwd. Only a simple leaf
//     filename is extracted; any value containing `/`, `\`, `.`, or `..` forces
//     an ask (it would resolve against the wrong base).
//   - positionalSkip (default: 0) — number of leading positional args that are
//     NOT filesystem paths (e.g. 1 for Invoke-WebRequest, whose positional-0 is
//     a `-Uri` URL). Without this a URL is mis-extracted as a path.
//   - optionalWrite (default: false) — a write cmdlet that only touches disk when
//     a path param is present (e.g. iwr/irm without -OutFile write to the
//     pipeline, not disk). Skips the "write with no target → ask" rule.
// ---------------------------------------------------------------------------

/**
 * Per-cmdlet parameter configuration consumed by
 * {@link extractPowerShellCommandPaths}. Common parameters (-Verbose,
 * -ErrorAction, …) are NOT listed here — they are merged in from
 * {@link COMMON_SWITCHES} / {@link COMMON_VALUE_PARAMS} at lookup time. Parameter
 * names are lowercase with a leading dash to match runtime comparison.
 */
export type CmdletPathConfig = {
  /** Whether this cmdlet reads or writes to the filesystem. */
  operationType: FileOperationType;
  /** Parameters that accept file paths (extracted + validated). */
  pathParams: string[];
  /** Switch parameters that take NO value (next arg is NOT consumed). */
  knownSwitches: string[];
  /** Value-taking params that are NOT paths (next arg consumed, not validated). */
  knownValueParams: string[];
  /**
   * Params accepting a leaf filename resolved by PowerShell relative to ANOTHER
   * param (not cwd). Safe to extract only when the value is a simple leaf (no
   * `/`, `\`, `.`, `..`); non-leaf values force an ask. Default: none.
   */
  leafOnlyPathParams?: string[];
  /**
   * Number of leading positional args to SKIP (not extracted as paths) — used
   * where positional-0 is a non-path value (e.g. Invoke-WebRequest's -Uri URL).
   * Default: 0.
   */
  positionalSkip?: number;
  /**
   * When true, the cmdlet only writes to disk when a pathParam is present;
   * without one it is effectively a read (output → pipeline). Skips the "write
   * with no target path" forced-ask. Cmdlets that ALWAYS write must NOT set this.
   * Default: false.
   */
  optionalWrite?: boolean;
};
