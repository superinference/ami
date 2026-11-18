import {
  COMMON_ALIASES,
  getAllCommands,
  hasCommandNamed,
  getVariablesByScope,
  deriveSecurityFlags,
  commandHasArgAbbreviation,
  resolveToCanonical,
  isPowerShellParameter,
} from './powershell-parser';
import type {
  ParsedPowerShellCommand,
  ParsedCommandElement,
  CommandElementType,
} from './powershell-parser';
// Reuse the canonical filesystem-operation discriminant (no duplicate type).
import type { FileOperationType } from '../permissions';

/**
 * PowerShell Constrained Language Mode (CLM) type-cast security boundary.
 *
 * Microsoft's CLM restricts .NET type usage to an allowlist when PowerShell runs
 * under AppLocker/WDAC system lockdown. Any type NOT in this set is considered
 * unsafe for untrusted code execution. We invert this: a type literal used in a
 * cast (`[Type]'value'`) or static-member access (`[Type]::member`) that is NOT
 * in the allowlist triggers an ask — one canonical check that replaces
 * enumerating individual dangerous types (named pipes, reflection, process
 * spawning, P/Invoke marshaling, remote WMI/LDAP binds, etc.).
 *
 * Source: https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_language_modes
 *
 * This mirrors Claude Code's PowerShellTool/clmTypes.ts (CLM_ALLOWED_TYPES,
 * normalizeTypeName, isClmAllowedType) and its security-hardened removals of the
 * network-binding accelerators.
 *
 * Normalization: entries stored lowercase, short AND full names where both exist
 * (PowerShell resolves accelerators like `[int]` → System.Int32 at runtime; we
 * match the literal text the command carries).
 */
export const CLM_ALLOWED_TYPES: ReadonlySet<string> = new Set(
  [
    // Type accelerators (short names as they appear in a cast literal).
    // SECURITY: 'adsi' and 'adsisearcher' are REMOVED from Microsoft's list.
    // Both are Active Directory Service Interface types that perform NETWORK
    // BINDS when cast:
    //   [adsi]'LDAP://evil.com/...'           → connects to an LDAP server
    //   [adsisearcher]'(objectClass=user)'    → binds to AD and queries
    // Microsoft allows them because CLM targets Windows admins in trusted
    // domains; we block them since the cast target isn't validated.
    'alias',
    'allowemptycollection',
    'allowemptystring',
    'allownull',
    'argumentcompleter',
    'argumentcompletions',
    'array',
    'bigint',
    'bool',
    'byte',
    'char',
    'cimclass',
    'cimconverter',
    'ciminstance',
    // 'cimsession' REMOVED — see the wmi/adsi rationale below.
    'cimtype',
    'cmdletbinding',
    'cultureinfo',
    'datetime',
    'decimal',
    'double',
    'dsclocalconfigurationmanager',
    'dscproperty',
    'dscresource',
    'experimentaction',
    'experimental',
    'experimentalfeature',
    'float',
    'guid',
    'hashtable',
    'int',
    'int16',
    'int32',
    'int64',
    'ipaddress',
    'ipendpoint',
    'long',
    'mailaddress',
    'norunspaceaffinity',
    'nullstring',
    'objectsecurity',
    'ordered',
    'outputtype',
    'parameter',
    'physicaladdress',
    'pscredential',
    'pscustomobject',
    'psdefaultvalue',
    'pslistmodifier',
    'psobject',
    'psprimitivedictionary',
    'pstypenameattribute',
    'ref',
    'regex',
    'sbyte',
    'securestring',
    'semver',
    'short',
    'single',
    'string',
    'supportswildcards',
    'switch',
    'timespan',
    'uint',
    'uint16',
    'uint32',
    'uint64',
    'ulong',
    'uri',
    'ushort',
    'validatecount',
    'validatedrive',
    'validatelength',
    'validatenotnull',
    'validatenotnullorempty',
    'validatenotnullorwhitespace',
    'validatepattern',
    'validaterange',
    'validatescript',
    'validateset',
    'validatetrusteddata',
    'validateuserdrive',
    'version',
    'void',
    'wildcardpattern',
    // SECURITY: 'wmi', 'wmiclass', 'wmisearcher', 'cimsession' REMOVED.
    // WMI casts run WMI queries that can target remote computers (network
    // request) and reach dangerous classes like Win32_Process. cimsession
    // opens a CIM session (network connection to a remote host):
    //   [wmi]'\\host\root\cimv2:Win32_Process.Handle="1"' → remote WMI
    //   [wmisearcher]'SELECT * FROM Win32_Process'          → runs WQL
    // Same rationale as the adsi/adsisearcher removal above.
    'x500distinguishedname',
    'x509certificate',
    'xml',
    // Full names for accelerators that resolve to System.* (a cast may use either).
    'system.array',
    'system.boolean',
    'system.byte',
    'system.char',
    'system.datetime',
    'system.decimal',
    'system.double',
    'system.guid',
    'system.int16',
    'system.int32',
    'system.int64',
    'system.numerics.biginteger',
    'system.sbyte',
    'system.single',
    'system.string',
    'system.timespan',
    'system.uint16',
    'system.uint32',
    'system.uint64',
    'system.uri',
    'system.version',
    'system.void',
    'system.collections.hashtable',
    'system.text.regularexpressions.regex',
    'system.globalization.cultureinfo',
    'system.net.ipaddress',
    'system.net.ipendpoint',
    'system.net.mail.mailaddress',
    'system.net.networkinformation.physicaladdress',
    'system.security.securestring',
    'system.security.cryptography.x509certificates.x509certificate',
    'system.security.cryptography.x509certificates.x500distinguishedname',
    'system.xml.xmldocument',
    // System.Management.Automation.* — FQ equivalents of PS-specific accelerators.
    'system.management.automation.pscredential',
    'system.management.automation.pscustomobject',
    'system.management.automation.pslistmodifier',
    'system.management.automation.psobject',
    'system.management.automation.psprimitivedictionary',
    'system.management.automation.psreference',
    'system.management.automation.semanticversion',
    'system.management.automation.switchparameter',
    'system.management.automation.wildcardpattern',
    'system.management.automation.language.nullstring',
    // Microsoft.Management.Infrastructure.* — FQ equivalents of CIM accelerators.
    // SECURITY: cimsession FQ REMOVED — same network-bind hazard as the short
    // name (creates a CIM session to a remote host).
    'microsoft.management.infrastructure.cimclass',
    'microsoft.management.infrastructure.cimconverter',
    'microsoft.management.infrastructure.ciminstance',
    'microsoft.management.infrastructure.cimtype',
    // FQ equivalents of remaining short-name accelerators.
    // SECURITY: DirectoryEntry/DirectorySearcher/ManagementObject/
    // ManagementClass/ManagementObjectSearcher FQ REMOVED — same network-bind
    // hazard as the short names adsi/adsisearcher/wmi/wmiclass/wmisearcher
    // (LDAP bind, remote WMI). See the short-name removal comments above.
    'system.collections.specialized.ordereddictionary',
    'system.security.accesscontrol.objectsecurity',
    // Arrays of allowed types are allowed (e.g. [string[]]). normalizeTypeName
    // strips [] before lookup, so store the base name.
    'object',
    'system.object',
    // ModuleSpecification — full qualified name.
    'microsoft.powershell.commands.modulespecification',
  ].map((t) => t.toLowerCase()),
);

/**
 * Normalize a type name as it appears in a PowerShell cast literal.
 * Handles: case folding, array suffix (`String[]` → `string`), and generic
 * brackets (`List[int]` → `list`, conservative — the outer generic wrapper may
 * be unsafe even when the type argument is safe, so we check the outer type).
 */
export function normalizeTypeName(name: string): string {
  return name
    .toLowerCase()
    .replace(/\[\]$/, '')
    .replace(/\[.*\]$/, '')
    .trim();
}

/**
 * True if `typeName` (as written in a cast literal) is in Microsoft's CLM
 * allowlist. Types NOT in this set trigger an ask — they reach system APIs CLM
 * blocks (reflection, P/Invoke, network binds, process spawning).
 */
export function isClmAllowedType(typeName: string): boolean {
  return CLM_ALLOWED_TYPES.has(normalizeTypeName(typeName));
}

// A PowerShell type literal applied as a cast or static-member access: `[Type]`
// immediately followed (modulo whitespace) by a value token (`'`, `"`, `$`, `@`,
// `(`) or a static-member accessor (`::`). The type name starts with a letter,
// may contain dots (FQ names) and a trailing `[]` (array). Requiring the literal
// to be *applied* (a value/`::` follows) means bash array/glob/test-bracket
// syntax (`[0]`, `[a-z]*`, `[ -f x ]`, `${a[i]}`) does not match.
const PS_TYPE_CAST_RE = /\[([A-Za-z_][A-Za-z0-9_.]*(?:\[\])?)\]\s*(?=['"$@(]|::)/g;

/**
 * Scan a command for PowerShell type-cast / static-member literals whose target
 * type is outside the CLM allowlist. The first non-allowlisted type found is
 * returned so the caller can surface an ask (never a silent allow). This is the
 * fail-safe CLM boundary: one allowlist check replaces enumerating every
 * individual dangerous .NET type.
 *
 * The input is scanned as given — callers that have already stripped invisible
 * Unicode / normalized alternate dashes should pass the normalized string.
 */
export function detectUnsafeTypeCast(command: string): {
  blocked: boolean;
  reason?: string;
  type?: string;
} {
  PS_TYPE_CAST_RE.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PS_TYPE_CAST_RE.exec(command)) !== null) {
    const raw = match[1];
    if (!isClmAllowedType(raw)) {
      return {
        blocked: true,
        type: raw,
        reason: `PowerShell casts to non-allowlisted .NET type [${raw}] — outside Constrained Language Mode's safe-type allowlist (can perform network binds, reflection, P/Invoke marshaling, or process spawning)`,
      };
    }
  }
  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Shared dangerous-cmdlet config lists.
//
// Mirrors Claude Code's src/utils/powershell/dangerousCmdlets.ts: one canonical
// set per hazard class, consumed by the detectors below so a cmdlet is listed
// once and every check picks it up (no sync drift). Entries are lowercase.
// ---------------------------------------------------------------------------

/**
 * Cmdlets that compile/load arbitrary .NET or module code at runtime. `Add-Type`
 * compiles and loads C#; module-import cmdlets run a `.psm1`'s top-level body on
 * import — the same arbitrary-code-execution risk as `Invoke-Expression`.
 */
export const CODE_EXEC_CMDLETS: ReadonlySet<string> = new Set(['add-type']);

/**
 * Cmdlets that load and execute module/script code. A `.psm1` file runs its
 * top-level body on import, so importing an attacker-controlled module executes
 * arbitrary code without an explicit `iex`.
 */
export const MODULE_LOADING_CMDLETS: ReadonlySet<string> = new Set([
  'import-module',
  'ipmo',
  'install-module',
  'save-module',
  'update-module',
  'install-script',
  'save-script',
]);

/**
 * WMI/CIM process spawners. `Invoke-WmiMethod -Class Win32_Process -Name Create`
 * (and the CIM equivalent) spawns arbitrary processes, bypassing a Start-Process
 * check. `-Class`/`-Name` accept arbitrary strings, so there is no narrow safe
 * form — any invocation is surfaced.
 */
export const WMI_CIM_CMDLETS: ReadonlySet<string> = new Set([
  'invoke-wmimethod',
  'iwmi',
  'invoke-cimmethod',
]);

/**
 * Alias/variable mutation cmdlets. `Set-Alias Get-Content Invoke-Expression`
 * hijacks future command resolution; `Set-Variable PSDefaultParameterValues …`
 * poisons every subsequent cmdlet. Neither effect is statically verifiable.
 *
 * The ultra-short `sv`/`nv` aliases from the upstream list are intentionally
 * omitted: this detector blocks unconditionally in command position and also
 * runs over POSIX-shell commands, where `sv` (runit service control) is a real
 * command — same no-POSIX-false-positive rationale as ENV_WRITE_CMDLETS.
 */
export const ALIAS_HIJACK_CMDLETS: ReadonlySet<string> = new Set([
  'set-alias',
  'sal',
  'new-alias',
  'nal',
  'set-variable',
  'new-variable',
]);

/**
 * Write cmdlets that, when targeting the `env:` drive, mutate environment
 * variables (e.g. `Set-Item Env:Path …`). Unlike Claude's list, bash-colliding
 * destructive aliases (`rm`, `del`, `rmdir`, `erase`) are intentionally omitted:
 * this detector also runs over POSIX-shell commands, and the env: scope gate
 * (below) already requires an `env:` reference before any of these can match.
 */
export const ENV_WRITE_CMDLETS: ReadonlySet<string> = new Set([
  'set-item',
  'si',
  'new-item',
  'ni',
  'remove-item',
  'ri',
  'clear-item',
  'cli',
  'set-content',
  'add-content',
  'ac',
]);

/**
 * Network cmdlets. A wildcard allow-rule for these (`invoke-webrequest*`)
 * enables arbitrary download/exfil without a prompt — there is no legitimate
 * narrow prefix. They are NOT unconditionally blocked (a bare web request is
 * allowed; only download-and-execute cradles are surfaced by
 * detectPowerShellExecution), but they are never offered as a wildcard rule.
 */
export const NETWORK_CMDLETS: ReadonlySet<string> = new Set([
  'invoke-webrequest',
  'invoke-restmethod',
]);

/**
 * Cmdlets that accept a `-FilePath` (or positional path) and execute the
 * file's contents as a script — the same arbitrary-code-execution risk as
 * `Invoke-Expression`, so they must never be blanket-allowed via a prefix rule.
 */
export const FILEPATH_EXECUTION_CMDLETS: ReadonlySet<string> = new Set([
  'invoke-command',
  'start-job',
  'start-threadjob',
  'register-scheduledjob',
]);

/**
 * Cmdlets where a scriptblock argument executes arbitrary code (not merely
 * filtering/transforming pipeline input). A `foo*` wildcard rule would
 * auto-allow any scriptblock passed to them forever.
 */
export const DANGEROUS_SCRIPT_BLOCK_CMDLETS: ReadonlySet<string> = new Set([
  'invoke-command',
  'invoke-expression',
  'start-job',
  'start-threadjob',
  'register-scheduledjob',
  'register-engineevent',
  'register-objectevent',
  'register-wmievent',
  'new-pssession',
  'enter-pssession',
]);

/**
 * Arg-gated cmdlets: safe for ordinary string arguments but dangerous when
 * passed a scriptblock / variable / subexpression. `ForEach-Object` and the
 * other allowlist cmdlets auto-allow safe args (StringConstant identifiers),
 * so the permission dialog only fires when the arg is a scriptblock —
 * accepting a `foreach-object*` wildcard at that point would match ALL future
 * invocations (`ForEach-Object { Remove-Item -Recurse / }`), bypassing the
 * arg gate forever. The native executables (ipconfig/route/hostname) carry the
 * same callback-gated flag cases (`ipconfig /all` allowed, `/flushdns`
 * rejected) and the same wildcard-bypass risk. Mirrors Claude Code's
 * ARG_GATED_CMDLETS (utils/powershell/dangerousCmdlets.ts).
 */
export const ARG_GATED_CMDLETS: ReadonlySet<string> = new Set([
  'select-object',
  'sort-object',
  'group-object',
  'where-object',
  'measure-object',
  'write-output',
  'write-host',
  'start-sleep',
  'format-table',
  'format-list',
  'format-wide',
  'format-custom',
  'out-string',
  'out-host',
  // Native executables with callback-gated args (same bypass risk).
  'ipconfig',
  'hostname',
  'route',
]);

// PowerShell shells / process spawners / code loaders that must never be
// offered as a bare wildcard prefix. The bare shell names (pwsh/powershell/cmd/
// bash/…) are already covered by permissions.ts's BARE_SHELL_PREFIXES, so this
// list holds only the PowerShell-cmdlet spawners/loaders not represented there.
const PS_SPAWNERS_AND_LOADERS: readonly string[] = [
  'start-process',
  'add-type',
  'new-object',
];

/**
 * Alias forms (from the shared {@link COMMON_ALIASES} map) whose canonical
 * cmdlet is in `canonical` — so `invoke-expression` in the set automatically
 * pulls in `iex`. Mirrors Claude Code's `aliasesOf()` derivation: add a cmdlet
 * to one validator list and its aliases follow, no separate maintenance.
 */
function aliasesOf(canonical: ReadonlySet<string>): string[] {
  return Object.entries(COMMON_ALIASES)
    .filter(([, target]) => canonical.has(target.toLowerCase()))
    .map(([alias]) => alias.toLowerCase());
}

/**
 * Commands never offered as a reusable wildcard-prefix ("allow always") rule in
 * the permission dialog. Derived from EVERY dangerous-cmdlet category above plus
 * the PowerShell spawners/loaders, then expanded with their aliases — so a
 * cmdlet listed once (in the category that fits its hazard) is covered here
 * automatically, with no sync drift. A wildcard rule for any of these would
 * defeat the per-command security checks (download/exfil, code-exec, process
 * spawn, scriptblock arg gate, …). Mirrors Claude Code's `NEVER_SUGGEST`
 * (utils/powershell/dangerousCmdlets.ts): the suggestion gate is a single-name
 * lookup on the command's first word, so the set holds bare names only.
 */
export const POWERSHELL_NEVER_SUGGEST: ReadonlySet<string> = (() => {
  const canonical = new Set<string>([
    ...PS_SPAWNERS_AND_LOADERS,
    ...CODE_EXEC_CMDLETS,
    ...MODULE_LOADING_CMDLETS,
    ...NETWORK_CMDLETS,
    ...FILEPATH_EXECUTION_CMDLETS,
    ...DANGEROUS_SCRIPT_BLOCK_CMDLETS,
    ...ALIAS_HIJACK_CMDLETS,
    ...WMI_CIM_CMDLETS,
    ...ARG_GATED_CMDLETS,
    // ForEach-Object's -MemberName (positional `% Delete`) resolves against the
    // runtime pipeline object (`Get-ChildItem | % Delete` invokes
    // FileInfo.Delete()); its arg carries no property/method signal, so it is
    // never allowlisted and must never be wildcard-suggested. Not in ARG_GATED.
    'foreach-object',
  ]);
  return new Set<string>([...canonical, ...aliasesOf(canonical)]);
})();

/**
 * True if `name` (a command's first word, any case, with or without a Windows
 * executable extension) is a PowerShell cmdlet/alias that must never be offered
 * as a wildcard-prefix permission rule. Consumed by permissions.ts's
 * prefix-suggestion helpers (suggestAllowPrefix / getFirstWordPrefix).
 */
export function isPowerShellNeverSuggest(name: string): boolean {
  const lower = name.toLowerCase().replace(/\.(exe|cmd|bat|com)$/, '');
  return POWERSHELL_NEVER_SUGGEST.has(lower);
}

type PsDetection = { blocked: boolean; reason?: string };
const PS_PASS: PsDetection = { blocked: false };

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

const POWERSHELL_EXECUTABLES: ReadonlySet<string> = new Set([
  'pwsh',
  'pwsh.exe',
  'powershell',
  'powershell.exe',
]);

/**
 * True if `name` is (or, via its path basename, resolves to) a PowerShell
 * executable. Handles bare names, `/usr/bin/pwsh`, and `C:\…\powershell.exe`.
 */
function isPowerShellExecutable(name: string): boolean {
  const lower = name.toLowerCase();
  if (POWERSHELL_EXECUTABLES.has(lower)) {
    return true;
  }
  const lastSep = Math.max(lower.lastIndexOf('/'), lower.lastIndexOf('\\'));
  return lastSep >= 0 && POWERSHELL_EXECUTABLES.has(lower.slice(lastSep + 1));
}

// A command/cmdlet appears in "command position" when it starts the whole string
// or follows a statement/pipeline separator (`;`, `|`, `&`, newline) or an
// opening group (`(`, `{`). An optional module qualifier
// (`Microsoft.PowerShell.Utility\`) may precede the name. Matching only in
// command position avoids flagging a cmdlet name that appears as a plain
// argument (e.g. `Write-Output set-alias`).
const PS_CMD_POS_PREFIX = String.raw`(?:^|[;|&(){}\n\r])\s*(?:[\w.]+[\\/])?`;

/**
 * True if any of `names` is invoked in command position within `command`.
 * Returns the matched name (lowercased) so callers can build a message.
 */
function cmdletInCommandPosition(
  command: string,
  names: Iterable<string>,
): string | undefined {
  for (const name of names) {
    const re = new RegExp(PS_CMD_POS_PREFIX + escapeRegExp(name) + String.raw`\b`, 'i');
    if (re.test(command)) {
      return name;
    }
  }
  return undefined;
}

/**
 * Split a command into the whitespace-separated tokens that follow the first
 * command-position match of `cmdletRe`, up to the next statement/pipeline
 * separator. Quoted runs are preserved as single tokens (quotes stripped).
 * Used by the New-Object / Start-Process argument scanners below.
 */
function argTokensAfter(command: string, cmdletRe: RegExp): string[] | undefined {
  const m = cmdletRe.exec(command);
  if (!m) {
    return undefined;
  }
  const rest = command.slice(m.index + m[0].length);
  // Stop at the next top-level statement or pipeline boundary.
  const segment = rest.split(/[;|\n\r]/)[0] ?? '';
  const raw = segment.match(/'[^']*'|"[^"]*"|\S+/g) ?? [];
  return raw.map((t) => t.replace(/^['"]|['"]$/g, ''));
}

/**
 * `New-Object` hazards (mirrors Claude's checkComObject + the CLM string-arg
 * extension): `-ComObject` instantiates an OLE automation object that is itself
 * an execution/download primitive, and a `-TypeName` passed as a *string*
 * (`New-Object System.Net.WebClient`) escapes the `[bracket]` CLM cast check, so
 * the type name is extracted (named / colon-bound / positional-0, skipping the
 * other New-Object value parameters) and run through the CLM allowlist.
 */
export function detectUnsafeNewObject(command: string): PsDetection {
  const cmdletRe = new RegExp(PS_CMD_POS_PREFIX + String.raw`new-object\b`, 'gi');
  cmdletRe.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = cmdletRe.exec(command)) !== null) {
    const rest = command.slice(m.index + m[0].length);
    const segment = rest.split(/[;|\n\r]/)[0] ?? '';
    const args = (segment.match(/'[^']*'|"[^"]*"|\S+/g) ?? []).map((t) =>
      t.replace(/^['"]|['"]$/g, ''),
    );
    // -ComObject (min abbrev -com; -co collides with -Confirm in PS 5.1).
    if (
      args.some((a) => {
        const l = a.toLowerCase().split(':')[0]!;
        return /^[-/]com/.test(l) && '-comobject'.startsWith(l.replace('/', '-'));
      })
    ) {
      return {
        blocked: true,
        reason:
          'PowerShell New-Object -ComObject instantiates a COM automation object which is an execution/download primitive',
      };
    }
    // Extract -TypeName (named, colon-bound, or positional-0) then CLM-check it.
    const VALUE_PARAMS = new Set(['-argumentlist', '-comobject', '-property']);
    const SWITCH_PARAMS = new Set(['-strict']);
    let typeName: string | undefined;
    for (let i = 0; i < args.length && typeName === undefined; i++) {
      const a = args[i]!;
      const lower = a.toLowerCase();
      const dashed = lower.replace(/^\//, '-');
      // Colon-bound: -TypeName:Foo.Bar (or -t:Foo.Bar).
      if (dashed.startsWith('-t') && dashed.includes(':')) {
        const paramPart = dashed.slice(0, dashed.indexOf(':'));
        if ('-typename'.startsWith(paramPart)) {
          typeName = a.slice(a.indexOf(':') + 1);
        }
        continue;
      }
      // Space-separated: -TypeName Foo.Bar.
      if (dashed.startsWith('-t') && '-typename'.startsWith(dashed)) {
        typeName = args[i + 1];
        continue;
      }
      // Other named params consume (or don't) a following value; skip to reach
      // a positional TypeName.
      if (dashed.startsWith('-')) {
        if (dashed.includes(':')) continue;
        if (SWITCH_PARAMS.has(dashed)) continue;
        if (VALUE_PARAMS.has(dashed)) i++;
        continue;
      }
      // First non-dash token binds positionally to -TypeName.
      typeName = a;
    }
    if (typeName !== undefined && !isClmAllowedType(typeName)) {
      return {
        blocked: true,
        reason: `PowerShell New-Object instantiates .NET type '${typeName}' outside the Constrained Language Mode allowlist`,
      };
    }
  }
  return PS_PASS;
}

/**
 * `Start-Process` elevation / nested-shell launch (mirrors Claude's
 * checkStartProcess): `-Verb RunAs` (space or colon syntax, quote/backtick
 * tolerant) requests a UAC elevation, and launching a nested `pwsh`/`powershell`
 * spawns an unvalidated child shell.
 */
function startProcessArgHazard(args: string[]): PsDetection {
  // -Verb RunAs — colon form (-Verb:'RunAs', backtick/quote tolerant).
  const colonVerb = args.some((a) =>
    /^[-/]v[a-z]*:[`'" ]*runas[`'" ]*$/i.test(a.replace(/`/g, '')),
  );
  // -Verb RunAs — space form: a -Verb/-v param token with a later bare `runas`.
  const spaceVerb =
    args.some((a) => /^[-/](?:v|ve|ver|verb)$/i.test(a.replace(/`/g, ''))) &&
    args.some((a) => a.replace(/[`'"]/g, '').toLowerCase() === 'runas');
  if (colonVerb || spaceVerb) {
    return {
      blocked: true,
      reason: 'PowerShell Start-Process -Verb RunAs requests elevated (administrator) privileges',
    };
  }
  // Nested PowerShell target (first positional or -FilePath value).
  if (args.some((a) => isPowerShellExecutable(a))) {
    return {
      blocked: true,
      reason:
        'PowerShell Start-Process launches a nested PowerShell process which cannot be validated',
    };
  }
  return PS_PASS;
}

export function detectStartProcessElevation(command: string): PsDetection {
  // start-process, and its aliases saps / start (COMMON_ALIASES). The `start`
  // alias is matched too — the reference checkStartProcess keys on
  // start-process / saps / start — but only in command position, so a bare
  // `start` argument or a word like `restart` cannot trigger it. EVERY
  // command-position occurrence is scanned (mirroring the reference's
  // getAllCommands) so a hazardous Start-Process after a benign one is not
  // missed.
  const cmdletRe = new RegExp(
    PS_CMD_POS_PREFIX + String.raw`(?:start-process|saps|start)\b`,
    'gi',
  );
  let m: RegExpExecArray | null;
  while ((m = cmdletRe.exec(command)) !== null) {
    const rest = command.slice(m.index + m[0].length);
    const segment = rest.split(/[;|\n\r]/)[0] ?? '';
    const args = (segment.match(/'[^']*'|"[^"]*"|\S+/g) ?? []).map((t) =>
      t.replace(/^['"]|['"]$/g, ''),
    );
    const hazard = startProcessArgHazard(args);
    if (hazard.blocked) {
      return hazard;
    }
  }
  return PS_PASS;
}

/**
 * Runtime-state hijack (mirrors Claude's checkRuntimeStateManipulation): any
 * alias/variable mutation cmdlet in command position. The command-position
 * matcher already strips an optional module qualifier.
 */
export function detectRuntimeStateHijack(command: string): PsDetection {
  if (cmdletInCommandPosition(command, ALIAS_HIJACK_CMDLETS)) {
    return {
      blocked: true,
      reason:
        'PowerShell creates or modifies an alias or variable that can hijack future command resolution',
    };
  }
  return PS_PASS;
}

/**
 * Environment-variable manipulation (mirrors Claude's checkEnvVarManipulation):
 * only fires when an `env:` scope reference is present, then blocks on a direct
 * `$env:NAME = …` assignment or an env: write cmdlet. Gating on an `env:`
 * reference keeps POSIX-shell commands (which share token names like
 * `set-content`) from matching. The `[Environment]::SetEnvironmentVariable(...)`
 * form is already surfaced by the CLM type-cast check (`[environment]` is not an
 * allowlisted type), so it is intentionally not duplicated here.
 */
export function detectEnvVarManipulation(command: string): PsDetection {
  if (!/\benv:/i.test(command)) {
    return PS_PASS;
  }
  if (/\$env:[\w{}()]+\s*\+?=/i.test(command)) {
    return {
      blocked: true,
      reason: 'PowerShell assigns to an environment variable ($env: scope)',
    };
  }
  if (cmdletInCommandPosition(command, ENV_WRITE_CMDLETS)) {
    return {
      blocked: true,
      reason: 'PowerShell modifies environment variables via an env: write cmdlet',
    };
  }
  return PS_PASS;
}

/**
 * Code-execution / module-load / WMI-spawn cmdlets (consumes CODE_EXEC_CMDLETS,
 * MODULE_LOADING_CMDLETS, WMI_CIM_CMDLETS). Any such cmdlet in command position
 * compiles/loads code or spawns a process and is surfaced for approval.
 */
export function detectDangerousPsCmdlets(command: string): PsDetection {
  const codeExec = cmdletInCommandPosition(command, CODE_EXEC_CMDLETS);
  if (codeExec) {
    return { blocked: true, reason: 'PowerShell Add-Type compiles and loads .NET code at runtime' };
  }
  if (cmdletInCommandPosition(command, MODULE_LOADING_CMDLETS)) {
    return {
      blocked: true,
      reason: "PowerShell loads a module/script whose top-level body executes on import",
    };
  }
  if (cmdletInCommandPosition(command, WMI_CIM_CMDLETS)) {
    return {
      blocked: true,
      reason: 'PowerShell WMI/CIM method invocation can spawn arbitrary processes (Win32_Process Create)',
    };
  }
  return PS_PASS;
}

/**
 * Stop-parsing token (`--%`) detection. PowerShell's `--%` tells the parser to
 * stop interpreting everything that follows and pass the remainder verbatim to
 * the target command — defeating any structural/token analysis of the tail
 * (a documented analysis-evasion / obfuscation technique). Like
 * `-EncodedCommand`, the remainder is opaque to the security checks, so its
 * presence is surfaced for approval (fail-safe: an un-analyzable command is
 * never silently allowed).
 *
 * PowerShell's tokenizer (SpecialCharacters.IsDash) treats en-dash (U+2013),
 * em-dash (U+2014) and horizontal-bar (U+2015) as dash-equivalents, so those
 * variants are normalized to ASCII `-` before matching — mirroring Claude Code's
 * parser token check `($tok.Text -replace '[–—―]','-') -eq '--%'`.
 * The token must stand alone (bounded by start/whitespace/statement separators)
 * so substrings like `git log --%H` or `sed 's/--%/x/'` do not match.
 */
export function detectStopParsingToken(command: string): PsDetection {
  const normalized = command.replace(/[–—―]/g, '-');
  if (/(?:^|[\s;|&(){}])--%(?=$|[\s;|&(){}])/.test(normalized)) {
    return {
      blocked: true,
      reason:
        'PowerShell stop-parsing token (--%) passes everything after it verbatim to the command — opaque to security analysis',
    };
  }
  return PS_PASS;
}

// ---------------------------------------------------------------------------
// PowerShell common parameters.
//
// Mirrors Claude Code's PowerShellTool/commonParameters.ts: the switches and
// value-taking parameters that [CmdletBinding()] adds to EVERY advanced cmdlet.
// A value-param consumes the token after it, so a positional-argument scanner
// must skip those values — otherwise `Start-Job -ErrorAction Stop` would
// mistake `Stop` for a positional -FilePath. Stored lowercase with a leading
// dash; callers lowercase their input. Source: about_CommonParameters.
// ---------------------------------------------------------------------------

export const COMMON_SWITCHES: readonly string[] = ['-verbose', '-debug'];

export const COMMON_VALUE_PARAMS: readonly string[] = [
  '-erroraction',
  '-warningaction',
  '-informationaction',
  '-progressaction',
  '-errorvariable',
  '-warningvariable',
  '-informationvariable',
  '-outvariable',
  '-outbuffer',
  '-pipelinevariable',
];

export const COMMON_PARAMETERS: ReadonlySet<string> = new Set([
  ...COMMON_SWITCHES,
  ...COMMON_VALUE_PARAMS,
]);

/**
 * Script-block consumers that only ever filter/transform pipeline input — their
 * block is a predicate/selector, not arbitrary script to run. A script block
 * passed to any OTHER cmdlet is treated as potential arbitrary code. Mirrors
 * Claude's SAFE_SCRIPT_BLOCK_CMDLETS. `foreach-object` is intentionally absent:
 * its block is arbitrary script, not a predicate.
 */
export const SAFE_SCRIPT_BLOCK_CMDLETS: ReadonlySet<string> = new Set([
  'where-object',
  'sort-object',
  'select-object',
  'group-object',
  'format-table',
  'format-list',
  'format-wide',
  'format-custom',
]);

/**
 * Scheduled-task persistence cmdlets. Creating/modifying a scheduled task
 * establishes persistence that survives the session. Mirrors Claude's
 * SCHEDULED_TASK_CMDLETS; the legacy `schtasks.exe /create|/change` form is
 * handled separately by detectScheduledTask.
 */
export const SCHEDULED_TASK_CMDLETS: ReadonlySet<string> = new Set([
  'register-scheduledtask',
  'new-scheduledtask',
  'new-scheduledtaskaction',
  'set-scheduledtask',
]);

// Alt parameter-prefix characters PowerShell's tokenizer (SpecialCharacters.
// IsDash) accepts as equivalent to ASCII '-': '/', en-dash, em-dash, horizontal
// bar. Normalized to '-' before any parameter-abbreviation test so
// `Start-Job –FilePath x` (en-dash) cannot bypass the check.
const PS_ALT_PARAM_PREFIX_RE = /^[/–—―]/;

function normalizeParamToken(token: string): string {
  return token.replace(/`/g, '').replace(PS_ALT_PARAM_PREFIX_RE, '-').toLowerCase();
}

/**
 * True if `arg` is an abbreviation of `fullParam` at least as long as
 * `minPrefix` (e.g. matches `-f`, `-fi`, … `-filepath` for fullParam
 * `-filepath`, minPrefix `-f`). Tolerant of alt dash prefixes and colon-bound
 * values (`-FilePath:x`). Mirrors Claude's psExeHasParamAbbreviation.
 */
function argMatchesParam(arg: string, fullParam: string, minPrefix: string): boolean {
  const name = normalizeParamToken(arg).split(':')[0]!;
  return (
    name.startsWith('-') &&
    name.length >= minPrefix.length &&
    fullParam.startsWith(name)
  );
}

/**
 * True if the token list contains a non-dash positional string argument, after
 * accounting for common value-parameters (which consume the following token).
 * Over-flagging any positional string is fail-safe — the reference does the same
 * for -FilePath / -MemberName positional binding.
 */
function hasPositionalStringArg(args: string[]): boolean {
  for (let i = 0; i < args.length; i++) {
    const norm = normalizeParamToken(args[i]!);
    // A script-block argument (`{ … }`) selects the ScriptBlock parameter-set,
    // not -FilePath/-MemberName, so a positional path/member cannot bind. Stop
    // scanning — the script-block hazard is covered by detectScriptBlockInjection.
    if (args[i]!.startsWith('{')) {
      return false;
    }
    if (norm.startsWith('-')) {
      const name = norm.split(':')[0]!;
      if (
        !norm.includes(':') &&
        COMMON_VALUE_PARAMS.some((p) => name.length >= 3 && p.startsWith(name))
      ) {
        i++; // value-param consumes its following value
      }
      continue;
    }
    return true;
  }
  return false;
}

/**
 * `-FilePath`/positional script execution on the FILEPATH_EXECUTION_CMDLETS
 * (Invoke-Command, Start-Job, …). A `-FilePath`/`-LiteralPath` flag OR any
 * positional string arg (which binds to -FilePath via parameter-set resolution)
 * executes an arbitrary script file — the same risk as Invoke-Expression.
 * Mirrors Claude's checkDangerousFilePathExecution.
 */
export function detectFilePathExecution(command: string): PsDetection {
  for (const name of FILEPATH_EXECUTION_CMDLETS) {
    const re = new RegExp(PS_CMD_POS_PREFIX + escapeRegExp(name) + String.raw`\b`, 'gi');
    const args = argTokensAfter(command, re);
    if (args === undefined) {
      continue;
    }
    if (
      args.some(
        (a) =>
          argMatchesParam(a, '-filepath', '-f') ||
          argMatchesParam(a, '-literalpath', '-l'),
      ) ||
      hasPositionalStringArg(args)
    ) {
      return {
        blocked: true,
        reason: `PowerShell ${name} executes an arbitrary script file (-FilePath / positional path binding)`,
      };
    }
  }
  return PS_PASS;
}

/**
 * `ForEach-Object -MemberName` (or a positional member name) invokes a method by
 * string name on every piped object — e.g. `Get-Process | ForEach-Object Kill`
 * — with no script-block or `.Method()` syntax for other checks to catch.
 * Aliases `%` / `foreach` resolve to foreach-object. Mirrors Claude's
 * checkForEachMemberName.
 */
export function detectForEachMemberName(command: string): PsDetection {
  // `%` is a non-word char so a trailing `\b` would fail before a space; match
  // it without the word boundary while keeping `\b` for the word-form names.
  const re = new RegExp(
    PS_CMD_POS_PREFIX + String.raw`(?:(?:foreach-object|foreach)\b|%)`,
    'gi',
  );
  const args = argTokensAfter(command, re);
  if (args === undefined) {
    return PS_PASS;
  }
  if (args.some((a) => argMatchesParam(a, '-membername', '-m')) || hasPositionalStringArg(args)) {
    return {
      blocked: true,
      reason:
        'PowerShell ForEach-Object invokes a method by name (-MemberName / positional) which cannot be validated',
    };
  }
  return PS_PASS;
}

/**
 * Scheduled-task persistence (mirrors Claude's checkScheduledTask): a
 * SCHEDULED_TASK_CMDLETS cmdlet in command position, or legacy
 * `schtasks(.exe) /create|/change` (both `-`/`/` delimiter forms).
 */
export function detectScheduledTask(command: string): PsDetection {
  if (cmdletInCommandPosition(command, SCHEDULED_TASK_CMDLETS)) {
    return {
      blocked: true,
      reason: 'PowerShell creates or modifies a scheduled task (persistence primitive)',
    };
  }
  const schtasks = new RegExp(
    PS_CMD_POS_PREFIX + String.raw`schtasks(?:\.exe)?\b`,
    'i',
  );
  if (schtasks.test(command) && /[-/](?:create|change)\b/i.test(command)) {
    return {
      blocked: true,
      reason: 'schtasks create/change modifies scheduled tasks (persistence primitive)',
    };
  }
  return PS_PASS;
}

/**
 * `Invoke-Item` / `ii` opens a path with the default shell handler
 * (ShellExecute); on an executable it runs arbitrary code. Mirrors Claude's
 * checkInvokeItem.
 */
export function detectInvokeItem(command: string): PsDetection {
  if (cmdletInCommandPosition(command, ['invoke-item', 'ii'])) {
    return {
      blocked: true,
      reason:
        'PowerShell Invoke-Item opens a file with its default handler (ShellExecute) — runs arbitrary code on executables',
    };
  }
  return PS_PASS;
}

/**
 * Standalone `Invoke-Expression` eval (mirrors Claude's checkInvokeExpression):
 * `Invoke-Expression` runs an arbitrary string as PowerShell code — the eval
 * primitive. The existing download-cradle / base64 checks in
 * detectPowerShellExecution only surface it when *combined* with a downloader or
 * `[Convert]::FromBase64String`; a bare `Invoke-Expression $payload` (or
 * `$code | Invoke-Expression`) is equally arbitrary code execution and is
 * surfaced here regardless of what feeds it.
 *
 * SECURITY/POSIX-SAFETY: only the unambiguous hyphenated cmdlet name
 * `invoke-expression` is matched. The `iex` alias is intentionally NOT flagged
 * here because `iex` is also the Elixir interactive-shell binary (`iex -S mix`)
 * and detectPowerShellExecution runs over POSIX-shell commands too — flagging
 * bare `iex` would false-positive on Linux. Same no-POSIX-collision rationale as
 * the `sv`/`sc`/`rm` omissions in ALIAS_HIJACK_CMDLETS / ENV_WRITE_CMDLETS. The
 * `iex` alias is still surfaced in the download-cradle and base64 combinations
 * (where the surrounding PowerShell signals disambiguate it from Elixir).
 */
export function detectInvokeExpression(command: string): PsDetection {
  if (cmdletInCommandPosition(command, ['invoke-expression'])) {
    return {
      blocked: true,
      reason:
        'PowerShell Invoke-Expression runs an arbitrary string as code (eval) — cannot be statically validated',
    };
  }
  return PS_PASS;
}

/**
 * Script-block injection (mirrors Claude's checkScriptBlockInjection, denylist
 * half): when a `{ … }` script block is present as an argument, any
 * DANGEROUS_SCRIPT_BLOCK_CMDLETS cmdlet (Invoke-Command, Start-Job, Register-*
 * event, New/Enter-PSSession, Invoke-Expression) in command position executes
 * that block as arbitrary code. The reference's allowlist "all commands safe"
 * second pass is intentionally omitted here: detectPowerShellExecution runs over
 * EVERY command (not a PowerShell-only AST like Claude), so an allowlist default
 * would over-flag ordinary POSIX commands that merely contain `{ … }` brace
 * groups. The denylist keys on PowerShell-only cmdlet names, so it cannot
 * misfire on bash.
 */
export function detectScriptBlockInjection(command: string): PsDetection {
  if (!/\{[^}]*\}/.test(command)) {
    return PS_PASS;
  }
  const hit = cmdletInCommandPosition(command, DANGEROUS_SCRIPT_BLOCK_CMDLETS);
  if (hit) {
    return {
      blocked: true,
      reason:
        'PowerShell passes a script block to a cmdlet that executes it as arbitrary code',
    };
  }
  return PS_PASS;
}

/**
 * Dynamic command-name invocation (mirrors Claude's checkDynamicCommandName):
 * the call operator `&` applied to a runtime expression rather than a literal
 * command name — `& ('i'+'ex')`, `& ('iex','x')[0]`, `& ${function:Invoke-Expression}`,
 * `& @(...)`. The command name is computed at runtime and cannot be statically
 * validated, so it is surfaced for approval. Only the PowerShell-distinctive
 * forms (`& (` quoted/concat expr, `& ${…}`, `& @(`) are matched so the check
 * cannot misfire on a POSIX background `cmd &` + subshell.
 */
export function detectDynamicCommandName(command: string): PsDetection {
  if (/&\s*(?:\$\{|\(\s*['"]|@\()/.test(command)) {
    return {
      blocked: true,
      reason:
        'PowerShell invokes a command whose name is a dynamic expression (call operator &) which cannot be statically validated',
    };
  }
  return PS_PASS;
}

/**
 * Aggregate of the PowerShell dangerous-pattern detectors added on top of the
 * download-cradle / encoded-command / CLM-cast checks already in
 * detectPowerShellExecution. Runs in a fixed order and returns the first block.
 * The input must already be Unicode-stripped / alt-dash-normalized by the caller
 * (detectPowerShellExecution does this), mirroring Claude's ordered validator
 * chain — a parse/analysis that finds no explicit block falls through to the
 * caller's final decision (never a silent allow of an unhandled hazard).
 */
export function detectDangerousPowerShellCommand(command: string): PsDetection {
  const checks = [
    detectUnsafeNewObject,
    detectStartProcessElevation,
    detectRuntimeStateHijack,
    detectEnvVarManipulation,
    detectDangerousPsCmdlets,
    detectStopParsingToken,
    detectDynamicCommandName,
    detectInvokeExpression,
    detectScriptBlockInjection,
    detectFilePathExecution,
    detectForEachMemberName,
    detectScheduledTask,
    detectInvokeItem,
  ];
  for (const check of checks) {
    const r = check(command);
    if (r.blocked) {
      return r;
    }
  }
  return PS_PASS;
}

// ---------------------------------------------------------------------------
// PowerShell read-only cmdlet allowlist (auto-allow).
//
// Mirrors Claude Code's PowerShellTool/readOnlyValidation.ts CMDLET_ALLOWLIST /
// isReadOnlyCommand: the Windows counterpart of the POSIX read-only SAFE_COMMANDS
// set in permissions.ts. A command composed solely of these side-effect-free
// cmdlets (Get-ChildItem, Get-Content, Select-String, …) is auto-allowed in
// `ask` mode so a user is not prompted for a plain read on Windows (where
// PowerShell is AMI's default shell).
//
// Only the hyphenated Verb-Noun cmdlet names are listed — never a bare alias
// (`cat`, `ls`, `sort`, `echo`, `cd`): those collide with POSIX commands already
// classified by SAFE_COMMANDS, and resolving them here could auto-allow a bash
// command that the bash classifier would gate. Hyphenated cmdlet names cannot
// collide with a POSIX command, so this set is PowerShell-only by construction.
//
// Every listed cmdlet is a pure reader/formatter/transformer with no filesystem
// or state write: Get-* (read), Format-*/Out-*/ConvertTo-*/ConvertFrom-*/
// Join-*/Split-*/Measure-*/Group-*/Sort-*/Select-*/Compare-* (pipeline shaping,
// output only), Test-Path/Resolve-Path/Convert-Path (path probes), and the cwd
// movers (Set/Push/Pop-Location — a bare cwd change is inert; the cd+write and
// multi-cd desync guards in the classifier run BEFORE this check). Writers
// (Set-/Add-/Out-File/Export-/Tee-Object/New-/Remove-Item, Invoke-*) are
// deliberately absent.
// ---------------------------------------------------------------------------

/**
 * Per-cmdlet read-only flag configuration. Mirrors Claude Code's
 * readOnlyValidation.ts CMDLET_ALLOWLIST `CommandConfig`:
 *   - `safeFlags` — the ONLY flags the cmdlet may carry and still be a pure read
 *     (lowercase, leading dash — the runtime comparison form). A flag outside
 *     this set ∪ {@link COMMON_PARAMETERS} makes the command non-read-only (→ a
 *     prompt). Side-effecting / remote / external-entity flags (-ComputerName,
 *     -CimSession, Get-WinEvent -FilterXml/-FilterHashtable) are deliberately
 *     omitted so they are NOT auto-allowed.
 *   - `allowAllFlags` — the cmdlet's entire flag surface is display/selection
 *     only (Format-*, Select-Object, Get-ComputerInfo, …); every flag is
 *     accepted. The dangerous-arg VALUE vectors those cmdlets have in Claude
 *     (calculated-property hashtables, `$env:` leaks, script blocks) are already
 *     rejected wholesale by {@link PS_READONLY_DISQUALIFY_RE} — strictly more
 *     conservative than Claude's per-arg `argLeaksValue` element-type whitelist.
 *
 * COMMON_PARAMETERS (-Verbose/-ErrorAction/-ErrorVariable/…) are merged in at
 * check time, so they are never restated here.
 */
export type PowerShellReadOnlyConfig = {
  /** Flags this read-only cmdlet accepts (lowercase, leading dash). */
  safeFlags?: readonly string[];
  /** When true, every flag is accepted (whole flag surface is read-only). */
  allowAllFlags?: boolean;
};

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
  // NOTE (intentional AMI superset divergence): Claude removed Get-Help from its
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

export const POWERSHELL_READONLY_CMDLETS: ReadonlySet<string> = new Set(
  Object.keys(POWERSHELL_READONLY_CONFIG),
);

// Constructs that make a command un-analyzable as a pure read, so their presence
// fails the read-only classification closed (never auto-allow): variables /
// subexpressions / env leaks (`$`), script blocks (`{ }`), grouping /
// subexpressions (`(`), splatting / arrays / hashtables (`@`), the call operator
// (`&`), backtick escapes, and output redirection (`>` — a file write). Mirrors
// readOnlyValidation.ts's fail-closed posture (argLeaksValue + the
// SAFE_PATH_ELEMENT_TYPES whitelist) without needing the native AST: rejecting
// these wholesale is strictly more conservative than the per-arg element-type
// whitelist and keeps this a cheap synchronous check for the classifier.
const PS_READONLY_DISQUALIFY_RE = /[$`{}()@&>]/;

/**
 * Per-cmdlet safe-flag gate. Returns true when EVERY flag token in `segment`
 * (after the command name) is in the cmdlet's safeFlags set ∪
 * {@link COMMON_PARAMETERS}, or the cmdlet declares `allowAllFlags`. A flag
 * outside that surface — a side-effecting / remote / external-entity flag such
 * as `-ComputerName`, `-CimSession`, or Get-WinEvent `-FilterXml` — returns
 * false so the command is NOT auto-allowed. Mirrors Claude Code's
 * readOnlyValidation.ts CMDLET_ALLOWLIST `safeFlags`/`allowAllFlags` check +
 * COMMON_PARAMETERS merge; positional (non-flag) values are not gated here
 * (dangerous value forms are rejected wholesale by PS_READONLY_DISQUALIFY_RE).
 *
 * `cmdlet` is the already-normalized (lowercase, module/.exe-stripped) name the
 * caller resolved; it is guaranteed present in {@link POWERSHELL_READONLY_CONFIG}.
 */
function powerShellFlagsAllowed(segment: string, cmdlet: string): boolean {
  const config = POWERSHELL_READONLY_CONFIG[cmdlet];
  if (config === undefined) {
    return false;
  }
  if (config.allowAllFlags === true) {
    return true;
  }
  const allowed = new Set<string>([...(config.safeFlags ?? []), ...COMMON_PARAMETERS]);
  const tokens = segment.trim().match(/\S+/g) ?? [];
  // tokens[0] is the command name; validate every flag token after it.
  for (let i = 1; i < tokens.length; i++) {
    const norm = normalizeParamToken(tokens[i]!);
    if (!norm.startsWith('-')) {
      continue; // positional value, not a flag
    }
    const name = norm.split(':')[0]!; // drop a colon-bound value (-Path:x)
    if (!allowed.has(name)) {
      return false;
    }
  }
  return true;
}

/**
 * True if `command` is a PowerShell command composed solely of allowlisted
 * read-only cmdlets with no dangerous construct — safe to auto-allow in `ask`
 * mode, the Windows counterpart of the POSIX read-only SAFE_COMMANDS set.
 *
 * Fail-closed: it returns true ONLY when EVERY pipeline segment's command-position
 * name is in {@link POWERSHELL_READONLY_CMDLETS}, the command carries none of the
 * {@link PS_READONLY_DISQUALIFY_RE} constructs, and the defense-in-depth textual
 * detectors ({@link detectDangerousPowerShellCommand} / {@link detectUnsafeTypeCast})
 * find no hazard. Anything it cannot prove read-only returns false (→ the caller
 * falls through to a prompt). Mirrors Claude's isReadOnlyCommand contract.
 */
export function isReadOnlyPowerShellCommand(command: string): boolean {
  const trimmed = command.trim();
  if (trimmed.length === 0) {
    return false;
  }
  // Reject any un-analyzable construct up front (fail-closed).
  if (PS_READONLY_DISQUALIFY_RE.test(trimmed)) {
    return false;
  }
  // Statement separators (`;`) and newlines are not auto-allowed — one prompt for
  // a multi-statement command is the conservative choice.
  if (/[;\n\r]/.test(trimmed)) {
    return false;
  }
  // Every pipeline segment must START with an allowlisted read-only cmdlet.
  const segments = trimmed.split('|');
  for (const segment of segments) {
    const tokens = segment.trim().match(/\S+/g);
    if (tokens === undefined || tokens === null || tokens.length === 0) {
      return false; // empty segment (e.g. `Get-Process ||` or leading `|`)
    }
    const first = tokens[0]!;
    // A segment that leads with a flag/switch is malformed for a read classify.
    if (first.startsWith('-') || first.startsWith('/')) {
      return false;
    }
    // Strip an optional module qualifier (`Microsoft.PowerShell.Management\Get-…`)
    // and a Windows executable extension, then normalize case.
    const name = (first.split(/[\\/]/).pop() ?? first)
      .toLowerCase()
      .replace(/\.(exe|cmd|bat|com|ps1)$/, '');
    if (!POWERSHELL_READONLY_CMDLETS.has(name)) {
      return false;
    }
    // Per-cmdlet safe-flag gate: an allowlisted read cmdlet carrying a flag
    // outside its read-only surface (a side-effecting / remote / external-entity
    // flag like `-ComputerName`, `-CimSession`, or Get-WinEvent `-FilterXml`)
    // must not auto-allow — fall through to a prompt. COMMON_PARAMETERS
    // (-Verbose/-ErrorAction/…) are merged in automatically.
    if (!powerShellFlagsAllowed(segment, name)) {
      return false;
    }
  }
  // Defense in depth: never auto-allow something the hazard detectors flag.
  if (detectDangerousPowerShellCommand(trimmed).blocked) {
    return false;
  }
  if (detectUnsafeTypeCast(trimmed).blocked) {
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Per-cmdlet path-parameter configuration + path extraction.
//
// Mirrors Claude Code's PowerShellTool/pathValidation.ts CMDLET_PATH_CONFIG +
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

/**
 * Per-cmdlet path-parameter table. Keyed by the canonical (alias-resolved,
 * lowercase) cmdlet name — look up with {@link resolveToCanonical}.
 */
export const CMDLET_PATH_CONFIG: Record<string, CmdletPathConfig> = {
  // ─── Write/create operations ──────────────────────────────────────────────
  'set-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction', '-nonewline', '-asbytestream'],
    knownValueParams: ['-value', '-filter', '-include', '-exclude', '-credential', '-encoding', '-stream'],
  },
  'add-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction', '-nonewline', '-asbytestream'],
    knownValueParams: ['-value', '-filter', '-include', '-exclude', '-credential', '-encoding', '-stream'],
  },
  'remove-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-recurse', '-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'clear-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'out-file': {
    operationType: 'write',
    // -Path is PowerShell's documented ALIAS for -FilePath.
    pathParams: ['-filepath', '-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-nonewline', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-encoding', '-width'],
  },
  'tee-object': {
    operationType: 'write',
    pathParams: ['-filepath', '-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append'],
    knownValueParams: ['-inputobject', '-variable', '-encoding'],
  },
  'export-csv': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-notypeinformation', '-includetypeinformation', '-useculture', '-noheader', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-delimiter', '-encoding', '-quotefields', '-usequotes'],
  },
  'export-clixml': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-noclobber', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-depth', '-encoding'],
  },
  'new-item': {
    operationType: 'write',
    // -Path is position 0. -Name (leaf-only) is resolved RELATIVE TO -Path, so a
    // non-leaf value forces an ask (it would resolve against the wrong base).
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    leafOnlyPathParams: ['-name'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-itemtype', '-value', '-credential', '-type'],
  },
  'copy-item': {
    operationType: 'write',
    // -Path (source) + -Destination (dest) both extracted + validated as write.
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destination'],
    knownSwitches: ['-container', '-force', '-passthru', '-recurse', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-fromsession', '-tosession'],
  },
  'move-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destination'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential'],
  },
  'rename-item': {
    operationType: 'write',
    // -NewName is leaf-only per docs and Rename-Item rejects `..`, so it is a
    // knownValueParam (not leafOnly) — unlike New-Item -Name which accepts it.
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-newname', '-credential', '-filter', '-include', '-exclude'],
  },
  'set-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-value', '-credential', '-filter', '-include', '-exclude'],
  },
  'invoke-webrequest': {
    operationType: 'write',
    pathParams: ['-outfile', '-infile'],
    positionalSkip: 1, // positional-0 is -Uri (URL), not a filesystem path
    optionalWrite: true, // only writes with -OutFile; bare iwr is pipeline-only
    knownSwitches: ['-allowinsecureredirect', '-allowunencryptedauthentication', '-disablekeepalive', '-nobodyprogress', '-passthru', '-preservefileauthorizationmetadata', '-resume', '-skipcertificatecheck', '-skipheadervalidation', '-skiphttperrorcheck', '-usebasicparsing', '-usedefaultcredentials'],
    knownValueParams: ['-uri', '-method', '-body', '-contenttype', '-headers', '-maximumredirection', '-maximumretrycount', '-proxy', '-proxycredential', '-retryintervalsec', '-sessionvariable', '-timeoutsec', '-token', '-transferencoding', '-useragent', '-websession', '-credential', '-authentication', '-certificate', '-certificatethumbprint', '-form', '-httpversion'],
  },
  'invoke-restmethod': {
    operationType: 'write',
    pathParams: ['-outfile', '-infile'],
    positionalSkip: 1,
    optionalWrite: true,
    knownSwitches: ['-allowinsecureredirect', '-allowunencryptedauthentication', '-disablekeepalive', '-followrellink', '-nobodyprogress', '-passthru', '-preservefileauthorizationmetadata', '-resume', '-skipcertificatecheck', '-skipheadervalidation', '-skiphttperrorcheck', '-usebasicparsing', '-usedefaultcredentials'],
    knownValueParams: ['-uri', '-method', '-body', '-contenttype', '-headers', '-maximumfollowrellink', '-maximumredirection', '-maximumretrycount', '-proxy', '-proxycredential', '-responseheaderstvariable', '-retryintervalsec', '-sessionvariable', '-statuscodevariable', '-timeoutsec', '-token', '-transferencoding', '-useragent', '-websession', '-credential', '-authentication', '-certificate', '-certificatethumbprint', '-form', '-httpversion'],
  },
  'expand-archive': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destinationpath'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm'],
    knownValueParams: [],
  },
  'compress-archive': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destinationpath'],
    knownSwitches: ['-force', '-update', '-passthru', '-whatif', '-confirm'],
    knownValueParams: ['-compressionlevel'],
  },
  'set-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-value', '-type', '-filter', '-include', '-exclude', '-credential', '-inputobject'],
  },
  'new-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-value', '-propertytype', '-type', '-filter', '-include', '-exclude', '-credential'],
  },
  'remove-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-filter', '-include', '-exclude', '-credential'],
  },
  'clear-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential'],
  },
  'export-alias': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-passthru', '-whatif', '-confirm'],
    knownValueParams: ['-name', '-description', '-scope', '-as'],
  },
  // ─── Read operations ──────────────────────────────────────────────────────
  'get-content': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-usetransaction', '-wait', '-raw', '-asbytestream'],
    knownValueParams: ['-readcount', '-totalcount', '-tail', '-first', '-head', '-last', '-filter', '-include', '-exclude', '-credential', '-delimiter', '-encoding', '-stream'],
  },
  'get-childitem': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-recurse', '-force', '-name', '-usetransaction', '-followsymlink', '-directory', '-file', '-hidden', '-readonly', '-system'],
    knownValueParams: ['-filter', '-include', '-exclude', '-depth', '-attributes', '-credential'],
  },
  'get-item': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'get-filehash': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: [],
    knownValueParams: ['-algorithm', '-inputstream'],
  },
  'test-path': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-isvalid', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-pathtype', '-credential', '-olderthan', '-newerthan'],
  },
  'select-string': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-simplematch', '-casesensitive', '-quiet', '-list', '-notmatch', '-allmatches', '-noemphasis', '-raw'],
    knownValueParams: ['-inputobject', '-pattern', '-include', '-exclude', '-encoding', '-context', '-culture'],
  },
};

/**
 * Element types safe to treat as a static filesystem path. Anything else
 * (Variable, SubExpression, ExpandableString, ScriptBlock, …) is a
 * runtime-computed value whose path cannot be statically validated — it forces
 * an ask. Mirrors Claude's SAFE_PATH_ELEMENT_TYPES.
 */
const SAFE_PATH_ELEMENT_TYPES: ReadonlySet<CommandElementType> = new Set<CommandElementType>(['StringConstant', 'Parameter']);

/**
 * True if a lowercase parameter name (leading dash) matches any entry in
 * `paramList`, honoring PowerShell's unambiguous-prefix matching (`-Lit` matches
 * `-LiteralPath`). Mirrors Claude's `matchesParam`.
 */
function matchesCmdletParam(paramLower: string, paramList: readonly string[]): boolean {
  for (const p of paramList) {
    if (p === paramLower || (paramLower.length > 1 && p.startsWith(paramLower))) {
      return true;
    }
  }
  return false;
}

/**
 * True if a colon-syntax value (`-Path:<value>`) contains expression constructs
 * (arrays, subexpressions, variables, backtick escapes) that mask the real
 * runtime path. The outer CommandParameterAst hides these from the AST walk, so
 * they are detected textually. Mirrors Claude's `hasComplexColonValue`.
 */
function hasComplexColonValue(rawValue: string): boolean {
  return (
    rawValue.includes(',') ||
    rawValue.startsWith('(') ||
    rawValue.startsWith('[') ||
    rawValue.includes('`') ||
    rawValue.includes('@(') ||
    rawValue.startsWith('@{') ||
    rawValue.includes('$')
  );
}

/**
 * Extract filesystem paths from a parsed PowerShell command element using
 * {@link CMDLET_PATH_CONFIG}. Returns the extracted `paths`, the cmdlet's
 * `operationType`, `hasUnvalidatablePathArg` (true when any unknown -Param or
 * non-static value means the invocation cannot be statically validated → the
 * caller must ask), and the resolved `optionalWrite` knob.
 *
 * Mirrors Claude Code's PowerShellTool/pathValidation.ts `extractPathsFromCommand`,
 * including the three config knobs (leafOnlyPathParams / positionalSkip /
 * optionalWrite) with their defaults (none / 0 / false).
 */
export function extractPowerShellCommandPaths(cmd: ParsedCommandElement): {
  paths: string[];
  operationType: FileOperationType;
  hasUnvalidatablePathArg: boolean;
  optionalWrite: boolean;
} {
  const canonical = resolveToCanonical(cmd.name);
  const config = CMDLET_PATH_CONFIG[canonical];

  if (config === undefined) {
    return { paths: [], operationType: 'read', hasUnvalidatablePathArg: false, optionalWrite: false };
  }

  const switchParams = [...config.knownSwitches, ...COMMON_SWITCHES];
  const valueParams = [...config.knownValueParams, ...COMMON_VALUE_PARAMS];

  const paths: string[] = [];
  const args = cmd.args;
  const elementTypes = cmd.elementTypes;
  let hasUnvalidatablePathArg = false;
  let positionalsSeen = 0;
  const positionalSkip = config.positionalSkip ?? 0;

  const checkArgElementType = (argIdx: number): void => {
    if (elementTypes === undefined) return;
    const et = elementTypes[argIdx + 1];
    if (et !== undefined && !SAFE_PATH_ELEMENT_TYPES.has(et)) {
      hasUnvalidatablePathArg = true;
    }
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === undefined) continue;

    const argElementType = elementTypes ? elementTypes[i + 1] : undefined;
    if (isPowerShellParameter(arg, argElementType)) {
      // Normalize Unicode dash to ASCII `-` and split off any colon-bound value.
      const normalized = '-' + arg.slice(1);
      const colonIdx = normalized.indexOf(':', 1);
      const paramName = colonIdx > 0 ? normalized.substring(0, colonIdx) : normalized;
      const paramLower = paramName.toLowerCase();

      if (matchesCmdletParam(paramLower, config.pathParams)) {
        let value: string | undefined;
        if (colonIdx > 0) {
          const rawValue = arg.substring(colonIdx + 1);
          if (hasComplexColonValue(rawValue)) {
            hasUnvalidatablePathArg = true;
          } else {
            value = rawValue;
          }
        } else {
          const nextVal = args[i + 1];
          const nextType = elementTypes ? elementTypes[i + 2] : undefined;
          if (nextVal !== undefined && !isPowerShellParameter(nextVal, nextType)) {
            value = nextVal;
            checkArgElementType(i + 1);
            i++;
          }
        }
        if (value !== undefined && value.length > 0) {
          paths.push(value);
        }
      } else if (config.leafOnlyPathParams && matchesCmdletParam(paramLower, config.leafOnlyPathParams)) {
        let value: string | undefined;
        if (colonIdx > 0) {
          const rawValue = arg.substring(colonIdx + 1);
          if (hasComplexColonValue(rawValue)) {
            hasUnvalidatablePathArg = true;
          } else {
            value = rawValue;
          }
        } else {
          const nextVal = args[i + 1];
          const nextType = elementTypes ? elementTypes[i + 2] : undefined;
          if (nextVal !== undefined && !isPowerShellParameter(nextVal, nextType)) {
            value = nextVal;
            checkArgElementType(i + 1);
            i++;
          }
        }
        if (value !== undefined) {
          if (value.includes('/') || value.includes('\\') || value === '.' || value === '..') {
            // Non-leaf: cannot resolve correctly without joining against -Path.
            hasUnvalidatablePathArg = true;
          } else {
            paths.push(value);
          }
        }
      } else if (matchesCmdletParam(paramLower, switchParams)) {
        // Known switch — takes no value; do not consume the next arg.
      } else if (matchesCmdletParam(paramLower, valueParams)) {
        if (colonIdx > 0) {
          const rawValue = arg.substring(colonIdx + 1);
          if (hasComplexColonValue(rawValue)) {
            hasUnvalidatablePathArg = true;
          }
        } else {
          const nextArg = args[i + 1];
          const nextArgType = elementTypes ? elementTypes[i + 2] : undefined;
          if (nextArg !== undefined && !isPowerShellParameter(nextArg, nextArgType)) {
            checkArgElementType(i + 1);
            i++;
          }
        }
      } else {
        // Unknown parameter — we do not understand this invocation. Fail safe.
        hasUnvalidatablePathArg = true;
        // Defense-in-depth: a colon-bound value might still be a path; extract it
        // so deny-rule matching still runs.
        if (colonIdx > 0) {
          const rawValue = arg.substring(colonIdx + 1);
          if (!hasComplexColonValue(rawValue)) {
            paths.push(rawValue);
          }
        }
      }
      continue;
    }

    // Positional argument. Skip the configured number of leading non-path
    // positionals (e.g. iwr's -Uri URL) before extracting the rest as paths.
    if (positionalsSeen < positionalSkip) {
      positionalsSeen++;
      continue;
    }
    positionalsSeen++;
    checkArgElementType(i);
    paths.push(arg);
  }

  return {
    paths,
    operationType: config.operationType,
    hasUnvalidatablePathArg,
    optionalWrite: config.optionalWrite ?? false,
  };
}

// ---------------------------------------------------------------------------
// AST-based PowerShell security validator.
//
// Mirrors Claude Code's PowerShellTool/powershellSecurity.ts
// `powershellCommandIsSafe(command, parsed)`: the ordered validator chain that
// consumes a native-parsed `ParsedPowerShellCommand` AST (from
// powershell-parser.ts's `parsePowerShellCommand`) and, crucially, FAILS SAFE —
// a command whose AST could not be parsed (`valid === false`) returns `ask`,
// never `allow`.
//
// This complements the textual `detectDangerousPowerShellCommand` /
// `detectPowerShellExecution` guards used in the synchronous bash-security path:
// those run over EVERY shell command (no pwsh spawn) and therefore must keep the
// broad AST-only signals OFF to avoid over-flagging POSIX. When a REAL pwsh AST
// is available, this function adds the structural checks the textual path cannot
// safely do (dynamic command name via the StringConstant allowlist; subexpression
// / expandable-string / splatting / member-invocation signals; positional
// -FilePath/-MemberName binding; script-block injection triage; CLM type-literal
// allowlist; env: scope manipulation; AST-alias-aware Invoke-Expression) and
// DELEGATES the cmdlet/type-name hazards to the one canonical textual detector so
// there is no second, parallel implementation of them.
// ---------------------------------------------------------------------------

/** Result of the AST-based validator (parity with Claude's shape). */
export type PowerShellSecurityResult = {
  behavior: 'passthrough' | 'ask';
  message?: string;
};

const PS_SAFE_RESULT: PowerShellSecurityResult = { behavior: 'passthrough' };

function askResult(message: string): PowerShellSecurityResult {
  return { behavior: 'ask', message };
}

/**
 * Alt-dash-aware wrapper around {@link commandHasArgAbbreviation}. PowerShell's
 * tokenizer treats `/`, en-dash, em-dash and horizontal-bar as `-` equivalents,
 * so `Start-Job –FilePath x` (en-dash) must match `-filepath`. Reuses the shared
 * abbreviation helper (no re-implementation), retrying once with the alternate
 * prefixes normalized to `-`. Mirrors Claude's psExeHasParamAbbreviation.
 */
function psParamAbbrevAltAware(
  cmd: ParsedCommandElement,
  fullParam: string,
  minPrefix: string,
): boolean {
  if (commandHasArgAbbreviation(cmd, fullParam, minPrefix)) {
    return true;
  }
  const normalized: ParsedCommandElement = {
    ...cmd,
    args: cmd.args.map((a) =>
      a.length > 0 && PS_ALT_PARAM_PREFIX_RE.test(a[0]!) ? '-' + a.slice(1) : a,
    ),
  };
  return commandHasArgAbbreviation(normalized, fullParam, minPrefix);
}

/**
 * Validate a natively-parsed PowerShell command. Returns `ask` when ANY check
 * matches (or when the parse failed), `passthrough` only when the whole ordered
 * chain finds no hazard. The first `ask` short-circuits, matching the reference's
 * validator ordering.
 *
 * @param command - the original command text (kept for API parity / messages).
 * @param parsed - the native AST from `parsePowerShellCommand`.
 */
export function powershellCommandIsSafe(
  command: string,
  parsed: ParsedPowerShellCommand,
): PowerShellSecurityResult {
  // Fail-safe boundary: an un-parseable/un-analyzable command is never allowed.
  // Surface the parser's structured diagnostic (errorId + human-readable message
  // — CommandTooLong / EmptyOutput / InvalidJson / PwshError / PwshTimeout /
  // NoPowerShell / a syntax ErrorId, …) so the degraded-to-ask reason is
  // actionable instead of opaque. The parser already produces these diagnostics
  // (powershell-parser.ts makeInvalidResult); previously they were discarded
  // here. Mirrors Claude Code's parse-failure reason surfacing
  // (powershellPermissions.ts: "…cannot be parsed: <message>").
  if (!parsed.valid) {
    const diag = parsed.errors[0];
    const detail =
      diag !== undefined ? ` (${diag.errorId}: ${diag.message})` : '';
    return askResult(`Could not parse command for security analysis${detail}`);
  }

  const allCommands = getAllCommands(parsed);
  const flags = deriveSecurityFlags(parsed);

  // Invoke-Expression (eval). AST context means the `iex` alias CAN be flagged
  // here (hasCommandNamed resolves it) — unlike the POSIX-shared textual path,
  // which omits `iex` to avoid colliding with the Elixir shell.
  if (hasCommandNamed(parsed, 'Invoke-Expression')) {
    return askResult(
      'Command uses Invoke-Expression which can execute arbitrary code',
    );
  }

  // Dynamic command name: a legitimate command name is ALWAYS a StringConstant.
  // Any other element type in name position is a runtime-computed name
  // (`& ('i'+'ex')`, `& ('iex','x')[0]`, `& ${function:…}`). Allowlist
  // StringConstant rather than denylisting dynamic types. The `!== undefined`
  // guard keeps fail-open when elementTypes detail is unavailable.
  for (const cmd of allCommands) {
    if (cmd.elementType !== 'CommandAst') {
      continue;
    }
    const nameElementType = cmd.elementTypes?.[0];
    if (nameElementType !== undefined && nameElementType !== 'StringConstant') {
      return askResult(
        'Command name is a dynamic expression which cannot be statically validated',
      );
    }
  }

  // Encoded command: a nested PowerShell executable carrying -EncodedCommand (or
  // any `-e…` abbreviation, alt-dash tolerant) runs a base64 script that is
  // opaque to analysis. Checked BEFORE the nested-process check so the more
  // specific "encoded parameters" message wins (mirrors the reference order).
  for (const cmd of allCommands) {
    if (
      isPowerShellExecutable(cmd.name) &&
      psParamAbbrevAltAware(cmd, '-encodedcommand', '-e')
    ) {
      return askResult('Command uses encoded parameters which obscure intent');
    }
  }

  // Nested PowerShell process: ANY powershell executable in command position
  // (`pwsh -Command …`, `pwsh -File x.ps1`, or a bare `pwsh` receiving stdin)
  // spawns a child shell whose script cannot be statically validated. Same
  // reasoning as checkStartProcess's nested-target vector.
  for (const cmd of allCommands) {
    if (isPowerShellExecutable(cmd.name)) {
      return askResult(
        'Command spawns a nested PowerShell process which cannot be validated',
      );
    }
  }

  // (Download cradles — a downloader piped/split to Invoke-Expression — are
  // already asked by the Invoke-Expression check above, which alias-resolves
  // `iex` and scans every statement; a dedicated cradle check here would be
  // unreachable, so it is intentionally not duplicated.)

  // LOLBAS download utilities: the download operation itself (no IEX required).
  // Start-BitsTransfer is purpose-built for transfer; certutil / bitsadmin are
  // only flagged with the -urlcache / /transfer download arguments (both have
  // legitimate non-download uses).
  for (const cmd of allCommands) {
    const lower = cmd.name.toLowerCase();
    if (lower === 'start-bitstransfer') {
      return askResult('Command downloads files via BITS transfer');
    }
    if (lower === 'certutil' || lower === 'certutil.exe') {
      if (
        cmd.args.some((a) => {
          const la = a.toLowerCase();
          return la === '-urlcache' || la === '/urlcache';
        })
      ) {
        return askResult('Command uses certutil to download from a URL');
      }
    }
    if (lower === 'bitsadmin' || lower === 'bitsadmin.exe') {
      if (cmd.args.some((a) => a.toLowerCase() === '/transfer')) {
        return askResult('Command downloads files via BITS transfer');
      }
    }
  }

  // Script-block injection triage: a denylisted cmdlet carrying a script block
  // executes it as arbitrary code; otherwise, all script-block consumers must be
  // in the safe filter/output allowlist (alias-resolved), else ask.
  if (flags.hasScriptBlocks) {
    for (const cmd of allCommands) {
      if (DANGEROUS_SCRIPT_BLOCK_CMDLETS.has(cmd.name.toLowerCase())) {
        return askResult(
          'Command contains script block with dangerous cmdlet that may execute arbitrary code',
        );
      }
    }
    const allSafe = allCommands.every((cmd) => {
      const lower = cmd.name.toLowerCase();
      if (SAFE_SCRIPT_BLOCK_CMDLETS.has(lower)) {
        return true;
      }
      const alias = COMMON_ALIASES[lower];
      return alias !== undefined && SAFE_SCRIPT_BLOCK_CMDLETS.has(alias.toLowerCase());
    });
    if (!allSafe) {
      return askResult('Command contains script block that may execute arbitrary code');
    }
  }

  // Positional / flagged -FilePath execution on FILEPATH_EXECUTION_CMDLETS.
  for (const cmd of allCommands) {
    const lower = cmd.name.toLowerCase();
    const resolved = COMMON_ALIASES[lower]?.toLowerCase() ?? lower;
    if (!FILEPATH_EXECUTION_CMDLETS.has(resolved)) {
      continue;
    }
    if (
      psParamAbbrevAltAware(cmd, '-filepath', '-f') ||
      psParamAbbrevAltAware(cmd, '-literalpath', '-l') ||
      hasPositionalStringArgAst(cmd)
    ) {
      return askResult(`${cmd.name} -FilePath executes an arbitrary script file`);
    }
  }

  // ForEach-Object -MemberName / positional member-name binding.
  for (const cmd of allCommands) {
    const lower = cmd.name.toLowerCase();
    const resolved = COMMON_ALIASES[lower]?.toLowerCase() ?? lower;
    if (resolved !== 'foreach-object') {
      continue;
    }
    if (psParamAbbrevAltAware(cmd, '-membername', '-m') || hasPositionalStringArgAst(cmd)) {
      return askResult(
        'ForEach-Object invokes a method by name (-MemberName / positional) which cannot be validated',
      );
    }
  }

  // Start-Process -Verb RunAs — structural defense-in-depth. The textual
  // detectStartProcessElevation (reached via the delegation below) matches the
  // elevation verb with a regex, but it tokenizes on whitespace, so a colon-bound
  // value padded/obscured by quoting (`-Verb:" RunAs "`) splits across tokens and
  // slips past it. When a real AST is available, the parser hands us the bound
  // value as the parameter's structural child, so we normalize that child text
  // (strip quotes/backticks/whitespace) and ask when it resolves to `runas`.
  // Mirrors Claude Code's checkStartProcess children[] layer (PR #23554).
  for (const cmd of allCommands) {
    const lower = cmd.name.toLowerCase();
    if (lower !== 'start-process' && lower !== 'saps' && lower !== 'start') {
      continue;
    }
    if (startProcessColonVerbRunAs(cmd)) {
      return askResult(
        'PowerShell Start-Process -Verb RunAs requests elevated (administrator) privileges',
      );
    }
  }

  // AST-only structural signals from deriveSecurityFlags.
  if (flags.hasSubExpressions) {
    return askResult('Command contains subexpressions $()');
  }
  if (flags.hasExpandableStrings) {
    return askResult('Command contains expandable strings with embedded expressions');
  }
  if (flags.hasSplatting) {
    return askResult('Command uses splatting (@variable)');
  }
  if (flags.hasStopParsing) {
    return askResult('Command uses stop-parsing token (--%)');
  }
  if (flags.hasMemberInvocations) {
    return askResult('Command invokes .NET methods');
  }

  // CLM type-literal allowlist over every [Type] literal found in the AST.
  for (const t of parsed.typeLiterals ?? []) {
    if (!isClmAllowedType(t)) {
      return askResult(
        `Command uses .NET type [${t}] outside the ConstrainedLanguage allowlist`,
      );
    }
  }

  // Environment-variable manipulation: gated on an env: scope reference, then an
  // env: write cmdlet or an assignment.
  const envVars = getVariablesByScope(parsed, 'env');
  if (envVars.length > 0) {
    for (const cmd of allCommands) {
      if (ENV_WRITE_CMDLETS.has(cmd.name.toLowerCase())) {
        return askResult('Command modifies environment variables');
      }
    }
    if (flags.hasAssignments) {
      return askResult('Command modifies environment variables');
    }
  }

  // Per-cmdlet path-parameter constraints (CMDLET_PATH_CONFIG). For every command
  // whose cmdlet has a path config, extract its filesystem paths. The fail-safe
  // security model asks when the invocation cannot be statically understood:
  //   1. hasUnvalidatablePathArg — an unknown -Param, a non-static (Variable /
  //      SubExpression / array) path value, or a non-leaf leaf-only value. We
  //      cannot tell which path the command touches, so we ask.
  //   2. a write cmdlet with NO extractable path and NOT optionalWrite — e.g.
  //      `Set-Content` without a target. optionalWrite cmdlets (iwr/irm without
  //      -OutFile write to the pipeline, not disk) are exempt.
  // Mirrors Claude Code's PowerShellTool/pathValidation.ts
  // checkPathConstraintsForStatement path-arg handling.
  for (const cmd of allCommands) {
    const extracted = extractPowerShellCommandPaths(cmd);
    if (extracted.hasUnvalidatablePathArg) {
      return askResult(
        `${cmd.name} has a path argument that cannot be statically validated`,
      );
    }
    if (
      extracted.operationType !== 'read' &&
      !extracted.optionalWrite &&
      extracted.paths.length === 0
    ) {
      return askResult(
        `${cmd.name} writes to the filesystem but no target path could be determined`,
      );
    }
  }

  // Delegate the cmdlet/type-name hazards (New-Object, Start-Process, runtime
  // state, module/WMI/code-exec, scheduled task, Invoke-Item, encoded/download
  // utilities, CLM casts) to the single canonical textual detector so they are
  // implemented exactly once. The original command text carries the full
  // argument syntax those checks need.
  const textual = detectDangerousPowerShellCommand(command);
  if (textual.blocked) {
    return askResult(textual.reason ?? 'Command matched a PowerShell security check');
  }
  const typeCast = detectUnsafeTypeCast(command);
  if (typeCast.blocked) {
    return askResult(typeCast.reason ?? 'Command uses an unsafe .NET type cast');
  }

  return PS_SAFE_RESULT;
}

/**
 * True if a command binds a non-dash positional StringConstant argument (which
 * parameter-set resolution would route to -FilePath / -MemberName). A script
 * block arg selects the ScriptBlock parameter set instead, so it does not count.
 * Uses the AST `elementTypes` when present; over-flagging is fail-safe. Mirrors
 * the positional-binding scan in Claude's checkDangerousFilePathExecution /
 * checkForEachMemberName.
 */
/**
 * True if a command binds `-Verb RunAs` through a colon-bound parameter whose
 * value, after stripping quotes/backticks/whitespace, normalizes to `runas`
 * (`-Verb:RunAs`, `-Verb:'RunAs'`, `-Verb:" RunAs "`, `-V`+backtick+`erb:…`).
 * Reads the parser's structural `children[]` (the CommandParameterAst.Argument of
 * each colon-bound parameter) so an elevation verb obscured by quoting/padding
 * that the whitespace-tokenizing textual regex cannot anticipate is still
 * surfaced. Fail-open when `children` is unavailable (the textual regex layer
 * still runs). Mirrors Claude Code's checkStartProcess children[] layer.
 */
function startProcessColonVerbRunAs(cmd: ParsedCommandElement): boolean {
  if (cmd.children === undefined) {
    return false;
  }
  for (let i = 0; i < cmd.args.length; i++) {
    // Strip backticks before matching the parameter name (`-V`+backtick+`erb:`).
    const argName = cmd.args[i]!.replace(/`/g, '');
    if (!/^[-–—―/]v[a-z]*:/i.test(argName)) {
      continue;
    }
    const kids = cmd.children[i];
    if (kids === undefined) {
      continue;
    }
    for (const child of kids) {
      if (child.text.replace(/['"`\s]/g, '').toLowerCase() === 'runas') {
        return true;
      }
    }
  }
  return false;
}

function hasPositionalStringArgAst(cmd: ParsedCommandElement): boolean {
  for (let i = 0; i < cmd.args.length; i++) {
    const arg = cmd.args[i];
    const argType = cmd.elementTypes?.[i + 1];
    if (argType === 'StringConstant' && arg !== undefined && !arg.startsWith('-')) {
      return true;
    }
  }
  return false;
}
