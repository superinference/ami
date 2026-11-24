import { COMMON_ALIASES, getAllCommands, hasCommandNamed, getVariablesByScope, deriveSecurityFlags, resolveToCanonical, isPowerShellParameter } from './powershell-parser';
import type {
  ParsedPowerShellCommand,
  ParsedCommandElement,
  CommandElementType,
} from './powershell-parser';
// Reuse the canonical filesystem-operation discriminant (no duplicate type).
import type { FileOperationType } from '../permissions';
import { escapeRegExp } from '../utils/string';
import { COMMON_SWITCHES, COMMON_VALUE_PARAMS, PS_PASS, PS_SPAWNERS_AND_LOADERS, PS_TYPE_CAST_RE, aliasesOf, argMatchesParam, argTokensAfter, detectDynamicCommandName, detectStopParsingToken, hasPositionalStringArg, normalizeParamToken, normalizeTypeName } from './powershell-security-power-shell-read-only-config';
import type { PsDetection } from './powershell-security-power-shell-read-only-config';
import { POWERSHELL_READONLY_CONFIG, PS_READONLY_DISQUALIFY_RE } from './powershell-security-powershell-readonly-config';
import { CMDLET_PATH_CONFIG, PS_SAFE_RESULT, askResult, hasComplexColonValue, hasPositionalStringArgAst, matchesCmdletParam, psParamAbbrevAltAware, startProcessColonVerbRunAs } from './powershell-security-cmdlet-path-config';
import type { PowerShellSecurityResult } from './powershell-security-cmdlet-path-config';
export { COMMON_SWITCHES, COMMON_VALUE_PARAMS, detectDynamicCommandName, detectStopParsingToken, normalizeTypeName } from './powershell-security-power-shell-read-only-config';
export type { PowerShellReadOnlyConfig } from './powershell-security-power-shell-read-only-config';
export { POWERSHELL_READONLY_CONFIG, canonicalizePowerShellCommandName } from './powershell-security-powershell-readonly-config';
export type { CmdletPathConfig } from './powershell-security-powershell-readonly-config';
export { CMDLET_PATH_CONFIG } from './powershell-security-cmdlet-path-config';
export type { PowerShellSecurityResult } from './powershell-security-cmdlet-path-config';

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
 * This mirrors the reference's PowerShellTool/clmTypes.ts (CLM_ALLOWED_TYPES,
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
 * True if `typeName` (as written in a cast literal) is in Microsoft's CLM
 * allowlist. Types NOT in this set trigger an ask — they reach system APIs CLM
 * blocks (reflection, P/Invoke, network binds, process spawning).
 */
export function isClmAllowedType(typeName: string): boolean {
  return CLM_ALLOWED_TYPES.has(normalizeTypeName(typeName));
}

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
// Mirrors the reference's src/utils/powershell/dangerousCmdlets.ts: one canonical
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
 * variables (e.g. `Set-Item Env:Path …`). Unlike the reference's list, bash-colliding
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
 * rejected) and the same wildcard-bypass risk. Mirrors the reference's
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

/**
 * Commands never offered as a reusable wildcard-prefix ("allow always") rule in
 * the permission dialog. Derived from EVERY dangerous-cmdlet category above plus
 * the PowerShell spawners/loaders, then expanded with their aliases — so a
 * cmdlet listed once (in the category that fits its hazard) is covered here
 * automatically, with no sync drift. A wildcard rule for any of these would
 * defeat the per-command security checks (download/exfil, code-exec, process
 * spawn, scriptblock arg gate, …). Mirrors the reference's `NEVER_SUGGEST`
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
 * Cache of compiled command-position regexes keyed by cmdlet name. The cmdlet name
 * Sets are constant, but `cmdletInCommandPosition` is called ~25-35 times per bash
 * tool call (the whole PowerShell-detection chain); compiling a fresh RegExp (with
 * an escapeRegExp pass) for every name on every call was pure repeated work. The
 * regexes are stateless `.test()` (no `g` flag), so caching them is behavior-exact.
 */
const PS_CMD_POS_REGEX_CACHE = new Map<string, RegExp>();

function psCmdPosRegex(name: string): RegExp {
  let re = PS_CMD_POS_REGEX_CACHE.get(name);
  if (re === undefined) {
    re = new RegExp(PS_CMD_POS_PREFIX + escapeRegExp(name) + String.raw`\b`, 'i');
    PS_CMD_POS_REGEX_CACHE.set(name, re);
  }
  return re;
}

/**
 * True if any of `names` is invoked in command position within `command`.
 * Returns the matched name (lowercased) so callers can build a message.
 */
function cmdletInCommandPosition(
  command: string,
  names: Iterable<string>,
): string | undefined {
  for (const name of names) {
    if (psCmdPosRegex(name).test(command)) {
      return name;
    }
  }
  return undefined;
}

/**
 * `New-Object` hazards (mirrors the reference's checkComObject + the CLM string-arg
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
 * `Start-Process` elevation / nested-shell launch (mirrors the reference's
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
 * Runtime-state hijack (mirrors the reference's checkRuntimeStateManipulation): any
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
 * Environment-variable manipulation (mirrors the reference's checkEnvVarManipulation):
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

export const COMMON_PARAMETERS: ReadonlySet<string> = new Set([
  ...COMMON_SWITCHES,
  ...COMMON_VALUE_PARAMS,
]);

/**
 * Script-block consumers that only ever filter/transform pipeline input — their
 * block is a predicate/selector, not arbitrary script to run. A script block
 * passed to any OTHER cmdlet is treated as potential arbitrary code. Mirrors
 * the reference's SAFE_SCRIPT_BLOCK_CMDLETS. `foreach-object` is intentionally absent:
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
 * establishes persistence that survives the session. Mirrors the reference's
 * SCHEDULED_TASK_CMDLETS; the legacy `schtasks.exe /create|/change` form is
 * handled separately by detectScheduledTask.
 */
export const SCHEDULED_TASK_CMDLETS: ReadonlySet<string> = new Set([
  'register-scheduledtask',
  'new-scheduledtask',
  'new-scheduledtaskaction',
  'set-scheduledtask',
]);

/**
 * `-FilePath`/positional script execution on the FILEPATH_EXECUTION_CMDLETS
 * (Invoke-Command, Start-Job, …). A `-FilePath`/`-LiteralPath` flag OR any
 * positional string arg (which binds to -FilePath via parameter-set resolution)
 * executes an arbitrary script file — the same risk as Invoke-Expression.
 * Mirrors the reference's checkDangerousFilePathExecution.
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
 * Aliases `%` / `foreach` resolve to foreach-object. Mirrors the reference's
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
 * Scheduled-task persistence (mirrors the reference's checkScheduledTask): a
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
 * (ShellExecute); on an executable it runs arbitrary code. Mirrors the reference's
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
 * Standalone `Invoke-Expression` eval (mirrors the reference's checkInvokeExpression):
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
 * Script-block injection (mirrors the reference's checkScriptBlockInjection, denylist
 * half): when a `{ … }` script block is present as an argument, any
 * DANGEROUS_SCRIPT_BLOCK_CMDLETS cmdlet (Invoke-Command, Start-Job, Register-*
 * event, New/Enter-PSSession, Invoke-Expression) in command position executes
 * that block as arbitrary code. The reference's allowlist "all commands safe"
 * second pass is intentionally omitted here: detectPowerShellExecution runs over
 * EVERY command (not a PowerShell-only AST like the reference), so an allowlist default
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
 * Aggregate of the PowerShell dangerous-pattern detectors added on top of the
 * download-cradle / encoded-command / CLM-cast checks already in
 * detectPowerShellExecution. Runs in a fixed order and returns the first block.
 * The input must already be Unicode-stripped / alt-dash-normalized by the caller
 * (detectPowerShellExecution does this), mirroring the reference's ordered validator
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

export const POWERSHELL_READONLY_CMDLETS: ReadonlySet<string> = new Set(
  Object.keys(POWERSHELL_READONLY_CONFIG),
);

/**
 * Per-cmdlet safe-flag gate. Returns true when EVERY flag token in `segment`
 * (after the command name) is in the cmdlet's safeFlags set ∪
 * {@link COMMON_PARAMETERS}, or the cmdlet declares `allowAllFlags`. A flag
 * outside that surface — a side-effecting / remote / external-entity flag such
 * as `-ComputerName`, `-CimSession`, or Get-WinEvent `-FilterXml` — returns
 * false so the command is NOT auto-allowed. Mirrors the reference's
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
 * falls through to a prompt). Mirrors the reference's isReadOnlyCommand contract.
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

/**
 * Benign edit-adjacent PowerShell WRITE cmdlets that `acceptEdits` mode may
 * auto-allow — the Windows counterpart of the POSIX ACCEPT_EDITS_FS_COMMANDS
 * (`mkdir`/`touch`/`mv`/`cp`/`sed`). Canonical (lowercase) cmdlet names so an
 * alias (`sc`→`set-content`, `ac`→`add-content`, `ni`→`new-item`) resolves in
 * via {@link resolveToCanonical}. Mirrors the reference's PowerShell modeValidation
 * write-cmdlet allowlist.
 *
 * DELIBERATE EXCLUSIONS (superset divergence — never regress a delete guard):
 *  - `remove-item`/`clear-content` and their aliases are NOT here — every
 *    deletion/truncation stays gated behind an explicit prompt even in
 *    acceptEdits, exactly as the bash set excludes `rm`/`rmdir`.
 */
export const POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS: ReadonlySet<string> = new Set([
  'set-content',
  'add-content',
  'out-file',
  'tee-object',
  'new-item',
]);

/**
 * `New-Item -ItemType` values that create a FILESYSTEM LINK rather than a plain
 * file/directory. Link creation is a redirection/escape vector (a symlink can
 * point a later "edit" at a secret or system path), so `acceptEdits` must NEVER
 * silently auto-allow it — it falls through to an explicit prompt. Mirrors
 * the reference's New-Item link-creation detection.
 */
const NEW_ITEM_LINK_ITEM_TYPES: ReadonlySet<string> = new Set([
  'symboliclink',
  'hardlink',
  'junction',
]);

/**
 * True when `command` is a SINGLE benign PowerShell write cmdlet that
 * `acceptEdits` mode may auto-allow (the Windows counterpart of
 * {@link isReadOnlyPowerShellCommand}'s read path, for writes).
 *
 * Fail-closed, by construction conservative:
 *  - rejects any un-analyzable construct ({@link PS_READONLY_DISQUALIFY_RE}:
 *    `$`/backtick/`{}`/`()`/`@`/`&`/`>`) and any statement separator (`;`/`|`/
 *    newline) — one write cmdlet per invocation, nothing chained or piped;
 *  - the command-position name must canonicalize into
 *    {@link POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS};
 *  - `New-Item` creating a link ({@link NEW_ITEM_LINK_ITEM_TYPES} via
 *    `-ItemType`, incl. colon-bound / abbreviated / alt-dash forms) is rejected;
 *  - a defense-in-depth {@link detectDangerousPowerShellCommand} /
 *    {@link detectUnsafeTypeCast} scan must find no hazard.
 *
 * Anything it cannot prove benign returns false → the caller falls through to a
 * prompt. Credential/system-path and destructive guards still run in the
 * permission engine around this check; this only WIDENS the benign-write
 * auto-allow, never the dangerous surface.
 */
export function isAcceptEditsPowerShellWrite(command: string): boolean {
  const trimmed = command.trim();
  if (trimmed.length === 0) return false;
  // One write cmdlet, nothing chained/piped/redirected/expanded (fail-closed).
  if (PS_READONLY_DISQUALIFY_RE.test(trimmed)) return false;
  if (/[;\n\r|]/.test(trimmed)) return false;

  const tokens = trimmed.match(/\S+/g);
  if (tokens === null || tokens.length === 0) return false;
  const first = tokens[0]!;
  // A leading flag/switch is malformed for a write classify.
  if (first.startsWith('-') || first.startsWith('/')) return false;
  // Strip a module qualifier (`Microsoft.PowerShell.Management\New-Item`) then
  // resolve aliases (`sc`/`ac`/`ni`) + `.exe` to the canonical cmdlet name.
  const bare = (first.split(/[\\/]/).pop() ?? first);
  const cmdlet = resolveToCanonical(bare);
  if (!POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS.has(cmdlet)) return false;

  // New-Item link creation is a redirection vector — never auto-allow it.
  if (cmdlet === 'new-item') {
    for (let i = 1; i < tokens.length; i++) {
      const norm = normalizeParamToken(tokens[i]!);
      if (!norm.startsWith('-')) continue;
      // Match `-ItemType SymbolicLink`, `-ItemType:SymbolicLink`, and any
      // abbreviated spelling (`-it`/`-item…`) PowerShell accepts for the param.
      const [name, inlineValue] = norm.split(':', 2);
      if (name !== undefined && '-itemtype'.startsWith(name) && name.length >= 2) {
        const value = (inlineValue ?? tokens[i + 1] ?? '').replace(/`/g, '').toLowerCase();
        if (NEW_ITEM_LINK_ITEM_TYPES.has(value)) return false;
      }
    }
  }

  // Defense in depth: never auto-allow something the hazard detectors flag.
  if (detectDangerousPowerShellCommand(trimmed).blocked) return false;
  if (detectUnsafeTypeCast(trimmed).blocked) return false;
  return true;
}

/**
 * Element types safe to treat as a static filesystem path. Anything else
 * (Variable, SubExpression, ExpandableString, ScriptBlock, …) is a
 * runtime-computed value whose path cannot be statically validated — it forces
 * an ask. Mirrors the reference's SAFE_PATH_ELEMENT_TYPES.
 */
const SAFE_PATH_ELEMENT_TYPES: ReadonlySet<CommandElementType> = new Set<CommandElementType>(['StringConstant', 'Parameter']);

/**
 * Extract filesystem paths from a parsed PowerShell command element using
 * {@link CMDLET_PATH_CONFIG}. Returns the extracted `paths`, the cmdlet's
 * `operationType`, `hasUnvalidatablePathArg` (true when any unknown -Param or
 * non-static value means the invocation cannot be statically validated → the
 * caller must ask), and the resolved `optionalWrite` knob.
 *
 * Mirrors the reference's PowerShellTool/pathValidation.ts `extractPathsFromCommand`,
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
  // here. Mirrors the reference's parse-failure reason surfacing
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
  // Mirrors the reference's checkStartProcess children[] layer (PR #23554).
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
  // Mirrors the reference's PowerShellTool/pathValidation.ts
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
