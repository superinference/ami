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
export function detectStartProcessElevation(command: string): PsDetection {
  const cmdletRe = new RegExp(
    PS_CMD_POS_PREFIX + String.raw`(?:start-process|saps)\b`,
    'i',
  );
  const args = argTokensAfter(command, cmdletRe);
  if (args === undefined) {
    return PS_PASS;
  }
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
  ];
  for (const check of checks) {
    const r = check(command);
    if (r.blocked) {
      return r;
    }
  }
  return PS_PASS;
}
