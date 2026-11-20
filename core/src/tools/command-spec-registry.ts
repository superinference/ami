import { memoizeAsyncWithLRU } from '../utils/memoize';

/**
 * Extensible per-command option/argument schema resolver — AMI's parity analogue
 * of the reference's `utils/bash/registry.ts` (`getCommandSpec` / `loadFigSpec`).
 *
 * the reference resolves a command's flag/argument schema at runtime by (1) checking a
 * bundled table of built-in specs, then (2) dynamically importing a Fig
 * autocomplete spec (`@withfig/autocomplete/build/<command>.js`) as a fallback —
 * giving structured flag/arg coverage of thousands of CLI tools without
 * hand-maintaining each. The dynamic import is guarded by a path-safety check
 * (reject names containing `/`, `\`, `..`, or a leading `-`) and the whole lookup
 * is memoized by command name.
 *
 * This module provides the same three pieces:
 *   - {@link BUILTIN_COMMAND_SPECS} — a bundled table (seeded with commands whose
 *     global value-flags matter for prefix scoping but that the permission layer's
 *     hand-maintained `BASH_COMMAND_SPECS` does not already cover — disjoint, so
 *     there is no duplicate table of the same commands).
 *   - {@link loadFigSpec} — the path-safety-validated `@withfig/autocomplete`
 *     dynamic-import fallback (returns null when the optional package is absent,
 *     exactly like the reference when it is not installed).
 *   - {@link getCommandSpec} — built-in → fig → null, wrapped in the shared
 *     {@link memoizeWithLRU} (keyed by command name), so repeated lookups never
 *     re-run the dynamic import.
 *
 * The CommandSpec shape mirrors the reference's `@withfig/autocomplete`-compatible type
 * so a real Fig spec loaded via {@link loadFigSpec} is structurally usable.
 *
 * Wiring: {@link valueFlagsForCommand} derives, from a built-in spec's options,
 * the set of global flags that consume the following token as a value. The bash
 * permission classifier consults it (in `permissions.ts`) as a fallback when its
 * own `BASH_COMMAND_SPECS` has no entry for a command, so a global value-flag
 * before the subcommand (e.g. `make -C dir test`, `go -C dir build`) no longer
 * defeats SAFE_PREFIXES matching for registry-covered commands.
 */

/** A single positional/argument slot in a command or option. */
export type CommandArgument = {
  name?: string;
  description?: string;
  /** Supplying this argument is dangerous (e.g. a destructive target). */
  isDangerous?: boolean;
  /** Repeats infinitely, e.g. `echo hello world`. */
  isVariadic?: boolean;
  isOptional?: boolean;
  /** Wrapper commands whose argument is itself a command, e.g. `timeout`, `sudo`. */
  isCommand?: boolean;
  /** For module args, e.g. `python -m`. */
  isModule?: string | boolean;
  /** Script-file args, e.g. `node script.js`. */
  isScript?: boolean;
};

/** A flag/option a command accepts, optionally consuming argument(s). */
export type CommandOption = {
  /** Flag spelling(s), e.g. `'-C'` or `['-C', '--directory']`. */
  name: string | string[];
  description?: string;
  /** When present, the option consumes the following token(s) as its value. */
  args?: CommandArgument | CommandArgument[];
  isRequired?: boolean;
};

/** A command's option/argument/subcommand schema (Fig-autocomplete compatible). */
export type CommandSpec = {
  name: string;
  description?: string;
  subcommands?: CommandSpec[];
  args?: CommandArgument | CommandArgument[];
  options?: CommandOption[];
};

/**
 * Bundled built-in specs. Intentionally DISJOINT from permissions.ts's
 * `BASH_COMMAND_SPECS` (git/docker/kubectl/…) — this table adds commands whose
 * global value-flags were previously unmodeled, so extending the classifier via
 * {@link valueFlagsForCommand} widens coverage without a second copy of an
 * already-modeled command.
 */
export const BUILTIN_COMMAND_SPECS: readonly CommandSpec[] = [
  {
    name: 'make',
    description: 'GNU make build tool',
    options: [
      { name: ['-C', '--directory'], description: 'Change to directory before reading makefiles', args: { name: 'dir' } },
      { name: ['-f', '--file', '--makefile'], description: 'Use file as a makefile', args: { name: 'file' } },
      { name: ['-I', '--include-dir'], description: 'Search directory for included makefiles', args: { name: 'dir' } },
      { name: ['-o', '--old-file', '--assume-old'], description: 'Consider file very old', args: { name: 'file' } },
      { name: ['-W', '--what-if', '--new-file', '--assume-new'], description: 'Consider file infinitely new', args: { name: 'file' } },
    ],
  },
  {
    name: 'go',
    description: 'Go toolchain',
    options: [
      { name: ['-C'], description: 'Change to dir before running the command', args: { name: 'dir' } },
    ],
    subcommands: [
      { name: 'build' },
      { name: 'test' },
      { name: 'vet' },
      { name: 'run' },
    ],
  },
];

/** Normalize an option's `name` (string | string[]) to a flag-spelling array. */
function optionFlagNames(option: CommandOption): string[] {
  return Array.isArray(option.name) ? option.name : [option.name];
}

/**
 * Path-safety validator for a dynamically-imported spec name — mirrors the reference's
 * `loadFigSpec` guard: reject anything that could escape the package build
 * directory or be interpreted as a flag. A bare `-` is allowed (it names stdin in
 * some specs); any other leading-`-` name is rejected.
 */
export function isSafeCommandSpecName(command: string): boolean {
  if (!command) return false;
  if (command.includes('/') || command.includes('\\')) return false;
  if (command.includes('..')) return false;
  if (command.startsWith('-') && command !== '-') return false;
  return true;
}

/**
 * Optional Fig-autocomplete fallback loader. Dynamically imports
 * `@withfig/autocomplete/build/<command>.js` when the (optional) package is
 * present, after path-safety validation. Returns null when the name is unsafe or
 * the package/spec is absent — exactly like the reference when the dependency is
 * not installed.
 *
 * The specifier is assembled from a runtime variable so neither the type checker
 * nor the bundler statically resolves the optional dependency (it is not a
 * declared dependency of this package); a missing package throws at runtime and
 * is caught here.
 */
export async function loadFigSpec(command: string): Promise<CommandSpec | null> {
  if (!isSafeCommandSpecName(command)) return null;
  try {
    const specifier = '@withfig/autocomplete/build/' + command + '.js';
    const mod = (await import(specifier)) as { default?: CommandSpec } & CommandSpec;
    return mod.default ?? (mod.name ? mod : null);
  } catch {
    return null;
  }
}

/**
 * Resolve a command's spec: a built-in spec first, then the Fig fallback, then
 * null. Memoized by command name via {@link memoizeAsyncWithLRU} (cap 200, parity
 * with the reference's prefix.ts cache bound) so a cold lookup's dynamic import runs
 * at most once per command. Because the shared primitive caches the in-flight
 * promise and evicts it (identity-guarded) if it ever rejects, a transient failure
 * during the dynamic import can't poison future lookups for that command — a later
 * call retries cleanly.
 */
export const getCommandSpec = memoizeAsyncWithLRU(
  async (command: string): Promise<CommandSpec | null> => {
    const builtin = BUILTIN_COMMAND_SPECS.find((s) => s.name === command);
    if (builtin) return builtin;
    return loadFigSpec(command);
  },
  (command: string) => command,
  200,
);

/**
 * Derive, from a BUILT-IN spec's options, the set of flag spellings that consume
 * the following token as a value (every option declaring `args`). Returns
 * undefined when no built-in spec covers the command. Synchronous (built-in only)
 * so the bash permission classifier can consult it inside its synchronous
 * command-prefix parsing; the async Fig fallback is not used on that hot path.
 */
export function valueFlagsForCommand(command: string): ReadonlySet<string> | undefined {
  const spec = BUILTIN_COMMAND_SPECS.find((s) => s.name === command);
  if (!spec?.options) return undefined;
  const flags = new Set<string>();
  for (const option of spec.options) {
    if (option.args === undefined) continue;
    for (const name of optionFlagNames(option)) {
      if (name.startsWith('-')) flags.add(name);
    }
  }
  return flags.size > 0 ? flags : undefined;
}
