/**
 * Declarative per-command spec registry — a typed vocabulary for the flag /
 * option-argument arity of the shell *wrapper* commands AMI's security layer
 * must unwrap.
 *
 * Before this module, the knowledge of "which of a wrapper's options consume the
 * FOLLOWING token as a value" (so the wrapped command is located PAST that value
 * rather than the value being mistaken for the command) lived as hand-authored
 * regex literals duplicated across {@link ./bash-security} and
 * {@link ../permissions}. That is the ad-hoc, per-wrapper modelling the parity
 * audit flagged. This module makes it DECLARATIVE: each wrapper is a
 * {@link CommandSpec} whose {@link CommandSpecOption}s carry their value arity,
 * and {@link buildWrapperValueFlagPattern} derives the matcher from the spec so
 * the two consumers share a single source of truth and cannot drift.
 *
 * The {@link CommandSpec} / {@link CommandSpecArgument} / {@link CommandSpecOption}
 * vocabulary mirrors the reference's bash command-spec model
 * (src/utils/bash/registry.ts + src/utils/bash/specs/*): `isCommand`,
 * `isVariadic`, `isOptional`, `isModule`, `isScript`, `isDangerous` on an
 * argument; `name` / `args` / `isRequired` on an option. AMI's specs are a
 * SUPERSET — e.g. the `timeout` spec carries AMI's richer `-k`/`-s` value
 * options that the reference's spec omits — because AMI's wrapper-unwrapping needs
 * them; never a subset.
 */

/**
 * A positional argument of a command spec. Mirrors the reference's `Argument`
 * (src/utils/bash/registry.ts): the flags describe what the token means so a
 * consumer can reason about it structurally instead of by command-specific
 * regex.
 */
export interface CommandSpecArgument {
  name?: string;
  description?: string;
  /** The argument is itself a command run by the wrapper (e.g. `timeout <cmd>`). */
  isCommand?: boolean;
  /** The argument repeats (e.g. `echo a b c`). */
  isVariadic?: boolean;
  /** The argument may be omitted. */
  isOptional?: boolean;
  /** The argument names a module (e.g. `python -m <module>`). */
  isModule?: string | boolean;
  /** The argument names a script file (e.g. `node <script.js>`). */
  isScript?: boolean;
  /** The argument is inherently dangerous. */
  isDangerous?: boolean;
}

/**
 * An option (flag) of a command spec. Mirrors the reference's `Option`. When
 * {@link args} is present the option consumes a following token as its value —
 * the key arity fact {@link buildWrapperValueFlagPattern} derives from.
 */
export interface CommandSpecOption {
  /** The flag spelling(s), e.g. `'-n'` or `['-n', '--ntasks']`. */
  name: string | string[];
  description?: string;
  /** The value operand(s) the option consumes; presence ⇒ value-taking. */
  args?: CommandSpecArgument | CommandSpecArgument[];
  isRequired?: boolean;
}

/**
 * A declarative model of one command's arguments and options. Mirrors the reference
 * Code's `CommandSpec`.
 */
export interface CommandSpec {
  name: string;
  description?: string;
  subcommands?: CommandSpec[];
  args?: CommandSpecArgument | CommandSpecArgument[];
  options?: CommandSpecOption[];
}

/**
 * The spec registry. Models the shell *wrappers* AMI's security layer unwraps
 * and whose option arity it must know — `srun` (SLURM launcher) plus the
 * value-flag-bearing `timeout` / `stdbuf` / `nice` / `env`. `nohup` is a pure
 * command-wrapper with no options; `time` carries only the value-less `-p`.
 * `sleep` / `alias` / `pyright` round out the reference's spec catalog
 * (src/utils/bash/specs/*) so the declarative model is a superset of the
 * reference, not a subset. Authored faithfully to the reference's specs where they
 * overlap, extended where AMI needs more (see `timeout`).
 */
export const COMMAND_SPECS: readonly CommandSpec[] = [
  {
    name: 'srun',
    description: 'Run a command on SLURM cluster nodes',
    options: [
      { name: ['-n', '--ntasks'], description: 'Number of tasks', args: { name: 'count' } },
      { name: ['-N', '--nodes'], description: 'Number of nodes', args: { name: 'count' } },
    ],
    args: { name: 'command', description: 'Command to run on the cluster', isCommand: true },
  },
  {
    name: 'timeout',
    description: 'Run a command with a time limit',
    // AMI superset: the reference's timeout spec has no options; AMI's wrapper-unwrapping
    // must know `-k`/`--kill-after` and `-s`/`--signal` take a following operand.
    options: [
      { name: ['-k', '--kill-after'], description: 'Send KILL after this delay', args: { name: 'duration' } },
      { name: ['-s', '--signal'], description: 'Signal to send on timeout', args: { name: 'signal' } },
    ],
    args: [
      { name: 'duration', description: 'Time limit (e.g. 10, 5s, 2m)', isOptional: false },
      { name: 'command', description: 'Command to run', isCommand: true },
    ],
  },
  {
    name: 'stdbuf',
    description: 'Run a command with modified buffering for its standard streams',
    options: [
      { name: ['-i', '--input'], description: 'Adjust stdin buffering', args: { name: 'MODE' } },
      { name: ['-o', '--output'], description: 'Adjust stdout buffering', args: { name: 'MODE' } },
      { name: ['-e', '--error'], description: 'Adjust stderr buffering', args: { name: 'MODE' } },
    ],
    args: { name: 'command', description: 'Command to run', isCommand: true },
  },
  {
    name: 'nice',
    description: 'Run a command with an adjusted scheduling priority',
    options: [
      { name: ['-n', '--adjustment'], description: 'Niceness adjustment', args: { name: 'N' } },
    ],
    args: { name: 'command', description: 'Command to run', isCommand: true },
  },
  {
    name: 'env',
    description: 'Run a command in a modified environment',
    options: [
      // Value-less env flags (modelled so the derived value-flag matcher excludes
      // them — only `-u`/`--unset` consumes the following NAME operand).
      { name: ['-i', '--ignore-environment'], description: 'Start with an empty environment' },
      { name: ['-0', '--null'], description: 'End each output line with NUL, not newline' },
      { name: ['-v', '--debug'], description: 'Print verbose information for each step' },
      { name: ['-u', '--unset'], description: 'Remove a variable from the environment', args: { name: 'NAME' } },
    ],
    args: { name: 'command', description: 'Command to run', isCommand: true },
  },
  {
    name: 'nohup',
    description: 'Run a command immune to hangups',
    args: { name: 'command', description: 'Command to run with nohup', isCommand: true },
  },
  {
    name: 'time',
    description: 'Time a command',
    // `time -p` is a value-less flag; `time` has no value-taking options.
    options: [{ name: '-p', description: 'Use the POSIX output format' }],
    args: { name: 'command', description: 'Command to time', isCommand: true },
  },
  {
    name: 'sleep',
    description: 'Delay for a specified amount of time',
    args: {
      name: 'duration',
      description: 'Duration to sleep (seconds or with suffix like 5s, 2m, 1h)',
      isOptional: false,
    },
  },
  {
    name: 'alias',
    description: 'Create or list command aliases',
    args: {
      name: 'definition',
      description: 'Alias definition in the form name=value',
      isOptional: true,
      isVariadic: true,
    },
  },
  {
    name: 'pyright',
    description: 'Type checker for Python',
    options: [
      { name: ['--help', '-h'], description: 'Show help message' },
      { name: '--version', description: 'Print pyright version and exit' },
      { name: ['--watch', '-w'], description: 'Continue to run and watch for changes' },
      { name: ['--project', '-p'], description: 'Use the configuration file at this location', args: { name: 'FILE OR DIRECTORY' } },
      { name: '-', description: 'Read file or directory list from stdin' },
      { name: '--createstub', description: 'Create type stub file(s) for import', args: { name: 'IMPORT' } },
      { name: ['--typeshedpath', '-t'], description: 'Use typeshed type stubs at this location', args: { name: 'DIRECTORY' } },
      { name: '--verifytypes', description: 'Verify completeness of types in py.typed package', args: { name: 'IMPORT' } },
      { name: '--ignoreexternal', description: 'Ignore external imports for --verifytypes' },
      { name: '--pythonpath', description: 'Path to the Python interpreter', args: { name: 'FILE' } },
      { name: '--pythonplatform', description: 'Analyze for platform', args: { name: 'PLATFORM' } },
      { name: '--pythonversion', description: 'Analyze for Python version', args: { name: 'VERSION' } },
      { name: ['--venvpath', '-v'], description: 'Directory that contains virtual environments', args: { name: 'DIRECTORY' } },
      { name: '--outputjson', description: 'Output results in JSON format' },
      { name: '--verbose', description: 'Emit verbose diagnostics' },
      { name: '--stats', description: 'Print detailed performance stats' },
      { name: '--dependencies', description: 'Emit import dependency information' },
      { name: '--level', description: 'Minimum diagnostic level', args: { name: 'LEVEL' } },
      { name: '--skipunannotated', description: 'Skip type analysis of unannotated functions' },
      { name: '--warnings', description: 'Use exit code of 1 if warnings are reported' },
      { name: '--threads', description: 'Use up to N threads to parallelize type checking', args: { name: 'N', isOptional: true } },
    ],
    args: {
      name: 'files',
      description: 'Specify files or directories to analyze (overrides config file)',
      isVariadic: true,
      isOptional: true,
    },
  },
  // Interpreters. These declare the structured per-argument attributes the reference's
  // fig specs carry (`isModule` on `python -m <module>`, `isCommand`+`isDangerous`
  // on an inline-code flag like `python -c <code>` / `node -e <code>`, and an
  // `isScript` positional) so the prefix extractor ({@link buildSpecPrefix}) can
  // FOLD a module name into a canonical prefix (`python -m http.server`) while
  // STOPPING at inline code — the code must never become part of a grantable
  // prefix. Mirrors the reference's python `-c` break + `-m`/isModule inclusion
  // (src/utils/shell/specPrefix.ts L103-119, L229-236).
  {
    name: 'python',
    description: 'Python interpreter',
    options: [
      { name: '-m', description: 'Run a library module as a script', args: { name: 'module', isModule: true } },
      { name: '-c', description: 'Run inline code passed as a string', args: { name: 'cmd', isCommand: true, isDangerous: true } },
    ],
    args: { name: 'script', description: 'Script file to run', isScript: true, isOptional: true },
  },
  {
    name: 'python3',
    description: 'Python 3 interpreter',
    options: [
      { name: '-m', description: 'Run a library module as a script', args: { name: 'module', isModule: true } },
      { name: '-c', description: 'Run inline code passed as a string', args: { name: 'cmd', isCommand: true, isDangerous: true } },
    ],
    args: { name: 'script', description: 'Script file to run', isScript: true, isOptional: true },
  },
  {
    name: 'node',
    description: 'Node.js interpreter',
    options: [
      { name: ['-e', '--eval'], description: 'Evaluate inline code', args: { name: 'script', isCommand: true, isDangerous: true } },
      { name: ['-p', '--print'], description: 'Evaluate inline code and print the result', args: { name: 'script', isCommand: true, isDangerous: true } },
    ],
    args: { name: 'script', description: 'Script file to run', isScript: true, isVariadic: true, isOptional: true },
  },
  {
    name: 'ruby',
    description: 'Ruby interpreter',
    options: [
      { name: '-e', description: 'Evaluate inline code', args: { name: 'code', isCommand: true, isDangerous: true } },
    ],
    args: { name: 'script', description: 'Script file to run', isScript: true, isOptional: true },
  },
  {
    name: 'perl',
    description: 'Perl interpreter',
    options: [
      { name: '-e', description: 'Evaluate inline code', args: { name: 'code', isCommand: true, isDangerous: true } },
    ],
    args: { name: 'script', description: 'Script file to run', isScript: true, isOptional: true },
  },
];

const URL_PROTOCOLS = ['http://', 'https://', 'ftp://'];

const toArray = <T>(v: T | T[]): T[] => (Array.isArray(v) ? v : [v]);

/** The option in `spec` whose flag spellings include `flag`, or undefined. */
function findSpecOption(spec: CommandSpec, flag: string): CommandSpecOption | undefined {
  return spec.options?.find((o) => optionNames(o).includes(flag));
}

/** The declared value operands of an option (empty when it takes none). */
function specOptionArgs(opt: CommandSpecOption | undefined): CommandSpecArgument[] {
  return opt?.args ? toArray(opt.args) : [];
}

/**
 * How many words a scoped prefix extends to for a command, DRIVEN BY the
 * structured per-argument attributes its spec declares — the parity analogue of
 * the reference's `calculateDepth` (src/utils/shell/specPrefix.ts L139-209), minus
 * the fig/DEPTH_RULES table (AMI's subcommand-CLI depth lives in permissions.ts's
 * `BASH_COMMAND_SPECS`; this consumes the attribute model those CLIs lack).
 *
 * - an option arg marked `isCommand`/`isModule` (a nested command/module, e.g.
 *   `python -m <module>`) ⇒ depth 3 (the prefix extends past the flag + operand);
 * - a positional `isCommand` ⇒ depth to that positional (bounded at 3);
 * - a `isVariadic` leading positional ⇒ depth 1 (the first word is the whole
 *   meaningful prefix — e.g. `node a.js b.js`);
 * - a REQUIRED (`!isOptional`) leading positional ⇒ depth 2;
 * - an `isDangerous` positional ⇒ depth 3; otherwise depth 2.
 */
export function calculateSpecDepth(args: string[], spec: CommandSpec): number {
  if (spec.options && args.some((a) => a.startsWith('-'))) {
    for (const a of args) {
      if (!a.startsWith('-')) continue;
      const opt = findSpecOption(spec, a);
      if (opt && specOptionArgs(opt).some((x) => x.isCommand || x.isModule)) return 3;
    }
  }
  if (spec.args) {
    const arr = toArray(spec.args);
    if (arr.some((a) => a.isCommand)) {
      return !Array.isArray(spec.args) && spec.args.isCommand
        ? 2
        : Math.min(2 + arr.findIndex((a) => a.isCommand), 3);
    }
    if (!spec.subcommands?.length) {
      if (arr.some((a) => a.isVariadic)) return 1;
      if (arr[0] && !arr[0].isOptional) return 2;
    }
  }
  return spec.args && toArray(spec.args).some((a) => a.isDangerous) ? 3 : 2;
}

/**
 * Whether prefix extraction should STOP at a bare positional `arg`. A path- or
 * URL-shaped token ends the prefix (a filename / URL must never leak into a
 * grantable rule) EXCEPT a module name that immediately follows a module flag
 * (`python -m http.server` — the dotted module looks file-shaped but belongs in
 * the prefix). Mirrors the reference's `shouldStopAtArg` (specPrefix.ts L211-241).
 */
function shouldStopAtSpecArg(arg: string, priorArgs: string[], spec: CommandSpec): boolean {
  if (arg.startsWith('-')) return true;
  const dot = arg.lastIndexOf('.');
  const hasExt = dot > 0 && dot < arg.length - 1 && !arg.substring(dot + 1).includes(':');
  const hasFile = arg.includes('/') || hasExt;
  const hasUrl = URL_PROTOCOLS.some((p) => arg.startsWith(p));
  if (!hasFile && !hasUrl) return false;
  const prev = priorArgs[priorArgs.length - 1];
  if (prev) {
    const opt = findSpecOption(spec, prev);
    if (opt && specOptionArgs(opt).some((a) => a.isModule)) return false; // module name
  }
  return true;
}

/**
 * Build a scoped permission prefix for `command` + its `args` from the command's
 * structured spec. Folds a module flag and its module name into the prefix
 * (`python -m http.server`), STOPS at an inline-code flag so the code is never
 * part of a grantable prefix (`python -c '<code>'` → `python`), and stops at a
 * file/URL operand. Depth is bounded by {@link calculateSpecDepth}. Pure over
 * `(string, string[], CommandSpec)`. Mirrors the reference's `buildPrefix`
 * (src/utils/shell/specPrefix.ts L88-137).
 */
export function buildSpecPrefix(command: string, args: string[], spec: CommandSpec): string {
  const maxDepth = calculateSpecDepth(args, spec);
  const parts = [command];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg || parts.length >= maxDepth) break;
    if (arg.startsWith('-')) {
      const oArgs = specOptionArgs(findSpecOption(spec, arg));
      // Inline-code flag (python -c, node -e): stop — the code after it must never
      // be folded into a grantable prefix.
      if (oArgs.some((a) => a.isCommand && a.isDangerous)) break;
      // Module flag (python -m): fold the flag in; the module name follows.
      if (oArgs.some((a) => a.isModule)) { parts.push(arg); continue; }
      break; // any other flag ends the prefix
    }
    if (shouldStopAtSpecArg(arg, args.slice(0, i), spec)) break;
    parts.push(arg);
  }
  return parts.join(' ');
}

/**
 * Whether `prefix` is a scoped interpreter MODULE-exec form (`python -m
 * http.server`) — an interpreter root followed by one of its module flags and a
 * module-shaped name. Such a prefix is a narrow, safe grant (it runs one named
 * module, not arbitrary code) even though the interpreter root would otherwise be
 * refused as a bare-shell prefix. Used by the suggestion layer to let the module
 * form through the bare-shell guard.
 */
export function isScopedModulePrefix(prefix: string): boolean {
  const words = prefix.split(' ');
  if (words.length !== 3) return false;
  const spec = getCommandSpec(words[0]!.toLowerCase());
  if (!spec) return false;
  const opt = findSpecOption(spec, words[1]!);
  return !!opt && specOptionArgs(opt).some((a) => a.isModule);
}

const SPEC_BY_NAME: ReadonlyMap<string, CommandSpec> = new Map(
  COMMAND_SPECS.map((spec) => [spec.name, spec]),
);

/** Look up the declarative spec for a command by name, or `undefined`. */
export function getCommandSpec(name: string): CommandSpec | undefined {
  return SPEC_BY_NAME.get(name);
}

/** Normalize an option's `name` (string | string[]) to a flat list of spellings. */
function optionNames(option: CommandSpecOption): string[] {
  return Array.isArray(option.name) ? option.name : [option.name];
}

/**
 * Derive the anchored matcher for a wrapper's VALUE-taking option flags — every
 * spelling of every option that declares `args` (so it consumes the following
 * token). Returns `undefined` when the command is unknown or has no value-taking
 * options.
 *
 * This is the single source of truth the security wrapper-unwrapping consumes
 * (via `WRAPPER_VALUE_FLAGS` and `SRUN_VALUE_FLAGS` in `./bash-security`),
 * replacing the previously duplicated hand-authored regex literals.
 */
export function buildWrapperValueFlagPattern(name: string): RegExp | undefined {
  const spec = getCommandSpec(name);
  if (!spec?.options) return undefined;
  const flags: string[] = [];
  for (const option of spec.options) {
    if (option.args === undefined) continue;
    flags.push(...optionNames(option));
  }
  if (flags.length === 0) return undefined;
  // Flag spellings are plain `-x` / `--word` tokens (no regex metacharacters
  // besides `-`, which is literal outside a character class), so the alternation
  // is safe to build directly.
  return new RegExp(`^(${flags.join('|')})$`);
}
