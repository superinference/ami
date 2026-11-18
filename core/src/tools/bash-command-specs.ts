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
 * vocabulary mirrors Claude Code's bash command-spec model
 * (src/utils/bash/registry.ts + src/utils/bash/specs/*): `isCommand`,
 * `isVariadic`, `isOptional`, `isModule`, `isScript`, `isDangerous` on an
 * argument; `name` / `args` / `isRequired` on an option. AMI's specs are a
 * SUPERSET — e.g. the `timeout` spec carries AMI's richer `-k`/`-s` value
 * options that Claude's spec omits — because AMI's wrapper-unwrapping needs
 * them; never a subset.
 */

/**
 * A positional argument of a command spec. Mirrors Claude Code's `Argument`
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
 * An option (flag) of a command spec. Mirrors Claude Code's `Option`. When
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
 * A declarative model of one command's arguments and options. Mirrors Claude
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
 * `sleep` / `alias` / `pyright` round out Claude Code's spec catalog
 * (src/utils/bash/specs/*) so the declarative model is a superset of the
 * reference, not a subset. Authored faithfully to Claude Code's specs where they
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
    // AMI superset: Claude's timeout spec has no options; AMI's wrapper-unwrapping
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
];

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
