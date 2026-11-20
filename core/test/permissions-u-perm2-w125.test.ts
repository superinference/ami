import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getSimpleCommandPrefix,
  suggestAllowPrefix,
  PermissionManager,
} from '../src/permissions';
import {
  calculateSpecDepth,
  buildSpecPrefix,
  isScopedModulePrefix,
  getCommandSpec,
  type CommandSpec,
} from '../src/tools/bash-command-specs';

// ===========================================================================
// UNIT U-perm2 (w125) — structured per-argument spec attributes driving prefix
// extraction (gap 4) + interpreter -c/-m special-casing in prefix construction
// (gap 5). Parity: the reference src/utils/shell/specPrefix.ts (buildPrefix /
// calculateDepth / shouldStopAtArg). AMI already declared the isCommand/isModule/
// isVariadic/isOptional/isDangerous attributes on its CommandSpec model
// (bash-command-specs.ts) but did not consume them for prefix DEPTH or module
// FOLDING; this wires them end-to-end through suggestAllowPrefix → the permission
// manager so `python -m http.server` yields a scoped allow rule while
// `python -c '<code>'` never does.
// ===========================================================================

// --- gap 4: calculateSpecDepth consumes the structured arg attributes ---------

test('calculateSpecDepth: an option whose arg isModule/isCommand → depth 3', () => {
  const python = getCommandSpec('python')!;
  assert.equal(calculateSpecDepth(['-m', 'http.server'], python), 3);
  const node = getCommandSpec('node')!;
  assert.equal(calculateSpecDepth(['-e', 'x'], node), 3);
});

test('calculateSpecDepth: a single positional isCommand → depth 2', () => {
  const spec: CommandSpec = { name: 'wrap', args: { name: 'cmd', isCommand: true } };
  assert.equal(calculateSpecDepth([], spec), 2);
});

test('calculateSpecDepth: an array positional isCommand → depth to that arg (capped 3)', () => {
  const atZero: CommandSpec = { name: 'w', args: [{ name: 'c', isCommand: true }] };
  assert.equal(calculateSpecDepth([], atZero), 2);
  const atOne: CommandSpec = { name: 'w', args: [{ name: 'dur' }, { name: 'c', isCommand: true }] };
  assert.equal(calculateSpecDepth([], atOne), 3);
});

test('calculateSpecDepth: a variadic leading positional → depth 1', () => {
  const node = getCommandSpec('node')!;
  assert.equal(calculateSpecDepth(['app.js'], node), 1);
});

test('calculateSpecDepth: a required (non-optional) leading positional → depth 2', () => {
  const spec: CommandSpec = { name: 'sleep', args: { name: 'duration', isOptional: false } };
  assert.equal(calculateSpecDepth([], spec), 2);
});

test('calculateSpecDepth: a dangerous positional → depth 3; otherwise 2', () => {
  const dangerous: CommandSpec = { name: 'x', args: { name: 'a', isOptional: true, isDangerous: true } };
  assert.equal(calculateSpecDepth([], dangerous), 3);
  const plain: CommandSpec = { name: 'x', args: { name: 'a', isOptional: true } };
  assert.equal(calculateSpecDepth([], plain), 2);
  const noArgs: CommandSpec = { name: 'x' };
  assert.equal(calculateSpecDepth([], noArgs), 2);
});

// --- gap 5: buildSpecPrefix folds -m <module>, stops at -c <code> -------------

test('buildSpecPrefix: folds a module flag + module name into the prefix', () => {
  const python = getCommandSpec('python')!;
  assert.equal(buildSpecPrefix('python', ['-m', 'http.server'], python), 'python -m http.server');
});

test('buildSpecPrefix: stops at an inline-code flag (code never folded in)', () => {
  const python = getCommandSpec('python')!;
  assert.equal(buildSpecPrefix('python', ['-c', 'import os; os.system("x")'], python), 'python');
  const node = getCommandSpec('node')!;
  assert.equal(buildSpecPrefix('node', ['-e', 'process.exit(1)'], node), 'node');
});

test('buildSpecPrefix: stops at a file/script operand', () => {
  const python = getCommandSpec('python')!;
  assert.equal(buildSpecPrefix('python', ['manage.py', 'migrate'], python), 'python');
});

test('buildSpecPrefix: stops at any other flag', () => {
  const python = getCommandSpec('python')!;
  assert.equal(buildSpecPrefix('python', ['-X', 'faulthandler', 'foo'], python), 'python');
});

test('buildSpecPrefix: caps at the computed depth', () => {
  const python = getCommandSpec('python')!;
  // depth 3 (module option) → module folds, trailing arg dropped.
  assert.equal(buildSpecPrefix('python', ['-m', 'http.server', 'extra'], python), 'python -m http.server');
});

test('buildSpecPrefix: stops at a URL operand (shouldStopAtArg url branch)', () => {
  // A synthetic high-depth spec so extraction reaches the URL token.
  const spec: CommandSpec = { name: 'tool', args: { name: 'a', isDangerous: true } };
  assert.equal(buildSpecPrefix('tool', ['fetch', 'https://evil/x'], spec), 'tool fetch');
});

// --- isScopedModulePrefix discriminates the safe module form ------------------

test('isScopedModulePrefix: true only for <interpreter> <moduleFlag> <module>', () => {
  assert.equal(isScopedModulePrefix('python -m http.server'), true);
  assert.equal(isScopedModulePrefix('python3 -m venv'), true);
  // wrong arity
  assert.equal(isScopedModulePrefix('python -m'), false);
  assert.equal(isScopedModulePrefix('python -m a b'), false);
  // flag is not a module flag
  assert.equal(isScopedModulePrefix('python -c foo'), false);
  // unknown command / non-interpreter
  assert.equal(isScopedModulePrefix('foo -m bar'), false);
  assert.equal(isScopedModulePrefix('git -C status'), false);
});

// --- integration: getSimpleCommandPrefix (permissions.ts) --------------------

test('getSimpleCommandPrefix: folds python -m <module> into a scoped prefix', () => {
  assert.equal(getSimpleCommandPrefix('python -m http.server'), 'python -m http.server');
  assert.equal(getSimpleCommandPrefix('python3 -m venv'), 'python3 -m venv');
});

test('getSimpleCommandPrefix: inline code / bare script / bare interpreter → null', () => {
  assert.equal(getSimpleCommandPrefix('python -c "import os"'), null);
  assert.equal(getSimpleCommandPrefix('python app.py'), null);
  assert.equal(getSimpleCommandPrefix('node -e "code"'), null);
  assert.equal(getSimpleCommandPrefix('node app.js'), null);
});

// --- integration: suggestAllowPrefix (the wired suggestion entry point) ------

test('suggestAllowPrefix: offers a scoped module-exec rule for python -m', () => {
  assert.equal(suggestAllowPrefix('python -m http.server'), 'python -m http.server');
  assert.equal(suggestAllowPrefix('python3 -m pytest'), 'python3 -m pytest');
});

test('suggestAllowPrefix: never offers a rule for interpreter inline code', () => {
  assert.equal(suggestAllowPrefix('python -c "import os"'), null);
  assert.equal(suggestAllowPrefix('node -e "process.exit(1)"'), null);
  // bare interpreter run of a script stays unsuggestable (bare-shell guard).
  assert.equal(suggestAllowPrefix('python manage.py migrate'), null);
});

test('suggestAllowPrefix: composes the module form with a bounded wrapper chain', () => {
  assert.equal(suggestAllowPrefix('nice python -m http.server'), 'nice python -m http.server');
});

test('suggestAllowPrefix: regression — nice node -e stays null (wrapper + inline code)', () => {
  assert.equal(suggestAllowPrefix('nice node -e "process.exit(1)"'), null);
});

// --- e2e: the suggested rule actually gates the command through the manager ---

test('e2e: the derived python -m rule allows the exact module run but not -c code', async () => {
  const prefix = suggestAllowPrefix('python -m http.server');
  // RED on revert: without the fix this is null and the rule below is never built.
  assert.equal(prefix, 'python -m http.server');

  const pm = new PermissionManager();
  pm.addRule({ tool: 'bash', pattern: prefix!, action: 'allow' });

  // The scoped rule auto-allows the exact module invocation.
  assert.equal(await pm.check('bash', { command: 'python -m http.server' }), 'allow');
  // It must NOT auto-allow an arbitrary inline-code run through the same interpreter.
  assert.notEqual(await pm.check('bash', { command: 'python -c "import os; os.system(\'rm -rf /\')"' }), 'allow');
});
