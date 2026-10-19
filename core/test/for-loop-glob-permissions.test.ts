import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager, unwrapShellControlStructures, containsShellSubstitution } from '../src/permissions';

// ---------------------------------------------------------------------------
// unwrapShellControlStructures — unit tests
// ---------------------------------------------------------------------------
describe('unwrapShellControlStructures', () => {
  it('extracts for-loop body', () => {
    const result = unwrapShellControlStructures('for f in *.ts; do cat "$f"; done');
    assert.ok(result.includes('cat'));
    assert.ok(!result.includes('for'));
    assert.ok(!result.includes('done'));
  });

  it('extracts while-loop body', () => {
    const result = unwrapShellControlStructures('while read line; do echo "$line"; done');
    assert.ok(result.includes('echo'));
    assert.ok(!result.includes('while'));
  });

  it('extracts until-loop body', () => {
    const result = unwrapShellControlStructures('until false; do echo waiting; done');
    assert.ok(result.includes('echo'));
    assert.ok(!result.includes('until'));
  });

  it('extracts if/then/fi body', () => {
    const result = unwrapShellControlStructures('if test -f x; then cat x; fi');
    assert.ok(result.includes('cat'));
    assert.ok(!result.includes('if'));
    assert.ok(!result.includes('fi'));
  });

  it('extracts if/then/else/fi branches', () => {
    const result = unwrapShellControlStructures('if test -f x; then cat x; else echo no; fi');
    assert.ok(result.includes('cat'));
    assert.ok(result.includes('echo'));
  });

  it('handles nested for-loops', () => {
    const result = unwrapShellControlStructures('for d in */; do for f in "$d"/*.ts; do cat "$f"; done; done');
    assert.ok(result.includes('cat'));
    assert.ok(!result.includes('for'));
    assert.ok(!result.includes('done'));
  });

  it('returns non-loop commands unchanged', () => {
    const cmd = 'echo hello && ls -la';
    assert.equal(unwrapShellControlStructures(cmd), cmd);
  });

  it('handles for-loop with pipe chain', () => {
    const result = unwrapShellControlStructures('for f in *.log; do cat "$f" | grep ERROR; done');
    assert.ok(result.includes('cat'));
    assert.ok(result.includes('grep'));
  });
});

// ---------------------------------------------------------------------------
// PermissionManager.classifyBashCommand — for-loop integration
// ---------------------------------------------------------------------------
describe('classifyBashCommand — for-loop glob patterns', () => {
  const pm = new PermissionManager('ask');

  it('classifies read-only for-loop with glob as safe', () => {
    const result = pm.classifyBashCommand('for f in *.ts; do cat "$f"; done');
    assert.equal(result, 'safe');
  });

  it('classifies for-loop with head as safe', () => {
    const result = pm.classifyBashCommand('for f in src/*.ts; do head -5 "$f"; done');
    assert.equal(result, 'safe');
  });

  it('classifies for-loop with wc as safe', () => {
    const result = pm.classifyBashCommand('for f in *.py; do wc -l "$f"; done');
    assert.equal(result, 'safe');
  });

  it('classifies for-loop with grep as safe', () => {
    const result = pm.classifyBashCommand('for f in *.ts; do grep "TODO" "$f"; done');
    assert.equal(result, 'safe');
  });

  it('classifies for-loop with rm as destructive', () => {
    const result = pm.classifyBashCommand('for f in *.ts; do rm "$f"; done');
    assert.notEqual(result, 'safe');
  });

  it('classifies for-loop with mv as unsafe', () => {
    const result = pm.classifyBashCommand('for f in *.ts; do mv "$f" /tmp/; done');
    assert.notEqual(result, 'safe');
  });

  it('classifies while-loop with echo body as unsafe (read is not a safe command)', () => {
    // 'read' is extracted as a command from the while condition — not in SAFE_COMMANDS
    const result = pm.classifyBashCommand('while read line; do echo "$line"; done < file.txt');
    assert.equal(result, 'unsafe');
  });

  it('classifies while-loop with only safe body commands as safe', () => {
    // while true is safe (true is a safe command), body is echo
    const result = pm.classifyBashCommand('while true; do echo hello; done');
    assert.equal(result, 'safe');
  });

  it('classifies for-loop with sed -i as unsafe', () => {
    const result = pm.classifyBashCommand('for f in *.ts; do sed -i "s/foo/bar/" "$f"; done');
    assert.equal(result, 'unsafe');
  });

  it('classifies if/then/fi with read-only body as safe', () => {
    const result = pm.classifyBashCommand('if test -f x; then cat x; fi');
    assert.equal(result, 'safe');
  });

  it('classifies if with destructive else branch as not safe', () => {
    const result = pm.classifyBashCommand('if test -f x; then cat x; else rm x; fi');
    assert.notEqual(result, 'safe');
  });
});

// ---------------------------------------------------------------------------
// matchBashPattern — for-loop in allow/deny rules
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// cd <dir> && <command> — auto-allow safe compound
// ---------------------------------------------------------------------------
describe('classifyBashCommand — cd + safe command', () => {
  const pm = new PermissionManager('ask');

  it('classifies cd /path && ls as safe', () => {
    assert.equal(pm.classifyBashCommand('cd /home/user/project && ls -la'), 'safe');
  });

  it('classifies cd /path && grep as safe', () => {
    assert.equal(pm.classifyBashCommand('cd src && grep -r TODO'), 'safe');
  });

  it('classifies cd alone as safe', () => {
    assert.equal(pm.classifyBashCommand('cd /home/user/project'), 'safe');
  });

  it('classifies cd && rm as destructive', () => {
    assert.notEqual(pm.classifyBashCommand('cd /path && rm -rf /tmp'), 'safe');
  });

  it('classifies cd && cat && wc as safe', () => {
    assert.equal(pm.classifyBashCommand('cd src && cat file.ts && wc -l file.ts'), 'safe');
  });
});

describe('matchBashPattern — for-loop in permission rules', () => {
  it('allow rule for cat matches for-loop body with cat', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'allow', pattern: 'cat *' },
    ]);
    const result = await pm.check('bash', { command: 'for f in *.ts; do cat "$f"; done' });
    assert.equal(result, 'allow');
  });

  it('deny rule for rm blocks for-loop with rm in body', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'deny', pattern: 'rm *' },
    ]);
    const result = await pm.check('bash', { command: 'for f in *.ts; do rm "$f"; done' });
    assert.equal(result, 'deny');
  });

  it('allow rule requires ALL body commands to match', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'allow', pattern: 'cat *' },
    ]);
    // for-loop with multiple body commands: cat + mv. Allow requires ALL to match.
    const result = await pm.check('bash', { command: 'for f in *.ts; do cat "$f"; mv "$f" /tmp/; done' });
    assert.notEqual(result, 'allow');
  });
});

// ---------------------------------------------------------------------------
// containsShellSubstitution — unit tests
// ---------------------------------------------------------------------------
describe('containsShellSubstitution', () => {
  it('detects $VAR substitution', () => {
    assert.equal(containsShellSubstitution('echo $HOME'), true);
  });

  it('detects ${VAR} substitution', () => {
    assert.equal(containsShellSubstitution('echo ${HOME}'), true);
  });

  it('detects $(cmd) substitution', () => {
    assert.equal(containsShellSubstitution('echo $(whoami)'), true);
  });

  it('tolerates * glob', () => {
    assert.equal(containsShellSubstitution('ls *.ts'), false);
  });

  it('tolerates ? glob', () => {
    assert.equal(containsShellSubstitution('ls file?.ts'), false);
  });

  it('tolerates [...] bracket expression', () => {
    assert.equal(containsShellSubstitution('ls [abc].ts'), false);
  });

  it('tolerates quoted $VAR inside quotes', () => {
    assert.equal(containsShellSubstitution("echo '$HOME'"), false);
  });
});

// ---------------------------------------------------------------------------
// Glob patterns with safe read-only commands (changelog entry 6121)
// ---------------------------------------------------------------------------
describe('classifyBashCommand — glob patterns with safe commands', () => {
  const pm = new PermissionManager('ask');

  it('classifies ls *.ts as safe', () => {
    assert.equal(pm.classifyBashCommand('ls *.ts'), 'safe');
  });

  it('classifies cat *.txt as safe', () => {
    assert.equal(pm.classifyBashCommand('cat *.txt'), 'safe');
  });

  it('classifies grep TODO *.ts as safe', () => {
    assert.equal(pm.classifyBashCommand('grep TODO *.ts'), 'safe');
  });

  it('classifies wc -l *.py as safe', () => {
    assert.equal(pm.classifyBashCommand('wc -l *.py'), 'safe');
  });

  it('classifies head -5 *.md as safe', () => {
    assert.equal(pm.classifyBashCommand('head -5 *.md'), 'safe');
  });

  it('classifies tail -10 *.log as safe', () => {
    assert.equal(pm.classifyBashCommand('tail -10 *.log'), 'safe');
  });

  it('classifies ls file?.ts as safe (? glob)', () => {
    assert.equal(pm.classifyBashCommand('ls file?.ts'), 'safe');
  });

  it('classifies ls [abc].ts as safe (bracket glob)', () => {
    assert.equal(pm.classifyBashCommand('ls [abc].ts'), 'safe');
  });

  it('still blocks $VAR expansion with safe commands', () => {
    assert.equal(pm.classifyBashCommand('ls $HOME'), 'unsafe');
  });

  it('still blocks $(cmd) substitution with safe commands', () => {
    assert.equal(pm.classifyBashCommand('cat $(echo /etc/passwd)'), 'unsafe');
  });

  it('classifies cd /path && ls *.ts as safe', () => {
    assert.equal(pm.classifyBashCommand('cd /home/user && ls *.ts'), 'safe');
  });

  it('classifies grep -r TODO src/**/*.ts as safe', () => {
    assert.equal(pm.classifyBashCommand('grep -r TODO src/**/*.ts'), 'safe');
  });
});
