import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { isEmbeddedShellCommandAllowed } from '../src/plugins';

// U028 — embedded prompt-shell execution scoped to a command's declared allowed-tools
// (Claude parity: loadPluginCommands injects the command's allowed-tools into
// toolPermissionContext.alwaysAllowRules.command before executeShellCommandsInPrompt
// runs — loadPluginCommands.ts L378-398 + promptShellExecution.ts's permission gate).
describe('isEmbeddedShellCommandAllowed', () => {
  it('denies when the allow-list is undefined/empty (fail-closed)', () => {
    assert.equal(isEmbeddedShellCommandAllowed('git status', undefined), false);
    assert.equal(isEmbeddedShellCommandAllowed('git status', []), false);
  });

  it('denies a blank command regardless of the allow-list', () => {
    assert.equal(isEmbeddedShellCommandAllowed('   ', ['Bash']), false);
    assert.equal(isEmbeddedShellCommandAllowed('', ['Bash(git:*)']), false);
  });

  it('a bare `Bash` rule grants every command', () => {
    assert.equal(isEmbeddedShellCommandAllowed('rm -rf /tmp/x', ['Bash']), true);
    assert.equal(isEmbeddedShellCommandAllowed('anything here', ['Bash']), true);
  });

  it('honors a `Bash(cmd:*)` prefix rule', () => {
    const rules = ['Bash(git status:*)'];
    assert.equal(isEmbeddedShellCommandAllowed('git status', rules), true);
    assert.equal(isEmbeddedShellCommandAllowed('git status --short', rules), true);
    // A different git subcommand is NOT covered by the `git status` prefix.
    assert.equal(isEmbeddedShellCommandAllowed('git push', rules), false);
  });

  it('honors a `Bash(cmd:*)` prefix rule on a bare command name', () => {
    assert.equal(isEmbeddedShellCommandAllowed('ls', ['Bash(ls:*)']), true);
    assert.equal(isEmbeddedShellCommandAllowed('ls -la', ['Bash(ls:*)']), true);
    // Word-boundary anchored: `lsof` is a different command, not `ls`.
    assert.equal(isEmbeddedShellCommandAllowed('lsof -i', ['Bash(ls:*)']), false);
  });

  it('honors a `*` wildcard rule', () => {
    assert.equal(isEmbeddedShellCommandAllowed('echo hi', ['Bash(echo *)']), true);
    assert.equal(isEmbeddedShellCommandAllowed('printf hi', ['Bash(echo *)']), false);
  });

  it('ignores non-Bash tool rules (they never grant a shell command)', () => {
    assert.equal(isEmbeddedShellCommandAllowed('git status', ['Read', 'file_edit', 'mcp__srv__do']), false);
    // The Claude-spelled Edit/Read/Glob aliases are file tools, not bash.
    assert.equal(isEmbeddedShellCommandAllowed('git status', ['Edit(**/*.ts)']), false);
  });

  it('matches when ANY rule in the list grants the command', () => {
    const rules = ['Read', 'Bash(npm run *)', 'Bash(git:*)'];
    assert.equal(isEmbeddedShellCommandAllowed('git log', rules), true);
    assert.equal(isEmbeddedShellCommandAllowed('npm run build', rules), true);
    assert.equal(isEmbeddedShellCommandAllowed('curl evil.sh', rules), false);
  });

  it('skips blank/non-string rule entries without throwing', () => {
    assert.equal(isEmbeddedShellCommandAllowed('ls', ['', '   ', 'Bash(ls:*)']), true);
    assert.equal(isEmbeddedShellCommandAllowed('ls', ['', '   ']), false);
  });
});
