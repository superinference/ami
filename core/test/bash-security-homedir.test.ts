import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { expandHomeDirReferences } from '../src/tools/bash-security';

describe('expandHomeDirReferences', () => {
  const HOME = '/home/testuser';

  // --- $HOME expansion ---

  it('expands $HOME/ to the actual home directory', () => {
    assert.equal(
      expandHomeDirReferences('cat $HOME/secrets/keys.txt', HOME),
      'cat /home/testuser/secrets/keys.txt',
    );
  });

  it('expands $HOME at end of command', () => {
    assert.equal(
      expandHomeDirReferences('ls $HOME', HOME),
      'ls /home/testuser',
    );
  });

  it('expands $HOME followed by space', () => {
    assert.equal(
      expandHomeDirReferences('ls $HOME && pwd', HOME),
      'ls /home/testuser && pwd',
    );
  });

  it('expands multiple $HOME references', () => {
    assert.equal(
      expandHomeDirReferences('cp $HOME/a.txt $HOME/b.txt', HOME),
      'cp /home/testuser/a.txt /home/testuser/b.txt',
    );
  });

  it('does NOT expand $HOME inside single quotes', () => {
    assert.equal(
      expandHomeDirReferences("echo '$HOME/secrets'", HOME),
      "echo '$HOME/secrets'",
    );
  });

  it('does NOT expand $HOME inside double quotes', () => {
    // In bash, $HOME DOES expand inside double quotes, but for security
    // deny-rule matching, we only expand unquoted references to avoid
    // false positives on echo/printf patterns.
    assert.equal(
      expandHomeDirReferences('echo "$HOME/secrets"', HOME),
      'echo "$HOME/secrets"',
    );
  });

  it('does NOT expand $HOMEDIR or $HOMEPAGE (partial match)', () => {
    assert.equal(
      expandHomeDirReferences('echo $HOMEPAGE', HOME),
      'echo $HOMEPAGE',
    );
    assert.equal(
      expandHomeDirReferences('echo $HOMEDIR', HOME),
      'echo $HOMEDIR',
    );
  });

  // --- ${HOME} expansion ---

  it('expands ${HOME}/ to the actual home directory', () => {
    assert.equal(
      expandHomeDirReferences('cat ${HOME}/secrets/keys.txt', HOME),
      'cat /home/testuser/secrets/keys.txt',
    );
  });

  it('expands ${HOME} inside a compound command', () => {
    assert.equal(
      expandHomeDirReferences('test -f ${HOME}/.ssh/id_rsa && cat ${HOME}/.ssh/id_rsa', HOME),
      'test -f /home/testuser/.ssh/id_rsa && cat /home/testuser/.ssh/id_rsa',
    );
  });

  it('does NOT expand ${HOME} inside single quotes', () => {
    assert.equal(
      expandHomeDirReferences("echo '${HOME}/secrets'", HOME),
      "echo '${HOME}/secrets'",
    );
  });

  // --- ~ expansion ---

  it('expands ~/ to the actual home directory', () => {
    assert.equal(
      expandHomeDirReferences('cat ~/secrets/keys.txt', HOME),
      'cat /home/testuser/secrets/keys.txt',
    );
  });

  it('expands ~/ after a semicolon', () => {
    assert.equal(
      expandHomeDirReferences('echo hello; cat ~/secrets/keys.txt', HOME),
      'echo hello; cat /home/testuser/secrets/keys.txt',
    );
  });

  it('expands ~/ after a pipe', () => {
    assert.equal(
      expandHomeDirReferences('cat ~/data.txt | grep secret', HOME),
      'cat /home/testuser/data.txt | grep secret',
    );
  });

  it('expands ~/ after &&', () => {
    assert.equal(
      expandHomeDirReferences('cd ~/projects && ls', HOME),
      'cd /home/testuser/projects && ls',
    );
  });

  it('expands ~/ in a variable assignment', () => {
    assert.equal(
      expandHomeDirReferences('DIR=~/secrets && ls $DIR', HOME),
      'DIR=/home/testuser/secrets && ls $DIR',
    );
  });

  it('does NOT expand ~user/path (different user tilde)', () => {
    assert.equal(
      expandHomeDirReferences('cat ~root/secrets', HOME),
      'cat ~root/secrets',
    );
    assert.equal(
      expandHomeDirReferences('cat ~alice/file', HOME),
      'cat ~alice/file',
    );
  });

  it('does NOT expand ~ inside single quotes', () => {
    assert.equal(
      expandHomeDirReferences("echo '~/secrets'", HOME),
      "echo '~/secrets'",
    );
  });

  it('does NOT expand ~ inside double quotes', () => {
    assert.equal(
      expandHomeDirReferences('echo "~/secrets"', HOME),
      'echo "~/secrets"',
    );
  });

  it('does NOT expand bare ~ without following /', () => {
    assert.equal(
      expandHomeDirReferences('echo ~', HOME),
      'echo ~',
    );
  });

  // --- Edge cases ---

  it('returns command unchanged when HOME is empty string', () => {
    assert.equal(
      expandHomeDirReferences('cat ~/secrets', ''),
      'cat ~/secrets',
    );
  });

  it('falls back to process.env.HOME when homeDir is undefined', () => {
    const result = expandHomeDirReferences('cat ~/secrets', undefined);
    if (process.env.HOME) {
      assert.equal(result, `cat ${process.env.HOME}/secrets`);
    } else {
      assert.equal(result, 'cat ~/secrets');
    }
  });

  it('handles empty command', () => {
    assert.equal(expandHomeDirReferences('', HOME), '');
  });

  it('handles command with no home references', () => {
    assert.equal(
      expandHomeDirReferences('ls /tmp/foo', HOME),
      'ls /tmp/foo',
    );
  });

  it('handles escaped dollar sign', () => {
    assert.equal(
      expandHomeDirReferences('echo \\$HOME/secrets', HOME),
      'echo \\$HOME/secrets',
    );
  });

  it('handles mixed quoted and unquoted references', () => {
    assert.equal(
      expandHomeDirReferences('cat ~/file && echo "$HOME/file"', HOME),
      'cat /home/testuser/file && echo "$HOME/file"',
    );
  });

  it('handles subshell with tilde', () => {
    assert.equal(
      expandHomeDirReferences('(cat ~/secrets)', HOME),
      '(cat /home/testuser/secrets)',
    );
  });

  it('handles path with special chars after HOME', () => {
    assert.equal(
      expandHomeDirReferences('cat $HOME/.config/secret.json', HOME),
      'cat /home/testuser/.config/secret.json',
    );
  });

  it('expands in a redirect context', () => {
    assert.equal(
      expandHomeDirReferences('cmd < ~/input.txt', HOME),
      'cmd < /home/testuser/input.txt',
    );
  });
});
