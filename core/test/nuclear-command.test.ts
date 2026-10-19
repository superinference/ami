import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectNuclearCommand, detectUnsafeFileCommand, detectShellStartupWrite, extractInputRedirectionPaths } from '../src/tools/bash-security';

describe('detectNuclearCommand', () => {
  // -----------------------------------------------------------------------
  // rm -rf / variants — MUST ALL BE BLOCKED
  // -----------------------------------------------------------------------

  describe('rm -rf root filesystem', () => {
    const mustBlock = [
      'rm -rf /',
      'rm -rf //',
      'rm -rf ///',
      'rm -rf /*',
      'rm -r -f /',
      'rm -f -r /',
      'rm --recursive --force /',
      'rm --force --recursive /',
      'rm -rf /',
      'rm  -rf  /',
      'rm -rfi /',
    ];
    for (const cmd of mustBlock) {
      it(`blocks: ${cmd}`, () => {
        const result = detectNuclearCommand(cmd);
        assert.equal(result.blocked, true, `should block: ${cmd}`);
        assert.ok(result.reason!.includes('rm -rf'));
      });
    }
  });

  describe('rm -rf critical system directories', () => {
    const systemDirs = ['/home', '/root', '/etc', '/usr', '/var', '/bin', '/sbin', '/lib', '/lib64', '/boot', '/opt', '/sys', '/proc', '/dev'];
    for (const dir of systemDirs) {
      it(`blocks: rm -rf ${dir}`, () => {
        const result = detectNuclearCommand(`rm -rf ${dir}`);
        assert.equal(result.blocked, true, `should block: rm -rf ${dir}`);
      });
    }
  });

  describe('rm -rf home directory expansions', () => {
    const homeTargets = [
      'rm -rf ~',
      'rm -rf ~/',
      'rm -rf $HOME',
      'rm -rf $HOME/',
      'rm -rf ${HOME}',
      'rm -rf ${HOME}/',
    ];
    for (const cmd of homeTargets) {
      it(`blocks: ${cmd}`, () => {
        const result = detectNuclearCommand(cmd);
        assert.equal(result.blocked, true, `should block: ${cmd}`);
        assert.ok(result.reason!.includes('home directory'));
      });
    }
  });

  describe('rm with quoted targets', () => {
    it('blocks: rm -rf "/"', () => {
      const result = detectNuclearCommand('rm -rf "/"');
      assert.equal(result.blocked, true);
    });
    it("blocks: rm -rf '/'", () => {
      const result = detectNuclearCommand("rm -rf '/'");
      assert.equal(result.blocked, true);
    });
  });

  describe('rm with command prefixes (evasion)', () => {
    it('blocks: sudo rm -rf /', () => {
      const result = detectNuclearCommand('sudo rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: sudo -u root rm -rf /', () => {
      const result = detectNuclearCommand('sudo -u root rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: env rm -rf /', () => {
      const result = detectNuclearCommand('env rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: command rm -rf /', () => {
      const result = detectNuclearCommand('command rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: /bin/rm -rf /', () => {
      const result = detectNuclearCommand('/bin/rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: /usr/bin/rm -rf /', () => {
      const result = detectNuclearCommand('/usr/bin/rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: \\rm -rf /', () => {
      const result = detectNuclearCommand('\\rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: watch rm -rf /', () => {
      const result = detectNuclearCommand('watch rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: watch -n 5 rm -rf /', () => {
      const result = detectNuclearCommand('watch -n 5 rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: ionice -c2 rm -rf /', () => {
      const result = detectNuclearCommand('ionice -c2 rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: setsid rm -rf /', () => {
      const result = detectNuclearCommand('setsid rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: timeout 30 rm -rf /', () => {
      const result = detectNuclearCommand('timeout 30 rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: stdbuf -oL rm -rf /', () => {
      const result = detectNuclearCommand('stdbuf -oL rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: nice -n 19 rm -rf /', () => {
      const result = detectNuclearCommand('nice -n 19 rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: nohup rm -rf /', () => {
      const result = detectNuclearCommand('nohup rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: script -c "rm -rf /"', () => {
      const result = detectNuclearCommand('script -c "rm -rf /"');
      assert.equal(result.blocked, true);
    });
    it('blocks: sudo nice -n 19 rm -rf /', () => {
      const result = detectNuclearCommand('sudo nice -n 19 rm -rf /');
      assert.equal(result.blocked, true);
    });
  });

  describe('rm in compound commands', () => {
    it('blocks: echo hi; rm -rf /', () => {
      const result = detectNuclearCommand('echo hi; rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: cd /tmp && rm -rf /', () => {
      const result = detectNuclearCommand('cd /tmp && rm -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks: true || rm -rf /', () => {
      const result = detectNuclearCommand('true || rm -rf /');
      assert.equal(result.blocked, true);
    });
  });

  // -----------------------------------------------------------------------
  // Disk wipe commands — MUST ALL BE BLOCKED
  // -----------------------------------------------------------------------

  describe('disk wipe commands', () => {
    it('blocks: mkfs /dev/sda', () => {
      const result = detectNuclearCommand('mkfs /dev/sda');
      assert.equal(result.blocked, true);
      assert.ok(result.reason!.includes('format'));
    });
    it('blocks: mkfs.ext4 /dev/sda1', () => {
      const result = detectNuclearCommand('mkfs.ext4 /dev/sda1');
      assert.equal(result.blocked, true);
    });
    it('blocks: mkfs.xfs /dev/nvme0n1', () => {
      const result = detectNuclearCommand('mkfs.xfs /dev/nvme0n1');
      assert.equal(result.blocked, true);
    });
    it('blocks: wipefs -a /dev/sda', () => {
      const result = detectNuclearCommand('wipefs -a /dev/sda');
      assert.equal(result.blocked, true);
    });
    it('blocks: shred /dev/sda', () => {
      const result = detectNuclearCommand('shred /dev/sda');
      assert.equal(result.blocked, true);
    });
    it('blocks: sudo mkfs.ext4 /dev/sda1', () => {
      const result = detectNuclearCommand('sudo mkfs.ext4 /dev/sda1');
      assert.equal(result.blocked, true);
    });
    it('blocks: dd if=/dev/zero of=/dev/sda', () => {
      const result = detectNuclearCommand('dd if=/dev/zero of=/dev/sda');
      assert.equal(result.blocked, true);
      assert.ok(result.reason!.includes('dd'));
    });
    it('blocks: dd if=/dev/urandom of=/dev/sda bs=1M', () => {
      const result = detectNuclearCommand('dd if=/dev/urandom of=/dev/sda bs=1M');
      assert.equal(result.blocked, true);
    });
    it('blocks: dd if=/dev/zero of=/dev/nvme0n1', () => {
      const result = detectNuclearCommand('dd if=/dev/zero of=/dev/nvme0n1');
      assert.equal(result.blocked, true);
    });
    it('blocks: dd if=/dev/zero of=/dev/mapper/root', () => {
      const result = detectNuclearCommand('dd if=/dev/zero of=/dev/mapper/root');
      assert.equal(result.blocked, true);
    });
  });

  // -----------------------------------------------------------------------
  // Fork bomb — MUST BE BLOCKED
  // -----------------------------------------------------------------------

  describe('fork bomb', () => {
    it('blocks: :(){ :|:& };:', () => {
      const result = detectNuclearCommand(':(){ :|:& };:');
      assert.equal(result.blocked, true);
      assert.ok(result.reason!.includes('fork bomb'));
    });
    it('blocks fork bomb with spaces', () => {
      const result = detectNuclearCommand(':() { : | : & } ; :');
      assert.equal(result.blocked, true);
    });
  });

  // -----------------------------------------------------------------------
  // SAFE commands — MUST NOT be blocked (no false positives)
  // -----------------------------------------------------------------------

  describe('safe commands not blocked', () => {
    const safeCommands = [
      'rm -rf node_modules/',
      'rm -rf /tmp/test-dir',
      'rm -rf ./build',
      'rm -rf dist/',
      'rm file.txt',
      'rm -f output.log',
      'rm -r /home/user/project/build/',
      'ls -la /',
      'cat /etc/hostname',
      'echo hello',
      'cd /',
      'find / -name "*.log"',
      'dd if=/dev/zero of=./test.img bs=1M count=10',
      'dd if=input.bin of=output.bin',
      'mkfs',
      'shred tempfile.txt',
      'npm run build',
      'git push origin main',
      'sudo apt update',
      'sudo systemctl restart nginx',
      'rm -rf /var/log/myapp/',
    ];
    for (const cmd of safeCommands) {
      it(`allows: ${cmd}`, () => {
        const result = detectNuclearCommand(cmd);
        assert.equal(result.blocked, false, `should allow: ${cmd}`);
      });
    }
  });

  // -----------------------------------------------------------------------
  // Edge cases
  // -----------------------------------------------------------------------

  describe('edge cases', () => {
    it('handles empty string', () => {
      const result = detectNuclearCommand('');
      assert.equal(result.blocked, false);
    });
    it('handles whitespace-only', () => {
      const result = detectNuclearCommand('   ');
      assert.equal(result.blocked, false);
    });
    it('handles null-ish input', () => {
      const result = detectNuclearCommand(undefined as any);
      assert.equal(result.blocked, false);
    });
    it('blocks rm -rf / with invisible unicode characters', () => {
      const result = detectNuclearCommand('rm​ -rf /');
      assert.equal(result.blocked, true);
    });
    it('blocks rm -rf / with trailing slash', () => {
      const result = detectNuclearCommand('rm -rf //');
      assert.equal(result.blocked, true);
    });
    it('does not block rm without -r flag', () => {
      const result = detectNuclearCommand('rm -f /somefile');
      assert.equal(result.blocked, false);
    });
    it('result has reason when blocked', () => {
      const result = detectNuclearCommand('rm -rf /');
      assert.equal(result.blocked, true);
      assert.equal(typeof result.reason, 'string');
      assert.ok(result.reason!.length > 10);
    });
    it('result has no reason when not blocked', () => {
      const result = detectNuclearCommand('ls');
      assert.equal(result.reason, undefined);
    });
  });

  // -----------------------------------------------------------------------
  // Nested shell commands: bash -c / sh -c
  // -----------------------------------------------------------------------

  describe('nested shell commands (bash -c / sh -c)', () => {
    it('blocks rm -rf / inside bash -c with double quotes', () => {
      const result = detectNuclearCommand('bash -c "rm -rf /"');
      assert.equal(result.blocked, true);
    });

    it('blocks rm -rf / inside sh -c with single quotes', () => {
      const result = detectNuclearCommand("sh -c 'rm -rf /'");
      assert.equal(result.blocked, true);
    });

    it('blocks rm -rf / inside bash -c with compound commands', () => {
      const result = detectNuclearCommand('bash -c "echo hi; rm -rf /"');
      assert.equal(result.blocked, true);
    });

    it('blocks dd wipe inside bash -c', () => {
      const result = detectNuclearCommand('bash -c "dd if=/dev/zero of=/dev/sda"');
      assert.equal(result.blocked, true);
    });

    it('allows safe commands inside bash -c', () => {
      const result = detectNuclearCommand('bash -c "echo hello && ls -la"');
      assert.equal(result.blocked, false);
    });

    it('allows bash -c with safe rm', () => {
      const result = detectNuclearCommand('bash -c "rm -rf /tmp/mydir"');
      assert.equal(result.blocked, false);
    });

    it('blocks nuclear command inside bash -c with $-quote', () => {
      const result = detectNuclearCommand("bash -c $'rm -rf /'");
      assert.equal(result.blocked, true);
    });

    it('blocks nuclear command prefixed before bash -c', () => {
      const result = detectNuclearCommand('echo start; bash -c "rm -rf /"');
      assert.equal(result.blocked, true);
    });

    it('blocks mkfs inside sh -c', () => {
      const result = detectNuclearCommand('sh -c "mkfs.ext4 /dev/sda1"');
      assert.equal(result.blocked, true);
    });

    it('blocks rm -rf /home via nested bash -c', () => {
      const result = detectNuclearCommand('bash -c "rm -rf /home"');
      assert.equal(result.blocked, true);
    });
  });

  // -----------------------------------------------------------------------
  // Inline env var prefix evasion — VAR=val rm -rf /
  // -----------------------------------------------------------------------

  describe('rm with inline env var prefix (evasion)', () => {
    it('blocks: TZ=UTC rm -rf /', () => {
      const result = detectNuclearCommand('TZ=UTC rm -rf /');
      assert.equal(result.blocked, true);
    });

    it('blocks: TZ="$HOME" rm -rf /', () => {
      const result = detectNuclearCommand('TZ="$HOME" rm -rf /');
      assert.equal(result.blocked, true);
    });

    it("blocks: LANG='C' rm -rf /", () => {
      const result = detectNuclearCommand("LANG='C' rm -rf /");
      assert.equal(result.blocked, true);
    });

    it('blocks: FOO=bar BAZ=qux rm -rf /', () => {
      const result = detectNuclearCommand('FOO=bar BAZ=qux rm -rf /');
      assert.equal(result.blocked, true);
    });

    it('blocks: env FOO=bar rm -rf /', () => {
      const result = detectNuclearCommand('env FOO=bar rm -rf /');
      assert.equal(result.blocked, true);
    });

    it('allows: TZ=UTC rm -rf ./build', () => {
      const result = detectNuclearCommand('TZ=UTC rm -rf ./build');
      assert.equal(result.blocked, false);
    });

    it('allows: FOO=bar ls -la', () => {
      const result = detectNuclearCommand('FOO=bar ls -la');
      assert.equal(result.blocked, false);
    });
  });

  // -----------------------------------------------------------------------
  // Subshell evasion — $(...) hiding nuclear commands
  // -----------------------------------------------------------------------

  describe('subshell evasion — $(...)', () => {
    it('blocks: $(rm -rf /)', () => {
      const result = detectNuclearCommand('$(rm -rf /)');
      assert.equal(result.blocked, true);
      assert.ok(result.reason!.includes('subshell'));
    });

    it('blocks: echo $(rm -rf /)', () => {
      const result = detectNuclearCommand('echo $(rm -rf /)');
      assert.equal(result.blocked, true);
    });

    it('blocks: $(mkfs.ext4 /dev/sda1)', () => {
      const result = detectNuclearCommand('$(mkfs.ext4 /dev/sda1)');
      assert.equal(result.blocked, true);
    });

    it('allows: echo $(date)', () => {
      const result = detectNuclearCommand('echo $(date)');
      assert.equal(result.blocked, false);
    });

    it('allows: echo $(ls -la /tmp)', () => {
      const result = detectNuclearCommand('echo $(ls -la /tmp)');
      assert.equal(result.blocked, false);
    });
  });

  describe('IaC destroy commands', () => {
    it('blocks: terraform destroy', () => {
      assert.equal(detectNuclearCommand('terraform destroy').blocked, true);
    });

    it('blocks: terraform apply -destroy', () => {
      assert.equal(detectNuclearCommand('terraform apply -destroy').blocked, true);
    });

    it('blocks: terraform destroy -auto-approve', () => {
      assert.equal(detectNuclearCommand('terraform destroy -auto-approve').blocked, true);
    });

    it('blocks: tofu destroy', () => {
      assert.equal(detectNuclearCommand('tofu destroy').blocked, true);
    });

    it('blocks: pulumi destroy', () => {
      assert.equal(detectNuclearCommand('pulumi destroy').blocked, true);
    });

    it('blocks: pulumi destroy --yes', () => {
      assert.equal(detectNuclearCommand('pulumi destroy --yes').blocked, true);
    });

    it('blocks: cdk destroy', () => {
      assert.equal(detectNuclearCommand('cdk destroy').blocked, true);
    });

    it('blocks: cdk destroy MyStack', () => {
      assert.equal(detectNuclearCommand('cdk destroy MyStack').blocked, true);
    });

    it('blocks: cdktf destroy', () => {
      assert.equal(detectNuclearCommand('cdktf destroy').blocked, true);
    });

    it('blocks: sudo terraform destroy', () => {
      assert.equal(detectNuclearCommand('sudo terraform destroy').blocked, true);
    });

    it('blocks: terraform destroy inside bash -c', () => {
      assert.equal(detectNuclearCommand('bash -c "terraform destroy"').blocked, true);
    });

    it('blocks: terraform destroy in compound command', () => {
      assert.equal(detectNuclearCommand('cd infra && terraform destroy').blocked, true);
    });

    it('allows: terraform plan', () => {
      assert.equal(detectNuclearCommand('terraform plan').blocked, false);
    });

    it('allows: terraform apply', () => {
      assert.equal(detectNuclearCommand('terraform apply').blocked, false);
    });

    it('allows: terraform init', () => {
      assert.equal(detectNuclearCommand('terraform init').blocked, false);
    });

    it('allows: pulumi up', () => {
      assert.equal(detectNuclearCommand('pulumi up').blocked, false);
    });

    it('allows: pulumi preview', () => {
      assert.equal(detectNuclearCommand('pulumi preview').blocked, false);
    });

    it('allows: cdk deploy', () => {
      assert.equal(detectNuclearCommand('cdk deploy').blocked, false);
    });

    it('allows: cdk synth', () => {
      assert.equal(detectNuclearCommand('cdk synth').blocked, false);
    });

    it('allows: cdk diff', () => {
      assert.equal(detectNuclearCommand('cdk diff').blocked, false);
    });

    it('reason mentions IaC', () => {
      const result = detectNuclearCommand('terraform destroy');
      assert.ok(result.reason?.includes('IaC'));
    });
  });
});

// ---------------------------------------------------------------------------
// detectUnsafeFileCommand — blocks `file` with -m/--magic-file/-f/--files-from
// ---------------------------------------------------------------------------

describe('detectUnsafeFileCommand', () => {
  describe('blocks unsafe flags', () => {
    it('blocks: file -m /etc/magic.custom test.bin', () => {
      assert.ok(detectUnsafeFileCommand('file -m /etc/magic.custom test.bin'));
    });

    it('blocks: file --magic-file /etc/magic test.bin', () => {
      assert.ok(detectUnsafeFileCommand('file --magic-file /etc/magic test.bin'));
    });

    it('blocks: file -f /tmp/file-list', () => {
      assert.ok(detectUnsafeFileCommand('file -f /tmp/file-list'));
    });

    it('blocks: file --files-from /tmp/list.txt', () => {
      assert.ok(detectUnsafeFileCommand('file --files-from /tmp/list.txt'));
    });

    it('blocks: sudo file -m /etc/magic test.bin', () => {
      assert.ok(detectUnsafeFileCommand('sudo file -m /etc/magic test.bin'));
    });

    it('blocks: file -im /etc/magic test.bin (combined flags)', () => {
      assert.ok(detectUnsafeFileCommand('file -im /etc/magic test.bin'));
    });

    it('blocks: nice file -m /etc/magic test.bin', () => {
      assert.ok(detectUnsafeFileCommand('nice file -m /etc/magic test.bin'));
    });

    it('blocks: timeout 10 file --magic-file /etc/magic test.bin', () => {
      assert.ok(detectUnsafeFileCommand('timeout 10 file --magic-file /etc/magic test.bin'));
    });
  });

  describe('allows safe usage', () => {
    it('allows: file test.bin', () => {
      assert.ok(!detectUnsafeFileCommand('file test.bin'));
    });

    it('allows: file -i test.bin', () => {
      assert.ok(!detectUnsafeFileCommand('file -i test.bin'));
    });

    it('allows: file -b test.bin', () => {
      assert.ok(!detectUnsafeFileCommand('file -b test.bin'));
    });

    it('allows: ls -la /tmp/file', () => {
      assert.ok(!detectUnsafeFileCommand('ls -la /tmp/file'));
    });

    it('allows: echo file content', () => {
      assert.ok(!detectUnsafeFileCommand('echo file content'));
    });
  });
});

// ---------------------------------------------------------------------------
// detectShellStartupWrite — blocks writes to shell startup files
// ---------------------------------------------------------------------------

describe('detectShellStartupWrite', () => {
  describe('blocks writes to shell startup files', () => {
    it('blocks: echo "export PATH" >> ~/.bashrc', () => {
      assert.ok(detectShellStartupWrite('echo "export PATH" >> ~/.bashrc').blocked);
    });

    it('blocks: echo "alias x=y" >> ~/.zshrc', () => {
      assert.ok(detectShellStartupWrite('echo "alias x=y" >> ~/.zshrc').blocked);
    });

    it('blocks: tee -a ~/.bash_profile < script.sh', () => {
      assert.ok(detectShellStartupWrite('tee -a ~/.bash_profile < script.sh').blocked);
    });

    it('blocks: echo malicious >> ~/.zshenv', () => {
      assert.ok(detectShellStartupWrite('echo malicious >> ~/.zshenv').blocked);
    });

    it('blocks: sed -i "s/old/new/" ~/.profile', () => {
      assert.ok(detectShellStartupWrite('sed -i "s/old/new/" ~/.profile').blocked);
    });

    it('blocks: cp evil.sh ~/.bash_login', () => {
      assert.ok(detectShellStartupWrite('cp evil.sh ~/.bash_login').blocked);
    });

    it('blocks: echo payload >> $HOME/.bashrc', () => {
      assert.ok(detectShellStartupWrite('echo payload >> $HOME/.bashrc').blocked);
    });

    it('blocks: echo payload >> ${HOME}/.zshrc', () => {
      assert.ok(detectShellStartupWrite('echo payload >> ${HOME}/.zshrc').blocked);
    });

    it('blocks: mv evil.sh ~/.zlogin', () => {
      assert.ok(detectShellStartupWrite('mv evil.sh ~/.zlogin').blocked);
    });
  });

  describe('blocks writes to git config', () => {
    it('blocks: echo hook >> ~/.config/git/config', () => {
      assert.ok(detectShellStartupWrite('echo hook >> ~/.config/git/config').blocked);
    });

    it('blocks: cp hooks.sh $HOME/.config/git/hooks', () => {
      assert.ok(detectShellStartupWrite('cp hooks.sh $HOME/.config/git/hooks').blocked);
    });
  });

  describe('allows safe operations', () => {
    it('allows: cat ~/.bashrc', () => {
      assert.ok(!detectShellStartupWrite('cat ~/.bashrc').blocked);
    });

    it('allows: grep PATH ~/.zshrc', () => {
      assert.ok(!detectShellStartupWrite('grep PATH ~/.zshrc').blocked);
    });

    it('allows: source ~/.bashrc', () => {
      assert.ok(!detectShellStartupWrite('source ~/.bashrc').blocked);
    });

    it('allows: echo "hello world"', () => {
      assert.ok(!detectShellStartupWrite('echo "hello world"').blocked);
    });

    it('allows: echo data >> ~/project/notes.txt', () => {
      assert.ok(!detectShellStartupWrite('echo data >> ~/project/notes.txt').blocked);
    });

    it('provides reason mentioning the file', () => {
      const result = detectShellStartupWrite('echo payload >> ~/.bashrc');
      assert.ok(result.reason?.includes('.bashrc'));
    });
  });
});

// ---------------------------------------------------------------------------
// extractInputRedirectionPaths — extracts paths from < redirections
// ---------------------------------------------------------------------------

describe('extractInputRedirectionPaths', () => {
  it('extracts simple input redirect', () => {
    const paths = extractInputRedirectionPaths('sort < input.txt');
    assert.deepEqual(paths, ['input.txt']);
  });

  it('extracts multiple input redirects', () => {
    const paths = extractInputRedirectionPaths('cat < a.txt; sort < b.txt');
    assert.deepEqual(paths, ['a.txt', 'b.txt']);
  });

  it('extracts quoted paths', () => {
    const paths = extractInputRedirectionPaths('sort < "my file.txt"');
    assert.deepEqual(paths, ['my file.txt']);
  });

  it('extracts single-quoted paths', () => {
    const paths = extractInputRedirectionPaths("sort < 'my file.txt'");
    assert.deepEqual(paths, ['my file.txt']);
  });

  it('skips /dev/null', () => {
    const paths = extractInputRedirectionPaths('cmd < /dev/null');
    assert.deepEqual(paths, []);
  });

  it('skips /dev/fd/ paths', () => {
    const paths = extractInputRedirectionPaths('cmd < /dev/fd/3');
    assert.deepEqual(paths, []);
  });

  it('skips heredocs (<<)', () => {
    const paths = extractInputRedirectionPaths('cat << EOF\nhello\nEOF');
    assert.deepEqual(paths, []);
  });

  it('skips herestrings (<<<)', () => {
    const paths = extractInputRedirectionPaths('cat <<< "hello"');
    assert.deepEqual(paths, []);
  });

  it('handles fd prefix (0< file)', () => {
    const paths = extractInputRedirectionPaths('cmd 0< input.txt');
    assert.deepEqual(paths, ['input.txt']);
  });

  it('returns empty for no redirections', () => {
    const paths = extractInputRedirectionPaths('echo hello');
    assert.deepEqual(paths, []);
  });
});
