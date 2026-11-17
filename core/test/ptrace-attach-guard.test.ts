import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectHardlineCommand, PermissionManager } from '../src/permissions';

// Same-UID heap/credential scraping vectors. A prompt-injected command running
// as AMI's own UID can attach a debugger/tracer to the agent (or a peer) process
// and read provider API keys (ANTHROPIC_API_KEY / Bearer tokens) straight out of
// its heap, or read them via /proc/<pid>/mem. Claude Code closes this with
// prctl(PR_SET_DUMPABLE,0); AMI runs on Node (no libc FFI), so it blocks the
// attach vectors unconditionally at the permission boundary instead.
describe('Ptrace / heap-scraping attach guard (hardline)', () => {
  const blocked: Array<[string, string]> = [
    ['gdb -p 12345', 'gdb attach by -p pid'],
    ['gdb --pid 12345', 'gdb attach by --pid'],
    ['gdb --pid=12345', 'gdb attach by --pid='],
    ['gdb -q -p 999', 'gdb attach with leading flags'],
    ['gdb -p $PPID', 'gdb attach to parent (variable pid)'],
    ['lldb -p 12345', 'lldb attach by -p'],
    ['lldb --attach-pid 12345', 'lldb attach by --attach-pid'],
    ['lldb -n node', 'lldb attach by name'],
    ['strace -p 12345', 'strace attach by -p'],
    ['strace -f -p 12345', 'strace attach by -p with leading flag'],
    ['ltrace -p 12345', 'ltrace attach by -p'],
    ['cat /proc/self/mem', 'read own heap via /proc/self/mem'],
    ['dd if=/proc/1234/mem bs=1 2>/dev/null', 'read peer heap via /proc/<pid>/mem'],
    ['cat /proc/$PPID/mem', 'read parent heap via /proc/$PPID/mem'],
    ['cat /proc/${PPID}/mem', 'read parent heap via /proc/${PPID}/mem'],
    ['xxd /proc/self/mem', 'hexdump own heap'],
    ['bash -c "gdb -p 1"', 'gdb attach hidden inside bash -c payload'],
    ['python3 -c "import ctypes; ctypes.CDLL(None).process_vm_readv()"', 'process_vm_readv cross-process read'],
  ];

  for (const [cmd, desc] of blocked) {
    it(`blocks: ${cmd}`, () => {
      const result = detectHardlineCommand(cmd);
      assert.ok(result.blocked, `Expected "${cmd}" to be blocked (${desc})`);
      assert.ok(result.description, 'blocked command must carry a description');
    });
  }

  const allowed = [
    'cat /proc/meminfo',
    'cat /proc/cpuinfo',
    'cat /proc/self/status',
    'cat /proc/self/cmdline',
    'cat /proc/1/mounts',
    'gdb --version',
    'gdb ./a.out',
    'gdb --batch -x script.gdb ./a.out',
    'lldb ./a.out',
    'strace ls',
    'strace -f make',
    'ltrace ./prog',
    'grep -p pattern file',
  ];

  for (const cmd of allowed) {
    it(`allows: ${cmd}`, () => {
      const result = detectHardlineCommand(cmd);
      assert.ok(!result.blocked, `Expected "${cmd}" to be allowed`);
    });
  }

  // End-to-end: the guard must be enforced through the real PermissionManager
  // bash check, and — being a hardline — must override even the most permissive
  // modes (auto-allow / bypassPermissions), exactly like rm -rf /.
  describe('enforced end-to-end through PermissionManager.check (overrides every mode)', () => {
    for (const mode of ['ask', 'auto-allow', 'bypassPermissions'] as const) {
      it(`denies 'gdb -p $PPID' in ${mode} mode`, async () => {
        const pm = new PermissionManager(mode);
        assert.equal(await pm.check('bash', { command: 'gdb -p $PPID' }), 'deny');
      });
      it(`denies reading /proc/self/mem in ${mode} mode`, async () => {
        const pm = new PermissionManager(mode);
        assert.equal(await pm.check('bash', { command: 'cat /proc/self/mem' }), 'deny');
      });
    }

    it('does not deny a benign /proc read in auto-allow mode', async () => {
      const pm = new PermissionManager('auto-allow');
      assert.notEqual(await pm.check('bash', { command: 'cat /proc/meminfo' }), 'deny');
    });
  });
});
