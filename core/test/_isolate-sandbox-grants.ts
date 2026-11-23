/**
 * Test hermeticity helper: the sandbox egress grant registry lazily hydrates a
 * durable "remembered hosts" store from `<config dir>/sandbox-hosts.json`. Without
 * isolation, a developer's real `~/.ami/sandbox-hosts.json` leaks into any test that
 * builds a sandbox wrapper (extra `--cap-add NET_ADMIN` / iptables rules, extra
 * allowlisted hosts). Call this once at the top level of a test file to point the
 * config dir at a private temp dir for the file's lifetime and reset the registry.
 */
import { before, after } from 'node:test';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { clearSandboxPermissionGrants } from '../src/tools/sandbox-permission-registry';

export function isolateSandboxGrants(): void {
  let dir: string;
  let prev: string | undefined;
  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-sbx-hermetic-'));
    prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    clearSandboxPermissionGrants();
  });
  after(() => {
    clearSandboxPermissionGrants();
    if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  });
}
