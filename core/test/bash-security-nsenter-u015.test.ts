import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectContainerEscape, validateBashSecurity } from '../src/tools/bash-security';

// detectContainerEscape previously only inspected docker/podman `run`. `nsenter`
// is the canonical container→host escape (`nsenter -t 1 -m -p bash` enters the
// host init namespaces) and is independent of any container runtime. It is now
// detected as a container-escape vector.
describe('detectContainerEscape — nsenter namespace entry (U015)', () => {
  it('blocks nsenter targeting a process (-t)', () => {
    assert.equal(detectContainerEscape('nsenter -t 1 -m -u -i -n -p bash').blocked, true);
  });

  it('blocks nsenter --target', () => {
    assert.equal(detectContainerEscape('nsenter --target 1 --mount --pid sh').blocked, true);
  });

  it('blocks nsenter with a long namespace flag', () => {
    assert.equal(detectContainerEscape('nsenter --mount=/proc/1/ns/mnt sh').blocked, true);
  });

  it('blocks nsenter --all', () => {
    assert.equal(detectContainerEscape('nsenter -a -t 1').blocked, true);
  });

  it('does not block a bare nsenter mention with no namespace/target flag', () => {
    assert.equal(detectContainerEscape('man nsenter').blocked, false);
    assert.equal(detectContainerEscape('echo nsenter is a tool').blocked, false);
  });

  it('still blocks docker --privileged (no regression)', () => {
    assert.equal(detectContainerEscape('docker run --privileged img').blocked, true);
  });

  it('hard-blocks nsenter escape through the full validator (checkId 46)', () => {
    const r = validateBashSecurity('nsenter -t 1 -m -p bash');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, 46); // CHECK_IDS.CONTAINER_ESCAPE
  });
});
