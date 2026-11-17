import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getMaxListeners } from 'node:events';
import {
  createBoundedAbortController,
  raiseAbortListenerCap,
  DEFAULT_ABORT_MAX_LISTENERS,
} from '../src/utils/abort';
import { linkChildAbortToParent } from '../src/tools/task';

test('DEFAULT_ABORT_MAX_LISTENERS matches Claude Code (50)', () => {
  assert.equal(DEFAULT_ABORT_MAX_LISTENERS, 50);
});

test('createBoundedAbortController raises the signal listener cap', () => {
  const bare = new AbortController();
  // A bare signal uses Node's default EventTarget cap (10).
  assert.ok(getMaxListeners(bare.signal) <= 10);

  const bounded = createBoundedAbortController();
  assert.equal(getMaxListeners(bounded.signal), 50);

  const custom = createBoundedAbortController(7);
  assert.equal(getMaxListeners(custom.signal), 7);
});

test('createBoundedAbortController lets many abort listeners attach without a warning', () => {
  const warnings: Error[] = [];
  const onWarn = (w: Error): void => { warnings.push(w); };
  process.on('warning', onWarn);
  try {
    const c = createBoundedAbortController();
    for (let i = 0; i < 40; i++) {
      c.signal.addEventListener('abort', () => {}, { once: true });
    }
  } finally {
    process.removeListener('warning', onWarn);
  }
  assert.equal(
    warnings.filter(w => w.name === 'MaxListenersExceededWarning').length,
    0,
    'a bounded controller must not emit MaxListenersExceededWarning',
  );
});

test('raiseAbortListenerCap is best-effort (never throws on a plain signal)', () => {
  const c = new AbortController();
  assert.doesNotThrow(() => raiseAbortListenerCap(c.signal, 25));
  assert.equal(getMaxListeners(c.signal), 25);
  // default argument
  const d = new AbortController();
  raiseAbortListenerCap(d.signal);
  assert.equal(getMaxListeners(d.signal), 50);
});

test('linkChildAbortToParent raises the PARENT cap so many children can link to one turn signal', () => {
  // RED without the fix: the parent signal keeps Node's default cap (10) and a
  // wide sub-agent fan-out trips MaxListenersExceededWarning. GREEN: the cap is
  // raised to 50 before the first listener is attached.
  const parent = new AbortController();
  const warnings: Error[] = [];
  const onWarn = (w: Error): void => { warnings.push(w); };
  process.on('warning', onWarn);
  try {
    const unlinks: Array<() => void> = [];
    for (let i = 0; i < 30; i++) {
      unlinks.push(linkChildAbortToParent(parent.signal, new AbortController()));
    }
    assert.equal(getMaxListeners(parent.signal), 50);
    // teardown detaches every child listener
    for (const u of unlinks) u();
  } finally {
    process.removeListener('warning', onWarn);
  }
  assert.equal(
    warnings.filter(w => w.name === 'MaxListenersExceededWarning').length,
    0,
    'linking many children to one parent turn signal must not warn',
  );
});

test('linkChildAbortToParent still forwards an abort to the child (behavior preserved)', () => {
  const parent = new AbortController();
  const child = createBoundedAbortController();
  const unlink = linkChildAbortToParent(parent.signal, child);
  assert.equal(child.signal.aborted, false);
  parent.abort('turn interrupted');
  assert.equal(child.signal.aborted, true);
  unlink();
});

test('linkChildAbortToParent aborts an already-aborted parent immediately', () => {
  const parent = new AbortController();
  parent.abort('already gone');
  const child = createBoundedAbortController();
  linkChildAbortToParent(parent.signal, child);
  assert.equal(child.signal.aborted, true);
});
