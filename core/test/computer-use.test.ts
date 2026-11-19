import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  DEFAULT_COMPUTER_USE_CONFIG,
  readComputerUseConfig,
  getComputerUseCoordinateMode,
  resetComputerUseCoordinateMode,
  COMPUTER_USE_ENV,
  isComputerUseEnabled,
  SCREENSHOT_JPEG_QUALITY,
  MOVE_SETTLE_MS,
  API_RESIZE_PARAMS,
  targetImageSize,
  computeTargetDims,
  parseKeySequence,
  isBareEscape,
  planAnimatedMove,
  withoutTerminal,
  runBounded,
  requireComputerUseInput,
  setComputerUseInputProvider,
  resetComputerUseInput,
  createPbClipboard,
  typeViaClipboard,
  withModifiers,
  releasePressed,
  ensureOsPermissions,
  tryGetInstalledAppNames,
  createComputerUseExecutor,
  getComputerUseExecutor,
  resetComputerUseExecutor,
  dispatchComputerUseAction,
  computerUseTool,
  COMPUTER_USE_ACTIONS,
  type ComputerUseInputProvider,
  type ClipboardProvider,
  type ComputerUseExecutor,
} from '../src/tools/computer-use';
import type { ToolContext } from '../src/types';

// ── Fakes ────────────────────────────────────────────────────────────────────

interface Call {
  method: string;
  args: unknown[];
}

function makeFakeInput(overrides: Partial<ComputerUseInputProvider> = {}): {
  input: ComputerUseInputProvider;
  calls: Call[];
} {
  const calls: Call[] = [];
  const rec = (method: string) => (...args: unknown[]): Promise<void> => {
    calls.push({ method, args });
    return Promise.resolve();
  };
  const input: ComputerUseInputProvider = {
    isSupported: true,
    moveMouse: rec('moveMouse'),
    mouseLocation: async () => ({ x: 0, y: 0 }),
    mouseButton: rec('mouseButton'),
    mouseScroll: rec('mouseScroll'),
    key: rec('key'),
    keys: rec('keys'),
    typeText: rec('typeText'),
    getFrontmostAppInfo: () => ({ bundleId: 'com.test.app', appName: 'TestApp' }),
    ...overrides,
  };
  return { input, calls };
}

function makeFakeClipboard(initial = ''): { clipboard: ClipboardProvider; store: { value: string } } {
  const store = { value: initial };
  return {
    store,
    clipboard: {
      read: async () => store.value,
      write: async (t: string) => {
        store.value = t;
      },
    },
  };
}

function ctx(): ToolContext {
  return { cwd: '/tmp', abortSignal: new AbortController().signal } as ToolContext;
}

// ── Config knobs + coordinate mode ───────────────────────────────────────────

test('DEFAULT_COMPUTER_USE_CONFIG has the documented knobs + coordinate mode', () => {
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.enabled, false);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.coordinateMode, 'pixels');
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.pixelValidation, false);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.clipboardPasteMultiline, true);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.mouseAnimation, true);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.hideBeforeAction, true);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.autoTargetDisplay, true);
  assert.equal(DEFAULT_COMPUTER_USE_CONFIG.clipboardGuard, true);
});

test('readComputerUseConfig spreads a partial over defaults (partial inherits the rest)', () => {
  const cfg = readComputerUseConfig({ enabled: true });
  assert.equal(cfg.enabled, true);
  // Every other field inherited from defaults, none left undefined.
  assert.equal(cfg.coordinateMode, 'pixels');
  assert.equal(cfg.clipboardGuard, true);
  assert.equal(readComputerUseConfig().enabled, false);
  assert.equal(readComputerUseConfig({ coordinateMode: 'normalized' }).coordinateMode, 'normalized');
});

test('getComputerUseCoordinateMode freezes at first read', () => {
  resetComputerUseCoordinateMode();
  assert.equal(getComputerUseCoordinateMode({ coordinateMode: 'normalized' }), 'normalized');
  // A later flip does not change the frozen value.
  assert.equal(getComputerUseCoordinateMode({ coordinateMode: 'pixels' }), 'normalized');
  resetComputerUseCoordinateMode();
  assert.equal(getComputerUseCoordinateMode({ coordinateMode: 'pixels' }), 'pixels');
});

test('isComputerUseEnabled honors the opt-in env var', () => {
  assert.equal(isComputerUseEnabled({}), false);
  assert.equal(isComputerUseEnabled({ [COMPUTER_USE_ENV]: '1' }), true);
  assert.equal(isComputerUseEnabled({ [COMPUTER_USE_ENV]: 'TRUE' }), true);
  assert.equal(isComputerUseEnabled({ [COMPUTER_USE_ENV]: '0' }), false);
  assert.equal(isComputerUseEnabled({ [COMPUTER_USE_ENV]: 'nope' }), false);
});

// ── Screenshot quality + target dims ─────────────────────────────────────────

test('SCREENSHOT_JPEG_QUALITY default is 0.75', () => {
  assert.equal(SCREENSHOT_JPEG_QUALITY, 0.75);
});

test('targetImageSize bounds by long-edge and pixel budget, never upscales', () => {
  // Small image: unchanged (no upscale).
  assert.deepEqual(targetImageSize(100, 50, API_RESIZE_PARAMS), [100, 50]);
  // Long-edge bound: 4000 wide scales to maxLongEdge.
  const [w, h] = targetImageSize(4000, 1000, { maxLongEdge: 1000, maxPixels: 1e12 });
  assert.equal(w, 1000);
  assert.equal(h, 250);
  // Pixel-budget bound.
  const [pw, ph] = targetImageSize(2000, 2000, { maxLongEdge: 1e9, maxPixels: 1_000_000 });
  assert.ok(pw * ph <= 1_000_001);
  assert.equal(pw, ph);
  // Degenerate dims.
  assert.deepEqual(targetImageSize(0, 10), [0, 0]);
});

test('computeTargetDims applies the device scale factor before bounding', () => {
  const [w, h] = computeTargetDims(800, 600, 2);
  // 1600x1200 physical → within default bounds (long edge 1600 > 1568).
  assert.ok(w <= API_RESIZE_PARAMS.maxLongEdge);
  assert.equal(Math.round((w / h) * 100), Math.round((800 / 600) * 100));
});

// ── Keyboard parsing ─────────────────────────────────────────────────────────

test('parseKeySequence splits on + and drops empties', () => {
  assert.deepEqual(parseKeySequence('ctrl+shift+a'), ['ctrl', 'shift', 'a']);
  assert.deepEqual(parseKeySequence('a++b'), ['a', 'b']);
  assert.deepEqual(parseKeySequence(''), []);
});

test('isBareEscape matches single escape/esc case-insensitively only', () => {
  assert.equal(isBareEscape(['escape']), true);
  assert.equal(isBareEscape(['ESC']), true);
  assert.equal(isBareEscape(['Escape']), true);
  assert.equal(isBareEscape(['ctrl', 'escape']), false);
  assert.equal(isBareEscape(['a']), false);
  assert.equal(isBareEscape([]), false);
});

// ── Animation planning ───────────────────────────────────────────────────────

test('planAnimatedMove: disabled → single instant move with settle', () => {
  const frames = planAnimatedMove({ x: 0, y: 0 }, { x: 500, y: 500 }, false);
  assert.deepEqual(frames, [{ x: 500, y: 500, sleepMs: MOVE_SETTLE_MS }]);
});

test('planAnimatedMove: sub-pixel distance → no-op', () => {
  assert.deepEqual(planAnimatedMove({ x: 10, y: 10 }, { x: 10, y: 10 }, true), []);
});

test('planAnimatedMove: tiny distance under min duration → single move', () => {
  // distance 10px @ 2000px/s = 0.005s < 0.03s threshold.
  const frames = planAnimatedMove({ x: 0, y: 0 }, { x: 10, y: 0 }, true);
  assert.deepEqual(frames, [{ x: 10, y: 0, sleepMs: MOVE_SETTLE_MS }]);
});

test('planAnimatedMove: long distance → eased frames, last frame gets settle', () => {
  const frames = planAnimatedMove({ x: 0, y: 0 }, { x: 1000, y: 0 }, true);
  assert.ok(frames.length > 1);
  // First frame moved less than linear would (ease-out starts fast then slows? cubic ease-out is fast-start)
  assert.equal(frames[frames.length - 1]!.x, 1000);
  assert.equal(frames[frames.length - 1]!.sleepMs, MOVE_SETTLE_MS);
  // Intermediate frames use the frame interval, not the settle.
  assert.notEqual(frames[0]!.sleepMs, MOVE_SETTLE_MS);
  // Monotonic non-decreasing x toward target.
  for (let i = 1; i < frames.length; i++) {
    assert.ok(frames[i]!.x >= frames[i - 1]!.x);
  }
});

// ── withoutTerminal ──────────────────────────────────────────────────────────

test('withoutTerminal strips the terminal bundle id from the allow-list', () => {
  assert.deepEqual(withoutTerminal(['a', 'term', 'b'], 'term'), ['a', 'b']);
  assert.deepEqual(withoutTerminal(['a', 'b'], null), ['a', 'b']);
});

// ── runBounded ───────────────────────────────────────────────────────────────

test('runBounded resolves the inner result', async () => {
  assert.equal(await runBounded(async () => 42), 42);
});

test('runBounded rejects when the signal is already aborted', async () => {
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(() => runBounded(async () => 1, { signal: ac.signal }), /aborted before it started/);
});

test('runBounded times out a wedged call', async () => {
  await assert.rejects(
    () => runBounded(() => new Promise<void>(() => {}), { timeoutMs: 10 }),
    /exceeded 10ms/,
  );
});

// ── Native input loader ──────────────────────────────────────────────────────

test('requireComputerUseInput throws a clear platform-unavailability message when none installed', () => {
  resetComputerUseInput();
  assert.throws(
    () => requireComputerUseInput(),
    new RegExp(`unavailable on ${process.platform}`),
  );
});

test('requireComputerUseInput narrows on isSupported and caches the resolved backend', () => {
  resetComputerUseInput();
  const { input } = makeFakeInput();
  let built = 0;
  setComputerUseInputProvider(() => {
    built++;
    return input;
  });
  assert.equal(requireComputerUseInput(), input);
  assert.equal(requireComputerUseInput(), input);
  assert.equal(built, 1, 'cached — factory invoked once');
  resetComputerUseInput();
});

test('requireComputerUseInput throws when the backend reports unsupported', () => {
  resetComputerUseInput();
  setComputerUseInputProvider(() => makeFakeInput({ isSupported: false }).input);
  assert.throws(() => requireComputerUseInput(), /not supported on/);
  resetComputerUseInput();
});

// ── pbcopy/pbpaste clipboard ─────────────────────────────────────────────────

test('createPbClipboard reads/writes via the exec seam', async () => {
  const seen: Array<{ file: string; input?: string }> = [];
  const clip = createPbClipboard(async (file, _args, options) => {
    seen.push({ file, input: options?.input });
    return { stdout: file === 'pbpaste' ? 'hello' : '', stderr: '', code: 0 };
  });
  assert.equal(await clip.read(), 'hello');
  await clip.write('world');
  assert.deepEqual(seen.map((s) => s.file), ['pbpaste', 'pbcopy']);
  assert.equal(seen[1]!.input, 'world');
});

test('createPbClipboard throws on a non-zero exit', async () => {
  const clip = createPbClipboard(async () => ({ stdout: '', stderr: '', code: 1 }));
  await assert.rejects(() => clip.read(), /pbpaste exited with code 1/);
  await assert.rejects(() => clip.write('x'), /pbcopy exited with code 1/);
});

// ── typeViaClipboard ─────────────────────────────────────────────────────────

test('typeViaClipboard saves, writes, read-back verifies, pastes, and restores', async () => {
  const { input, calls } = makeFakeInput();
  const { clipboard, store } = makeFakeClipboard('USER-ORIGINAL');
  await typeViaClipboard(input, clipboard, 'to paste');
  // Cmd+V issued.
  assert.ok(calls.some((c) => c.method === 'keys' && Array.isArray(c.args[0]) && (c.args[0] as string[]).join('+') === 'command+v'));
  // User's clipboard restored in the finally.
  assert.equal(store.value, 'USER-ORIGINAL');
});

test('typeViaClipboard refuses to paste if the write does not round-trip, and still restores', async () => {
  const { input, calls } = makeFakeInput();
  const store = { value: 'ORIG' };
  const clipboard: ClipboardProvider = {
    read: async () => store.value,
    // Write that silently fails (does not persist the new text).
    write: async (t: string) => {
      if (t === 'target') return; // drop it
      store.value = t;
    },
  };
  await assert.rejects(() => typeViaClipboard(input, clipboard, 'target'), /did not round-trip/);
  assert.ok(!calls.some((c) => c.method === 'keys'), 'Cmd+V never pressed on mismatch');
  assert.equal(store.value, 'ORIG', 'original restored');
});

test('typeViaClipboard proceeds without restore when the initial save read fails', async () => {
  const { input } = makeFakeInput();
  const store = { value: '' };
  let reads = 0;
  const clipboard: ClipboardProvider = {
    read: async () => {
      reads++;
      if (reads === 1) throw new Error('pbpaste failed');
      return store.value;
    },
    write: async (t: string) => {
      store.value = t;
    },
  };
  await typeViaClipboard(input, clipboard, 'x');
  // No restore attempted (saved is undefined) — clipboard keeps the pasted text.
  assert.equal(store.value, 'x');
});

// ── withModifiers / releasePressed ───────────────────────────────────────────

test('releasePressed drains in reverse (last pressed first), swallowing errors', async () => {
  const released: string[] = [];
  const input = makeFakeInput({
    key: async (name: string, phase: string) => {
      if (phase === 'release') {
        if (name === 'shift') throw new Error('release boom');
        released.push(name);
      }
    },
  }).input;
  const pressed = ['command', 'shift', 'a'];
  await releasePressed(input, pressed);
  // Reverse order, 'shift' threw but was swallowed, drain continued.
  assert.deepEqual(released, ['a', 'command']);
  assert.equal(pressed.length, 0, 'drained via pop()');
});

test('withModifiers only releases presses that landed when a mid-press throws', async () => {
  const released: string[] = [];
  const input = makeFakeInput({
    key: async (name: string, phase: string) => {
      if (phase === 'press' && name === 'shift') throw new Error('press boom');
      if (phase === 'release') released.push(name);
    },
  }).input;
  await assert.rejects(
    () => withModifiers(input, ['command', 'shift', 'option'], async () => 'ran'),
    /press boom/,
  );
  // Only 'command' landed before the throw; only it is released.
  assert.deepEqual(released, ['command']);
});

// ── OS permission preflight ──────────────────────────────────────────────────

test('ensureOsPermissions returns granted when both grants present, else per-flag state', () => {
  assert.deepEqual(
    ensureOsPermissions({ checkAccessibility: () => true, checkScreenRecording: () => true }),
    { granted: true },
  );
  assert.deepEqual(
    ensureOsPermissions({ checkAccessibility: () => true, checkScreenRecording: () => false }),
    { granted: false, accessibility: true, screenRecording: false },
  );
});

// ── Fail-soft timed app enumeration ──────────────────────────────────────────

test('tryGetInstalledAppNames returns the list when it resolves in time', async () => {
  assert.deepEqual(await tryGetInstalledAppNames(async () => ['App A', 'App B'], 1000), ['App A', 'App B']);
});

test('tryGetInstalledAppNames fails soft to undefined on timeout and swallows the late rejection', async () => {
  let settled = false;
  const result = await tryGetInstalledAppNames(
    () =>
      new Promise<string[]>((_, reject) => {
        setTimeout(() => {
          settled = true;
          reject(new Error('late failure'));
        }, 30);
      }),
    5,
  );
  assert.equal(result, undefined);
  // Give the background promise time to reject; must not crash the process.
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(settled, true);
});

test('tryGetInstalledAppNames fails soft to undefined when enumeration throws immediately', async () => {
  assert.equal(await tryGetInstalledAppNames(async () => { throw new Error('boom'); }, 1000), undefined);
});

// ── Executor dispatch (integration over a fake backend) ──────────────────────

function makeExecutor(
  inputOverrides: Partial<ComputerUseInputProvider> = {},
  opts: { clipboard?: ClipboardProvider; animation?: boolean; onEsc?: () => void } = {},
): { executor: ComputerUseExecutor; input: ComputerUseInputProvider; calls: Call[] } {
  const { input, calls } = makeFakeInput(inputOverrides);
  const executor = createComputerUseExecutor({
    input,
    clipboard: opts.clipboard,
    getMouseAnimationEnabled: () => opts.animation ?? false,
    terminalBundleId: 'com.term',
    onExpectedEscape: opts.onEsc,
  });
  return { executor, input, calls };
}

test('executor.moveMouse moves then settles', async () => {
  const { executor, calls } = makeExecutor();
  await executor.moveMouse(10, 20);
  assert.deepEqual(calls[0], { method: 'moveMouse', args: [10, 20, false] });
});

test('executor.click without modifiers clicks directly; with modifiers brackets them', async () => {
  const plain = makeExecutor();
  await plain.executor.click(1, 2, 'left', 1);
  assert.ok(plain.calls.some((c) => c.method === 'mouseButton' && c.args[0] === 'left' && c.args[1] === 'click'));

  const modded = makeExecutor();
  await modded.executor.click(1, 2, 'right', 2, ['command']);
  const order = modded.calls.map((c) => `${c.method}:${c.args[0]}:${c.args[1] ?? ''}`);
  // press command → click → release command.
  assert.ok(order.includes('key:command:press'));
  assert.ok(order.includes('mouseButton:right:click'));
  assert.ok(order.includes('key:command:release'));
  assert.ok(order.lastIndexOf('key:command:release') > order.indexOf('mouseButton:right:click'));
});

test('executor.drag presses, moves, and ALWAYS releases the button even if the move throws', async () => {
  const released: string[] = [];
  const { executor } = makeExecutor({
    mouseLocation: async () => ({ x: 0, y: 0 }),
    moveMouse: async () => { throw new Error('move boom'); },
    mouseButton: async (button: string, phase: string) => {
      if (phase === 'release') released.push(button);
    },
  }, { animation: false });
  await assert.rejects(() => executor.drag(undefined, { x: 100, y: 100 }), /move boom/);
  assert.deepEqual(released, ['left'], 'left button released in finally');
});

test('executor.scroll moves then scrolls vertical-first', async () => {
  const { executor, calls } = makeExecutor();
  await executor.scroll(5, 5, 3, -4);
  const scrolls = calls.filter((c) => c.method === 'mouseScroll');
  assert.deepEqual(scrolls[0]!.args, [-4, 'vertical']);
  assert.deepEqual(scrolls[1]!.args, [3, 'horizontal']);
});

test('executor.scroll skips a zero axis', async () => {
  const { executor, calls } = makeExecutor();
  await executor.scroll(5, 5, 0, -4);
  const scrolls = calls.filter((c) => c.method === 'mouseScroll');
  assert.equal(scrolls.length, 1);
  assert.deepEqual(scrolls[0]!.args, [-4, 'vertical']);
});

test('executor.key repeats the sequence and notifies on a bare escape', async () => {
  let escapes = 0;
  const { executor, calls } = makeExecutor({}, { onEsc: () => escapes++ });
  await executor.key('ctrl+a', 2);
  const keys = calls.filter((c) => c.method === 'keys');
  assert.equal(keys.length, 2);
  assert.deepEqual(keys[0]!.args[0], ['ctrl', 'a']);
  assert.equal(escapes, 0, 'ctrl+a is not a bare escape');

  const esc = makeExecutor({}, { onEsc: () => escapes++ });
  await esc.executor.key('escape');
  assert.equal(escapes, 1);
});

test('executor.holdKey presses, holds, then releases everything pressed', async () => {
  const { executor, calls } = makeExecutor();
  await executor.holdKey(['shift', 'a'], 1);
  const presses = calls.filter((c) => c.method === 'key' && c.args[1] === 'press').map((c) => c.args[0]);
  const releases = calls.filter((c) => c.method === 'key' && c.args[1] === 'release').map((c) => c.args[0]);
  assert.deepEqual(presses, ['shift', 'a']);
  assert.deepEqual(releases.sort(), ['a', 'shift']);
});

test('executor.type uses typeText directly, or the clipboard path when viaClipboard', async () => {
  const direct = makeExecutor();
  await direct.executor.type('hi', { viaClipboard: false });
  assert.ok(direct.calls.some((c) => c.method === 'typeText' && c.args[0] === 'hi'));

  const { clipboard, store } = makeFakeClipboard('orig');
  const viaClip = makeExecutor({}, { clipboard });
  await viaClip.executor.type('pasted', { viaClipboard: true });
  assert.ok(viaClip.calls.some((c) => c.method === 'keys'));
  assert.equal(store.value, 'orig', 'clipboard restored');
});

test('executor clipboard + cursor + frontmost-app queries', async () => {
  const { clipboard } = makeFakeClipboard('clip');
  const { executor } = makeExecutor({ mouseLocation: async () => ({ x: 7, y: 9 }) }, { clipboard });
  assert.equal(await executor.readClipboard(), 'clip');
  await executor.writeClipboard('new');
  assert.equal(await executor.readClipboard(), 'new');
  assert.deepEqual(await executor.getCursorPosition(), { x: 7, y: 9 });
  assert.deepEqual(await executor.getFrontmostApp(), { bundleId: 'com.test.app', displayName: 'TestApp' });
});

test('executor.getFrontmostApp returns null when no bundle id', async () => {
  const { executor } = makeExecutor({ getFrontmostAppInfo: () => null });
  assert.equal(await executor.getFrontmostApp(), null);
  const { executor: e2 } = makeExecutor({ getFrontmostAppInfo: () => ({ appName: 'X' }) });
  assert.equal(await e2.getFrontmostApp(), null);
});

test('executor app enumeration + open delegate to the backend, or report unsupported', async () => {
  const withApps = makeExecutor({
    listInstalledApps: async () => [{ bundleId: 'com.a', displayName: 'A' }],
    openApp: async () => {},
  });
  assert.deepEqual(await withApps.executor.listInstalledApps(), [{ bundleId: 'com.a', displayName: 'A' }]);
  await withApps.executor.openApp('com.a');
  assert.ok(withApps.calls.length >= 0);

  const noApps = makeExecutor();
  assert.deepEqual(await noApps.executor.listInstalledApps(), []);
  await assert.rejects(() => noApps.executor.openApp('com.a'), /not supported/);
});

test('executor.computeCaptureDims scales a display geometry', () => {
  const { executor } = makeExecutor();
  const [w, h] = executor.computeCaptureDims({ width: 800, height: 600, scaleFactor: 2 });
  assert.ok(w > 0 && h > 0);
  assert.ok(w <= API_RESIZE_PARAMS.maxLongEdge);
  assert.equal(executor.terminalBundleId, 'com.term');
});

test('executor lazily surfaces the platform-unavailability message when no backend', async () => {
  resetComputerUseInput();
  const executor = createComputerUseExecutor({ getMouseAnimationEnabled: () => false });
  await assert.rejects(() => executor.getCursorPosition(), new RegExp(`unavailable on ${process.platform}`));
  resetComputerUseInput();
});

// ── dispatchComputerUseAction ────────────────────────────────────────────────

function stubExecutor(): { executor: ComputerUseExecutor; log: string[] } {
  const log: string[] = [];
  const executor = {
    terminalBundleId: null,
    computeCaptureDims: () => [0, 0] as [number, number],
    moveMouse: async (x, y) => void log.push(`move ${x},${y}`),
    click: async (x, y, b, c, m) => void log.push(`click ${b} ${c} ${x},${y} ${(m ?? []).join('+')}`),
    drag: async (f, t) => void log.push(`drag ${f ? `${f.x},${f.y}` : 'cur'}->${t.x},${t.y}`),
    scroll: async (x, y, dx, dy) => void log.push(`scroll ${dx},${dy}@${x},${y}`),
    key: async (seq, r) => void log.push(`key ${seq} x${r ?? 1}`),
    holdKey: async (k, d) => void log.push(`hold ${k.join('+')} ${d}`),
    type: async (t, o) => void log.push(`type ${t} clip=${o.viaClipboard}`),
    readClipboard: async () => 'CLIP',
    writeClipboard: async (t) => void log.push(`write ${t}`),
    getCursorPosition: async () => ({ x: 3, y: 4 }),
    getFrontmostApp: async () => ({ bundleId: 'com.f', displayName: 'Front' }),
    listInstalledApps: async () => [{ bundleId: 'com.a', displayName: 'A' }],
    openApp: async (b) => void log.push(`open ${b}`),
  } as ComputerUseExecutor;
  return { executor, log };
}

test('dispatchComputerUseAction routes each action and formats output', async () => {
  const { executor, log } = stubExecutor();
  assert.match(await dispatchComputerUseAction(executor, 'cursor_position', {}), /\(3, 4\)/);
  await dispatchComputerUseAction(executor, 'mouse_move', { coordinate: [1, 2] });
  await dispatchComputerUseAction(executor, 'left_click', { coordinate: [1, 2], modifiers: ['command', 5] });
  await dispatchComputerUseAction(executor, 'right_click', { coordinate: [1, 2] });
  await dispatchComputerUseAction(executor, 'middle_click', { coordinate: [1, 2] });
  await dispatchComputerUseAction(executor, 'double_click', { coordinate: [1, 2] });
  await dispatchComputerUseAction(executor, 'triple_click', { coordinate: [1, 2] });
  await dispatchComputerUseAction(executor, 'left_click_drag', { coordinate: [9, 9], start_coordinate: [1, 1] });
  await dispatchComputerUseAction(executor, 'scroll', { coordinate: [5, 5], scroll_x: 2, scroll_y: -3 });
  await dispatchComputerUseAction(executor, 'key', { text: 'ctrl+a', repeat: 3 });
  await dispatchComputerUseAction(executor, 'hold_key', { keys: ['shift'], duration: 50 });
  await dispatchComputerUseAction(executor, 'type', { text: 'hello', via_clipboard: true });
  assert.equal(await dispatchComputerUseAction(executor, 'read_clipboard', {}), 'CLIP');
  await dispatchComputerUseAction(executor, 'write_clipboard', { text: 'w' });
  assert.match(await dispatchComputerUseAction(executor, 'get_frontmost_app', {}), /Front \(com\.f\)/);
  assert.match(await dispatchComputerUseAction(executor, 'list_apps', {}), /A \(com\.a\)/);
  await dispatchComputerUseAction(executor, 'open_app', { bundleId: 'com.a' });

  assert.ok(log.includes('move 1,2'));
  assert.ok(log.includes('click left 1 1,2 command'), 'non-string modifier filtered out');
  assert.ok(log.includes('click left 2 1,2 '));
  assert.ok(log.includes('drag 1,1->9,9'));
  assert.ok(log.includes('scroll 2,-3@5,5'));
  assert.ok(log.includes('key ctrl+a x3'));
  assert.ok(log.includes('hold shift 50'));
  assert.ok(log.includes('type hello clip=true'));
  assert.ok(log.includes('open com.a'));
});

test('dispatchComputerUseAction uses the current cursor for a drag with no start', async () => {
  const { executor, log } = stubExecutor();
  await dispatchComputerUseAction(executor, 'left_click_drag', { coordinate: [9, 9] });
  assert.ok(log.includes('drag cur->9,9'));
});

test('dispatchComputerUseAction validates malformed action params', async () => {
  const { executor } = stubExecutor();
  await assert.rejects(() => dispatchComputerUseAction(executor, 'mouse_move', {}), /requires a \[x, y\]/);
  await assert.rejects(() => dispatchComputerUseAction(executor, 'key', { text: '' }), /non-empty 'text'/);
  await assert.rejects(() => dispatchComputerUseAction(executor, 'hold_key', { keys: [] }), /non-empty 'keys'/);
  await assert.rejects(() => dispatchComputerUseAction(executor, 'type', {}), /requires a 'text'/);
  await assert.rejects(() => dispatchComputerUseAction(executor, 'write_clipboard', {}), /requires a 'text'/);
  await assert.rejects(() => dispatchComputerUseAction(executor, 'open_app', {}), /requires a 'bundleId'/);
});

test('list_apps / frontmost report empty states', async () => {
  const empty = {
    ...stubExecutor().executor,
    listInstalledApps: async () => [],
    getFrontmostApp: async () => null,
  } as ComputerUseExecutor;
  assert.match(await dispatchComputerUseAction(empty, 'list_apps', {}), /no installed apps/);
  assert.match(await dispatchComputerUseAction(empty, 'get_frontmost_app', {}), /no frontmost app/);
});

// ── Tool definition (end-to-end through execute) ─────────────────────────────

test('computerUseTool is OFF by default and enabled by the opt-in env', () => {
  const prev = process.env[COMPUTER_USE_ENV];
  delete process.env[COMPUTER_USE_ENV];
  assert.equal(computerUseTool.isEnabled!(), false);
  process.env[COMPUTER_USE_ENV] = '1';
  assert.equal(computerUseTool.isEnabled!(), true);
  if (prev === undefined) delete process.env[COMPUTER_USE_ENV];
  else process.env[COMPUTER_USE_ENV] = prev;
});

test('computerUseTool schema advertises every action and is additionalProperties:false', () => {
  assert.equal(computerUseTool.name, 'computer_use');
  assert.equal(computerUseTool.inputSchema.additionalProperties, false);
  assert.deepEqual(computerUseTool.inputSchema.properties.action.enum, [...COMPUTER_USE_ACTIONS]);
  assert.deepEqual(computerUseTool.inputSchema.required, ['action']);
});

test('computerUseTool isReadOnlyForInput: queries are read-only, actuators are not', () => {
  assert.equal(computerUseTool.isReadOnlyForInput!({ action: 'cursor_position' }), true);
  assert.equal(computerUseTool.isReadOnlyForInput!({ action: 'read_clipboard' }), true);
  assert.equal(computerUseTool.isReadOnlyForInput!({ action: 'left_click' }), false);
});

test('computerUseTool.execute rejects an unknown action with the valid list', async () => {
  const res = await computerUseTool.execute({ action: 'teleport' }, ctx());
  assert.equal(res.isError, true);
  assert.match(res.output, /Unknown computer-use action 'teleport'/);
  assert.match(res.output, /cursor_position/);
});

test('computerUseTool.execute surfaces the platform-unavailability message with no backend', async () => {
  resetComputerUseInput();
  resetComputerUseExecutor();
  const res = await computerUseTool.execute({ action: 'cursor_position' }, ctx());
  assert.equal(res.isError, true);
  assert.match(res.output, new RegExp(`unavailable on ${process.platform}`));
  resetComputerUseInput();
});

test('computerUseTool.execute drives a real action through an injected backend (e2e)', async () => {
  resetComputerUseInput();
  resetComputerUseExecutor();
  const { input, calls } = makeFakeInput({ mouseLocation: async () => ({ x: 11, y: 22 }) });
  setComputerUseInputProvider(() => input);
  const res = await computerUseTool.execute({ action: 'cursor_position' }, ctx());
  assert.equal(res.isError, undefined);
  assert.match(res.output, /\(11, 22\)/);

  const moved = await computerUseTool.execute({ action: 'mouse_move', coordinate: [30, 40] }, ctx());
  assert.equal(moved.isError, undefined);
  assert.ok(calls.some((c) => c.method === 'moveMouse' && c.args[0] === 30 && c.args[1] === 40));
  resetComputerUseInput();
  resetComputerUseExecutor();
});

test('computerUseTool.execute reports a backend action failure as isError', async () => {
  resetComputerUseInput();
  resetComputerUseExecutor();
  setComputerUseInputProvider(() => makeFakeInput({ moveMouse: async () => { throw new Error('HID fault'); } }).input);
  const res = await computerUseTool.execute({ action: 'mouse_move', coordinate: [1, 1] }, ctx());
  assert.equal(res.isError, true);
  assert.match(res.output, /mouse_move failed: HID fault/);
  resetComputerUseInput();
  resetComputerUseExecutor();
});

test('getComputerUseExecutor caches without a signal and rebuilds with one', () => {
  resetComputerUseInput();
  resetComputerUseExecutor();
  setComputerUseInputProvider(() => makeFakeInput().input);
  const a = getComputerUseExecutor();
  const b = getComputerUseExecutor();
  assert.equal(a, b, 'cached singleton');
  const withSignal = getComputerUseExecutor({ signal: new AbortController().signal });
  assert.notEqual(withSignal, a, 'per-call signal rebuilds');
  resetComputerUseInput();
  resetComputerUseExecutor();
});
