/**
 * Computer-use GUI-automation tool (screenshot / mouse / keyboard / clipboard /
 * app-control) and its platform-independent algorithmic core.
 *
 * PARITY (additive) with the reference's macOS computer-use subsystem
 * (`src/utils/computerUse/{executor,gates,hostAdapter,inputLoader,mcpServer}.ts`).
 * the reference's implementation binds two Anthropic-proprietary native modules
 * (`@ant/computer-use-input` — Rust/enigo mouse+keyboard; `@ant/computer-use-swift`
 * — SCContentFilter screenshots / NSWorkspace / TCC) that do NOT ship with AMI.
 *
 * This module ports every PLATFORM-INDEPENDENT behavior of that subsystem — the
 * config knobs, coordinate-mode, timing/motion tunables, screenshot target-dim
 * computation, key-sequence parsing, abort/timeout-bounded run loop, best-effort
 * cleanup, read-back-verified clipboard paste, OS-permission preflight shape, and
 * the fail-soft timed installed-app enumeration — behind an INJECTABLE native
 * backend ({@link ComputerUseInputProvider} / {@link ClipboardProvider}). The real
 * I/O is supplied by a host that registers a backend via
 * {@link setComputerUseInputProvider}; absent one (every non-macOS host, and macOS
 * hosts without the native backend installed) the loader fails fast with a clear,
 * actionable platform-unavailability message — exactly mirroring the reference's
 * `requireComputerUseInput()` / `createCliExecutor` non-darwin guards.
 *
 * The tool is OFF by default (opt-in via {@link COMPUTER_USE_ENV}); when disabled
 * it is dropped from the advertised pool at registration (parity: the reference's
 * `adapter.isDisabled() ? { tools: [] }` ListTools gate), so the default tool set
 * is unchanged.
 *
 * Vendor gating that the reference layers on top (growthbook flag, subscription tier,
 * ant-bypass env vars) is intentionally OUT of scope — AMI gates purely on the
 * opt-in env flag and the platform/backend availability.
 */

import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { sleep } from '../utils/sleep';
import '../utils/shell';
import { errorMessage } from '../credential-guard';
import { COMPUTER_USE_ENV, KEY_REPEAT_CADENCE_MS, MOVE_SETTLE_MS, computeTargetDims, createPbClipboard, isBareEscape, isComputerUseEnabled, parseKeySequence, planAnimatedMove, readComputerUseConfig, releasePressed, runBounded, typeViaClipboard, withModifiers } from './computer-use-plan-animated-move';
import type { ComputerUseConfig, ComputerUseInputProvider, CoordinateMode } from './computer-use-plan-animated-move';
import { COMPUTER_USE_ACTIONS, COMPUTER_USE_TOOL_NAME } from './computer-use-computer-use-executor';
import type { ComputerUseExecutor, ComputerUseExecutorOptions, DisplayGeometry } from './computer-use-computer-use-executor';
export { ANIMATION_FRAME_RATE, ANIMATION_MAX_DURATION_SEC, ANIMATION_MIN_DURATION_SEC, ANIMATION_PX_PER_SEC, API_RESIZE_PARAMS, APP_ENUM_TIMEOUT_MS, CLIPBOARD_PASTE_SETTLE_MS, COMPUTER_USE_ENV, COORDINATE_MODES, DEFAULT_COMPUTER_USE_CONFIG, KEY_REPEAT_CADENCE_MS, MOVE_SETTLE_MS, RUN_LOOP_TIMEOUT_MS, SCREENSHOT_JPEG_QUALITY, computeTargetDims, createPbClipboard, isBareEscape, isComputerUseEnabled, parseKeySequence, planAnimatedMove, readComputerUseConfig, releasePressed, runBounded, targetImageSize, typeViaClipboard, withModifiers, withoutTerminal } from './computer-use-plan-animated-move';
export type { AnimationFrame, ClipboardProvider, ComputerUseConfig, ComputerUseInputProvider, ComputerUseSubGates, CoordinateMode, OsPermissionState, ResizeParams } from './computer-use-plan-animated-move';
export { COMPUTER_USE_ACTIONS, COMPUTER_USE_TOOL_NAME, ensureOsPermissions, toUndefined, tryGetInstalledAppNames } from './computer-use-computer-use-executor';
export type { ComputerUseExecutor, ComputerUseExecutorOptions, DisplayGeometry, OsPermissionProbe, ScreenshotResult } from './computer-use-computer-use-executor';

/**
 * Frozen-at-first-read coordinate mode. Parity: gates.ts `getChicagoCoordinateMode`
 * — setup builds tool descriptions and the executor scales coordinates off the SAME
 * value, so it must not change mid-session (a live flip would tell the model
 * "pixels" while transforming clicks as normalized).
 */
let frozenCoordinateMode: CoordinateMode | undefined;
export function getComputerUseCoordinateMode(partial?: Partial<ComputerUseConfig>): CoordinateMode {
  frozenCoordinateMode ??= readComputerUseConfig(partial).coordinateMode;
  return frozenCoordinateMode;
}
/** Test seam: clear the frozen coordinate mode. */
export function resetComputerUseCoordinateMode(): void {
  frozenCoordinateMode = undefined;
}

let cachedInput: ComputerUseInputProvider | undefined;
let injectedInputFactory: (() => ComputerUseInputProvider) | undefined;

/**
 * Register the native input backend (a host that bundles one, or a test). Resets
 * the cache so the next {@link requireComputerUseInput} resolves the new factory.
 */
export function setComputerUseInputProvider(factory: () => ComputerUseInputProvider): void {
  injectedInputFactory = factory;
  cachedInput = undefined;
}
/** Test seam: clear the registered backend and its cache. */
export function resetComputerUseInput(): void {
  injectedInputFactory = undefined;
  cachedInput = undefined;
}

/**
 * Lazy-cached, platform-narrowed native-input loader. Parity: inputLoader.ts
 * `requireComputerUseInput` — caches the resolved backend in a singleton and
 * narrows the discriminated union on `isSupported` so callers get a bare provider.
 * AMI ships NO native backend, so absent an injected one this fails fast with a
 * clear, actionable message that names the current platform — the parity-faithful
 * analog of the reference's non-darwin/`!isSupported` throws.
 */
export function requireComputerUseInput(): ComputerUseInputProvider {
  if (cachedInput) return cachedInput;
  if (!injectedInputFactory) {
    throw new Error(
      `Computer-use input capability is unavailable on ${process.platform}. ` +
        'Computer control requires a native input backend (macOS only); none is installed.',
    );
  }
  const provider = injectedInputFactory();
  if (!provider.isSupported) {
    throw new Error(
      `Computer-use input backend is not supported on ${process.platform}.`,
    );
  }
  return (cachedInput = provider);
}

/**
 * Build the computer-use executor over an input + clipboard backend. The native
 * backend is loaded lazily via {@link requireComputerUseInput} (which throws the
 * clear platform-unavailability message when none is installed), so a screenshot-
 * only or disabled flow never forces the native load. Parity: executor.ts
 * `createCliExecutor`.
 */
export function createComputerUseExecutor(
  opts: ComputerUseExecutorOptions = {},
): ComputerUseExecutor {
  const terminalBundleId = opts.terminalBundleId ?? null;
  const clipboard = opts.clipboard ?? createPbClipboard();
  const mouseAnimationEnabled = opts.getMouseAnimationEnabled ?? ((): boolean => true);
  const signal = opts.signal;
  // Lazy backend accessor — resolved on the first mouse/keyboard call.
  const getInput = (): ComputerUseInputProvider => opts.input ?? requireComputerUseInput();

  const moveAndSettle = async (
    input: ComputerUseInputProvider,
    x: number,
    y: number,
  ): Promise<void> => {
    await input.moveMouse(x, y, false);
    await sleep(MOVE_SETTLE_MS);
  };

  return {
    terminalBundleId,

    computeCaptureDims(display: DisplayGeometry): [number, number] {
      return computeTargetDims(display.width, display.height, display.scaleFactor);
    },

    async moveMouse(x: number, y: number): Promise<void> {
      await moveAndSettle(getInput(), x, y);
    },

    async click(x, y, button, count, modifiers): Promise<void> {
      const input = getInput();
      await moveAndSettle(input, x, y);
      if (modifiers && modifiers.length > 0) {
        await runBounded(
          () => withModifiers(input, modifiers, () => input.mouseButton(button, 'click', count)),
          { signal },
        );
      } else {
        await input.mouseButton(button, 'click', count);
      }
    },

    async drag(from, to): Promise<void> {
      const input = getInput();
      if (from !== undefined) {
        await moveAndSettle(input, from.x, from.y);
      }
      await input.mouseButton('left', 'press');
      await sleep(MOVE_SETTLE_MS);
      try {
        // Button is ALWAYS released even if the move throws (never left stuck).
        const start = await input.mouseLocation();
        const frames = planAnimatedMove(start, to, mouseAnimationEnabled());
        for (const f of frames) {
          await input.moveMouse(f.x, f.y, false);
          await sleep(f.sleepMs);
        }
      } finally {
        await input.mouseButton('left', 'release');
      }
    },

    async scroll(x, y, dx, dy): Promise<void> {
      const input = getInput();
      await moveAndSettle(input, x, y);
      // Vertical-first — the common axis; a horizontal failure shouldn't lose it.
      if (dy !== 0) await input.mouseScroll(dy, 'vertical');
      if (dx !== 0) await input.mouseScroll(dx, 'horizontal');
    },

    async key(keySequence, repeat): Promise<void> {
      const input = getInput();
      const parts = parseKeySequence(keySequence);
      const isEsc = isBareEscape(parts);
      const n = repeat ?? 1;
      await runBounded(async () => {
        for (let i = 0; i < n; i++) {
          if (i > 0) await sleep(KEY_REPEAT_CADENCE_MS);
          if (isEsc) opts.onExpectedEscape?.();
          await input.keys(parts);
        }
      }, { signal });
    },

    async holdKey(keyNames, durationMs): Promise<void> {
      const input = getInput();
      // `pressed` tracks landed presses; `orphaned` guards a timeout-orphan race
      // so a late lambda can't push after the finally snapshots.
      const pressed: string[] = [];
      let orphaned = false;
      try {
        await runBounded(async () => {
          for (const k of keyNames) {
            if (orphaned) return;
            if (isBareEscape([k])) opts.onExpectedEscape?.();
            await input.key(k, 'press');
            pressed.push(k);
          }
        }, { signal });
        await sleep(durationMs);
      } finally {
        orphaned = true;
        await runBounded(() => releasePressed(input, pressed), { signal });
      }
    },

    async type(text, typeOpts): Promise<void> {
      const input = getInput();
      if (typeOpts.viaClipboard) {
        await runBounded(() => typeViaClipboard(input, clipboard, text), { signal });
        return;
      }
      await input.typeText(text);
    },

    readClipboard(): Promise<string> {
      return clipboard.read();
    },
    writeClipboard(text: string): Promise<void> {
      return clipboard.write(text);
    },

    async getCursorPosition(): Promise<{ x: number; y: number }> {
      return getInput().mouseLocation();
    },

    async getFrontmostApp(): Promise<{ bundleId: string; displayName: string } | null> {
      const info = getInput().getFrontmostAppInfo();
      if (!info || !info.bundleId) return null;
      return { bundleId: info.bundleId, displayName: info.appName ?? info.bundleId };
    },

    async listInstalledApps(): Promise<Array<{ bundleId: string; displayName: string }>> {
      const input = getInput();
      if (!input.listInstalledApps) return [];
      return runBounded(() => input.listInstalledApps!(), { signal });
    },

    async openApp(bundleId: string): Promise<void> {
      const input = getInput();
      if (!input.openApp) {
        throw new Error('Opening apps is not supported by this computer-use backend.');
      }
      await input.openApp(bundleId);
    },
  };
}

// ── Host adapter (parity: hostAdapter.ts getComputerUseHostAdapter) ──────────

let cachedExecutor: ComputerUseExecutor | undefined;

/**
 * DI seam so tests can force {@link getComputerUseExecutor} to throw into the
 * tool's catch arm without a native backend.
 */
export const _computerUseExecutorHooks: {
  create: typeof createComputerUseExecutor;
} = { create: createComputerUseExecutor };

/**
 * Process-lifetime executor singleton used by the tool, built from the real
 * injected native backend + the pbcopy/pbpaste clipboard. Parity: hostAdapter.ts
 * `getComputerUseHostAdapter`. Throws the clear platform-unavailability message
 * (via the loader) on the first native call when no backend is installed.
 */
export function getComputerUseExecutor(
  opts?: Pick<ComputerUseExecutorOptions, 'terminalBundleId' | 'signal'>,
): ComputerUseExecutor {
  // A per-call signal must not be cached into the singleton; rebuild when supplied.
  if (opts?.signal) {
    return _computerUseExecutorHooks.create({
      ...opts,
      getMouseAnimationEnabled: () => readComputerUseConfig().mouseAnimation,
    });
  }
  cachedExecutor ??= _computerUseExecutorHooks.create({
    getMouseAnimationEnabled: () => readComputerUseConfig().mouseAnimation,
  });
  return cachedExecutor;
}
/** Test seam: clear the executor singleton. */
export function resetComputerUseExecutor(): void {
  cachedExecutor = undefined;
  _computerUseExecutorHooks.create = createComputerUseExecutor;
}
export type ComputerUseAction = (typeof COMPUTER_USE_ACTIONS)[number];

function asCoordinate(v: unknown): { x: number; y: number } | undefined {
  if (Array.isArray(v) && v.length === 2 && typeof v[0] === 'number' && typeof v[1] === 'number') {
    return { x: v[0], y: v[1] };
  }
  return undefined;
}

/**
 * Dispatch a parsed computer-use action against an executor. Pure of tool
 * plumbing so it is unit-testable with a fake executor; returns the human-readable
 * output string (and, for queries, the structured value). Throws a validation
 * Error for a malformed action/params.
 */
export async function dispatchComputerUseAction(
  executor: ComputerUseExecutor,
  action: ComputerUseAction,
  input: Record<string, unknown>,
): Promise<string> {
  const coord = asCoordinate(input.coordinate);
  const modifiers = Array.isArray(input.modifiers)
    ? (input.modifiers.filter((m) => typeof m === 'string') as string[])
    : undefined;
  const needCoord = (): { x: number; y: number } => {
    if (!coord) throw new Error(`Action '${action}' requires a [x, y] coordinate.`);
    return coord;
  };
  switch (action) {
    case 'cursor_position': {
      const p = await executor.getCursorPosition();
      return `cursor at (${p.x}, ${p.y})`;
    }
    case 'mouse_move': {
      const p = needCoord();
      await executor.moveMouse(p.x, p.y);
      return `moved to (${p.x}, ${p.y})`;
    }
    case 'left_click':
    case 'right_click':
    case 'middle_click': {
      const p = needCoord();
      const button = action === 'left_click' ? 'left' : action === 'right_click' ? 'right' : 'middle';
      await executor.click(p.x, p.y, button, 1, modifiers);
      return `${button} click at (${p.x}, ${p.y})`;
    }
    case 'double_click':
    case 'triple_click': {
      const p = needCoord();
      const count = action === 'double_click' ? 2 : 3;
      await executor.click(p.x, p.y, 'left', count, modifiers);
      return `${count}x click at (${p.x}, ${p.y})`;
    }
    case 'left_click_drag': {
      const to = needCoord();
      const from = asCoordinate(input.start_coordinate);
      await executor.drag(from, to);
      return `dragged to (${to.x}, ${to.y})`;
    }
    case 'scroll': {
      const p = needCoord();
      const dx = typeof input.scroll_x === 'number' ? input.scroll_x : 0;
      const dy = typeof input.scroll_y === 'number' ? input.scroll_y : 0;
      await executor.scroll(p.x, p.y, dx, dy);
      return `scrolled (${dx}, ${dy}) at (${p.x}, ${p.y})`;
    }
    case 'key': {
      if (typeof input.text !== 'string' || input.text.length === 0) {
        throw new Error("Action 'key' requires a non-empty 'text' key sequence.");
      }
      const repeat = typeof input.repeat === 'number' ? input.repeat : undefined;
      await executor.key(input.text, repeat);
      return `pressed ${input.text}${repeat && repeat > 1 ? ` x${repeat}` : ''}`;
    }
    case 'hold_key': {
      const keyNames = Array.isArray(input.keys)
        ? (input.keys.filter((k) => typeof k === 'string') as string[])
        : [];
      if (keyNames.length === 0) throw new Error("Action 'hold_key' requires a non-empty 'keys' array.");
      const durationMs = typeof input.duration === 'number' ? input.duration : 0;
      await executor.holdKey(keyNames, durationMs);
      return `held ${keyNames.join('+')} for ${durationMs}ms`;
    }
    case 'type': {
      if (typeof input.text !== 'string') throw new Error("Action 'type' requires a 'text' string.");
      await executor.type(input.text, { viaClipboard: input.via_clipboard === true });
      return `typed ${input.text.length} chars`;
    }
    case 'read_clipboard': {
      return await executor.readClipboard();
    }
    case 'write_clipboard': {
      if (typeof input.text !== 'string') throw new Error("Action 'write_clipboard' requires a 'text' string.");
      await executor.writeClipboard(input.text);
      return `wrote ${input.text.length} chars to clipboard`;
    }
    case 'get_frontmost_app': {
      const app = await executor.getFrontmostApp();
      return app ? `${app.displayName} (${app.bundleId})` : 'no frontmost app';
    }
    case 'list_apps': {
      const apps = await executor.listInstalledApps();
      return apps.length === 0
        ? 'no installed apps reported'
        : apps.map((a) => `${a.displayName} (${a.bundleId})`).join('\n');
    }
    case 'open_app': {
      if (typeof input.bundleId !== 'string' || input.bundleId.length === 0) {
        throw new Error("Action 'open_app' requires a 'bundleId' string.");
      }
      await executor.openApp(input.bundleId);
      return `opened ${input.bundleId}`;
    }
  }
}

export const computerUseTool: ToolDefinition = {
  name: COMPUTER_USE_TOOL_NAME,
  description:
    'Control the desktop GUI: move/click the mouse, press keys, type text, manage the clipboard, ' +
    'and inspect/launch applications. macOS only; requires a native input backend. Opt-in via ' +
    `the ${COMPUTER_USE_ENV} env var.`,
  searchHint: 'gui automation mouse keyboard screenshot clipboard app control desktop macos',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: [...COMPUTER_USE_ACTIONS],
        description: 'The GUI action to perform.',
      },
      coordinate: {
        type: 'array',
        items: { type: 'number' },
        description: '[x, y] target in the active coordinate mode (pixels or normalized).',
      },
      start_coordinate: {
        type: 'array',
        items: { type: 'number' },
        description: 'Optional [x, y] drag start for left_click_drag (defaults to the current cursor).',
      },
      modifiers: {
        type: 'array',
        items: { type: 'string' },
        description: 'Modifier keys held during a click (e.g. ["command"]).',
      },
      text: { type: 'string', description: 'Text to type, key sequence for key, or clipboard text.' },
      via_clipboard: { type: 'boolean', description: 'For type: paste via Cmd+V instead of per-grapheme typing.' },
      repeat: { type: 'number', description: 'For key: repeat the key sequence this many times.' },
      keys: { type: 'array', items: { type: 'string' }, description: 'For hold_key: keys to hold down.' },
      duration: { type: 'number', description: 'For hold_key: hold duration in ms.' },
      scroll_x: { type: 'number', description: 'For scroll: horizontal delta.' },
      scroll_y: { type: 'number', description: 'For scroll: vertical delta.' },
      bundleId: { type: 'string', description: 'For open_app: the application bundle id.' },
    },
    required: ['action'],
    additionalProperties: false,
  },
  isReadOnly: false,
  isReadOnlyForInput: (input): boolean => {
    const a = input.action;
    return a === 'cursor_position' || a === 'get_frontmost_app' || a === 'list_apps' || a === 'read_clipboard';
  },
  // Effects reach outside the deterministic local environment (drives the OS GUI).
  isOpenWorld: true,
  // Dropped from the advertised pool unless opted in (parity: isDisabled ListTools gate).
  isEnabled: () => isComputerUseEnabled(),
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const action = input.action;
    if (typeof action !== 'string' || !(COMPUTER_USE_ACTIONS as readonly string[]).includes(action)) {
      return {
        output: `Unknown computer-use action '${String(action)}'. Valid actions: ${COMPUTER_USE_ACTIONS.join(', ')}.`,
        isError: true,
      };
    }
    let executor: ComputerUseExecutor;
    try {
      executor = getComputerUseExecutor({ signal: context.abortSignal });
    } catch (err) {
      // The loader surfaces the clear platform-unavailability message here.
      return { output: errorMessage(err), isError: true };
    }
    try {
      const output = await dispatchComputerUseAction(executor, action as ComputerUseAction, input);
      return { output };
    } catch (err) {
      return { output: `Computer-use ${action} failed: ${errorMessage(err)}`, isError: true };
    }
  },
};
