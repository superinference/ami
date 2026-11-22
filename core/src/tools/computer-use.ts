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
import { sleep, withTimeout } from '../utils/sleep';
import { execFileNoThrow, type ExecFileNoThrowResult } from '../utils/shell';
import { errorMessage } from '../credential-guard';

// ── Config knobs + coordinate mode (parity: gates.ts) ────────────────────────

/** How model-supplied coordinates are interpreted. Parity: the reference's `CoordinateMode`. */
export const COORDINATE_MODES = ['pixels', 'normalized'] as const;
export type CoordinateMode = (typeof COORDINATE_MODES)[number];

/**
 * Behavioral sub-gates for the computer-use capability. Parity: the reference's
 * `CuSubGates` (gates.ts DEFAULTS, minus `enabled`/`coordinateMode`).
 */
export interface ComputerUseSubGates {
  /** Decode+crop a JPEG patch to verify a click landed on the expected pixels. */
  pixelValidation: boolean;
  /** Allow the Cmd+V clipboard-paste typing path for multi-line text. */
  clipboardPasteMultiline: boolean;
  /** Ease-out animate the cursor on drag motion (vs an instant move). */
  mouseAnimation: boolean;
  /** Hide non-allow-listed windows before a screenshot/action. */
  hideBeforeAction: boolean;
  /** Auto-resolve the display a target app is on instead of the main display. */
  autoTargetDisplay: boolean;
  /** Save+restore the user's clipboard around a paste so it is never clobbered. */
  clipboardGuard: boolean;
}

/** Full computer-use config: the sub-gates plus the top-level enable + coordinate mode. */
export interface ComputerUseConfig extends ComputerUseSubGates {
  enabled: boolean;
  coordinateMode: CoordinateMode;
}

/**
 * Typed defaults. Parity: the reference's gates.ts `DEFAULTS`. A partial config spreads
 * OVER these so e.g. `{ enabled: true }` alone inherits every other field (parity:
 * the reference's `{ ...DEFAULTS, ...getDynamicConfig(...) }` partial-over-defaults merge).
 */
export const DEFAULT_COMPUTER_USE_CONFIG: ComputerUseConfig = {
  enabled: false,
  pixelValidation: false,
  clipboardPasteMultiline: true,
  mouseAnimation: true,
  hideBeforeAction: true,
  autoTargetDisplay: true,
  clipboardGuard: true,
  coordinateMode: 'pixels',
};

/**
 * Merge a partial config over {@link DEFAULT_COMPUTER_USE_CONFIG} so an operator
 * supplying only a subset of knobs inherits the rest (no `undefined` fields leak
 * through). Parity: gates.ts `readConfig()`.
 */
export function readComputerUseConfig(partial?: Partial<ComputerUseConfig>): ComputerUseConfig {
  return { ...DEFAULT_COMPUTER_USE_CONFIG, ...(partial ?? {}) };
}

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

/** Opt-in env var enabling the computer-use tool. OFF by default. Accepts `1`/`true`. */
export const COMPUTER_USE_ENV = 'AMI_COMPUTER_USE';

/** Whether the computer-use tool is enabled (parity: the reference's `adapter.isDisabled()` inverse). */
export function isComputerUseEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[COMPUTER_USE_ENV];
  if (raw === undefined) return false;
  const v = raw.trim().toLowerCase();
  return v === '1' || v === 'true';
}

// ── Timing / motion tunables (parity: executor.ts L57-L255) ──────────────────

/** JPEG quality for screenshots. Parity: executor.ts `SCREENSHOT_JPEG_QUALITY`. */
export const SCREENSHOT_JPEG_QUALITY = 0.75;
/** Instant-move → HID→AppKit round-trip settle before reading cursor/dispatching a click. */
export const MOVE_SETTLE_MS = 50;
/** Cursor-animation frame rate (parity: executor.ts `frameRate = 60`). */
export const ANIMATION_FRAME_RATE = 60;
/** Distance-proportional animation speed (parity: executor.ts `distance / 2000`). */
export const ANIMATION_PX_PER_SEC = 2000;
/** Max animated-move duration cap (parity: executor.ts `Math.min(..., 0.5)`). */
export const ANIMATION_MAX_DURATION_SEC = 0.5;
/** Below this duration, fall through to an instant move (parity: executor.ts `< 0.03`). */
export const ANIMATION_MIN_DURATION_SEC = 0.03;
/** Inter-repeat key cadence — 125Hz USB polling (parity: executor.ts `sleep(8)`). */
export const KEY_REPEAT_CADENCE_MS = 8;
/** Clipboard paste-vs-restore settle (parity: executor.ts typeViaClipboard `sleep(100)`). */
export const CLIPBOARD_PASTE_SETTLE_MS = 100;
/** Abort/timeout ceiling for a bounded native run-loop call (parity: drainRunLoop 30s). */
export const RUN_LOOP_TIMEOUT_MS = 30_000;
/** Installed-app enumeration timeout (parity: mcpServer.ts `APP_ENUM_TIMEOUT_MS = 1000`). */
export const APP_ENUM_TIMEOUT_MS = 1_000;

/**
 * API image-resize bounds. The screenshot path pre-sizes the capture to within
 * these so the API transcoder's early-return fires (no server-side resize, so
 * coordinate scaling stays coherent). Parity: the reference's `API_RESIZE_PARAMS`
 * (values are AMI's — the shape/intent is the parity target, not the proprietary
 * numbers).
 */
export interface ResizeParams {
  /** Max pixels (w*h) before the image is scaled down. */
  maxPixels: number;
  /** Max length of the longer edge before the image is scaled down. */
  maxLongEdge: number;
}
export const API_RESIZE_PARAMS: ResizeParams = {
  maxPixels: 1_192_464,
  maxLongEdge: 1568,
};

/**
 * Scale a physical (w,h) down to fit within `params` while preserving aspect
 * ratio; never upscales. Parity: the reference's `targetImageSize`. Returns integer dims.
 */
export function targetImageSize(
  physW: number,
  physH: number,
  params: ResizeParams = API_RESIZE_PARAMS,
): [number, number] {
  if (physW <= 0 || physH <= 0) return [0, 0];
  // Longer-edge bound.
  const longEdge = Math.max(physW, physH);
  const edgeScale = longEdge > params.maxLongEdge ? params.maxLongEdge / longEdge : 1;
  // Pixel-budget bound.
  const pixels = physW * physH;
  const pixelScale = pixels > params.maxPixels ? Math.sqrt(params.maxPixels / pixels) : 1;
  const scale = Math.min(edgeScale, pixelScale);
  return [Math.max(1, Math.round(physW * scale)), Math.max(1, Math.round(physH * scale))];
}

/**
 * Logical → physical (×scaleFactor) → API target dims. Parity: executor.ts
 * `computeTargetDims`.
 */
export function computeTargetDims(
  logicalW: number,
  logicalH: number,
  scaleFactor: number,
): [number, number] {
  const physW = Math.round(logicalW * scaleFactor);
  const physH = Math.round(logicalH * scaleFactor);
  return targetImageSize(physW, physH, API_RESIZE_PARAMS);
}

// ── Keyboard helpers (parity: executor.ts L92-101, L455-473) ─────────────────

/** Split an xdotool-style "ctrl+shift+a" sequence into parts, dropping empties. */
export function parseKeySequence(keySequence: string): string[] {
  return keySequence.split('+').filter((p) => p.length > 0);
}

/**
 * Single-element key sequence matching "escape"/"esc" (case-insensitive). Used to
 * hole-punch the abort tap for a model-synthesized Escape while `ctrl+escape`
 * still passes. Parity: executor.ts `isBareEscape`.
 */
export function isBareEscape(parts: readonly string[]): boolean {
  if (parts.length !== 1) return false;
  const lower = parts[0]!.toLowerCase();
  return lower === 'escape' || lower === 'esc';
}

// ── Animation planning (parity: executor.ts animatedMove L208-255) ───────────

/** One planned cursor step: an absolute target plus the sleep that follows it. */
export interface AnimationFrame {
  x: number;
  y: number;
  /** Sleep AFTER moving to (x,y), in ms. */
  sleepMs: number;
}

/**
 * Pure planner for an ease-out-cubic (1-(1-t)^3) cursor animation at
 * {@link ANIMATION_FRAME_RATE}fps, distance-proportional duration at
 * {@link ANIMATION_PX_PER_SEC} capped at {@link ANIMATION_MAX_DURATION_SEC}. When
 * animation is disabled, the distance is sub-pixel, or the computed duration is
 * below {@link ANIMATION_MIN_DURATION_SEC}, falls through to a single instant move
 * with the {@link MOVE_SETTLE_MS} settle. Returns an empty list for a <1px no-op.
 * Parity: executor.ts `animatedMove` (extracted as a testable pure function).
 */
export function planAnimatedMove(
  start: { x: number; y: number },
  target: { x: number; y: number },
  mouseAnimationEnabled: boolean,
): AnimationFrame[] {
  const deltaX = target.x - start.x;
  const deltaY = target.y - start.y;
  const distance = Math.hypot(deltaX, deltaY);
  if (!mouseAnimationEnabled) {
    return [{ x: target.x, y: target.y, sleepMs: MOVE_SETTLE_MS }];
  }
  if (distance < 1) return [];
  const durationSec = Math.min(distance / ANIMATION_PX_PER_SEC, ANIMATION_MAX_DURATION_SEC);
  if (durationSec < ANIMATION_MIN_DURATION_SEC) {
    return [{ x: target.x, y: target.y, sleepMs: MOVE_SETTLE_MS }];
  }
  const frameIntervalMs = 1000 / ANIMATION_FRAME_RATE;
  const totalFrames = Math.floor(durationSec * ANIMATION_FRAME_RATE);
  const frames: AnimationFrame[] = [];
  for (let frame = 1; frame <= totalFrames; frame++) {
    const t = frame / totalFrames;
    const eased = 1 - Math.pow(1 - t, 3);
    frames.push({
      x: Math.round(start.x + deltaX * eased),
      y: Math.round(start.y + deltaY * eased),
      // Last frame has no inter-frame sleep; it gets the HID settle instead.
      sleepMs: frame < totalFrames ? frameIntervalMs : MOVE_SETTLE_MS,
    });
  }
  return frames;
}

// ── Capture targeting helpers (parity: executor.ts withoutTerminal L276-292) ──

/**
 * Strip the host terminal's bundle id from a screenshot allow-list so the terminal
 * never photobombs a capture. Parity: executor.ts `withoutTerminal`.
 */
export function withoutTerminal(
  allowed: readonly string[],
  terminalBundleId: string | null,
): string[] {
  return terminalBundleId === null
    ? [...allowed]
    : allowed.filter((id) => id !== terminalBundleId);
}

// ── Bounded run loop (parity: drainRunLoop.ts) ───────────────────────────────

/**
 * Run `fn()` bounded by {@link RUN_LOOP_TIMEOUT_MS} (or `timeoutMs`) and the
 * provided abort signal, so a wedged native call can never hang the turn. Parity:
 * the reference's `drainRunLoop` (a bounded run-loop pump with a 30s ceiling wrapping
 * each native call) — AMI has no CFRunLoop, so this is the abort/timeout boundary
 * only, implemented on the canonical {@link withTimeout}.
 */
export async function runBounded<T>(
  fn: () => Promise<T>,
  opts?: { timeoutMs?: number; signal?: AbortSignal },
): Promise<T> {
  if (opts?.signal?.aborted) {
    throw new Error('Computer-use action aborted before it started.');
  }
  return withTimeout(
    fn(),
    opts?.timeoutMs ?? RUN_LOOP_TIMEOUT_MS,
    `Computer-use native call exceeded ${opts?.timeoutMs ?? RUN_LOOP_TIMEOUT_MS}ms.`,
  );
}

// ── Native input backend (parity: inputLoader.ts) ────────────────────────────

/**
 * Native keyboard/mouse backend. Parity: the narrowed `ComputerUseInputAPI`
 * (`@ant/computer-use-input`). A host supplies a real implementation via
 * {@link setComputerUseInputProvider}; `isSupported` discriminates the platform-
 * narrowed union so callers get a bare provider without re-checking.
 */
export interface ComputerUseInputProvider {
  readonly isSupported: boolean;
  moveMouse(x: number, y: number, animate: boolean): Promise<void>;
  mouseLocation(): Promise<{ x: number; y: number }>;
  mouseButton(
    button: 'left' | 'right' | 'middle',
    phase: 'press' | 'release' | 'click',
    count?: 1 | 2 | 3,
  ): Promise<void>;
  mouseScroll(amount: number, axis: 'vertical' | 'horizontal'): Promise<void>;
  key(name: string, phase: 'press' | 'release'): Promise<void>;
  keys(parts: readonly string[]): Promise<void>;
  typeText(text: string): Promise<void>;
  getFrontmostAppInfo(): { bundleId?: string; appName?: string } | null;
  listInstalledApps?(): Promise<Array<{ bundleId: string; displayName: string }>>;
  openApp?(bundleId: string): Promise<void>;
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

// ── Clipboard backend (parity: executor.ts pbcopy/pbpaste L70-88) ────────────

/** Read/write the system clipboard. Parity: the reference's pbpaste/pbcopy executor pair. */
export interface ClipboardProvider {
  read(): Promise<string>;
  write(text: string): Promise<void>;
}

type ExecFn = (
  file: string,
  args: string[],
  options?: { input?: string },
) => Promise<ExecFileNoThrowResult>;

/**
 * Default clipboard backend shelling out to `pbpaste`/`pbcopy` via the canonical
 * {@link execFileNoThrow}. Parity: executor.ts `readClipboardViaPbpaste` /
 * `writeClipboardViaPbcopy`. The exec fn is injectable so the round-trip / error
 * paths are testable without a real clipboard.
 */
export function createPbClipboard(exec: ExecFn = execFileNoThrow): ClipboardProvider {
  return {
    async read(): Promise<string> {
      const { stdout, code } = await exec('pbpaste', []);
      if (code !== 0) throw new Error(`pbpaste exited with code ${code}`);
      return stdout;
    },
    async write(text: string): Promise<void> {
      const { code } = await exec('pbcopy', [], { input: text });
      if (code !== 0) throw new Error(`pbcopy exited with code ${code}`);
    },
  };
}

// ── Modifier bracketing (parity: executor.ts L122-165) ───────────────────────

/**
 * Release `pressed` in reverse (last pressed = first released), draining via
 * pop() so a late push is still released on the next iteration. Errors swallowed
 * (best-effort) so a release failure never masks the primary error. Parity:
 * executor.ts `releasePressed`.
 */
export async function releasePressed(
  input: ComputerUseInputProvider,
  pressed: string[],
): Promise<void> {
  let k: string | undefined;
  while ((k = pressed.pop()) !== undefined) {
    try {
      await input.key(k, 'release');
    } catch {
      // Swallow — best-effort release.
    }
  }
}

/**
 * Bracket `fn()` with modifier press/release. `pressed` tracks which presses
 * actually landed, so a mid-press throw only releases what was pressed — no stuck
 * modifiers. The finally covers both press-phase and fn() throws. Parity:
 * executor.ts `withModifiers`.
 */
export async function withModifiers<T>(
  input: ComputerUseInputProvider,
  mods: readonly string[],
  fn: () => Promise<T>,
): Promise<T> {
  const pressed: string[] = [];
  try {
    for (const m of mods) {
      await input.key(m, 'press');
      pressed.push(m);
    }
    return await fn();
  } finally {
    await releasePressed(input, pressed);
  }
}

/**
 * Read-back-verified clipboard paste. Save → write → READ-BACK VERIFY (never Cmd+V
 * on mismatch) → Cmd+V → settle → restore-in-finally (a throw never clobbers the
 * user's clipboard; restore failures swallowed). Parity: executor.ts
 * `typeViaClipboard`.
 */
export async function typeViaClipboard(
  input: ComputerUseInputProvider,
  clipboard: ClipboardProvider,
  text: string,
): Promise<void> {
  let saved: string | undefined;
  try {
    saved = await clipboard.read();
  } catch {
    // Proceed without restore if the save read fails.
  }
  try {
    await clipboard.write(text);
    if ((await clipboard.read()) !== text) {
      throw new Error('Clipboard write did not round-trip.');
    }
    await input.keys(['command', 'v']);
    await sleep(CLIPBOARD_PASTE_SETTLE_MS);
  } finally {
    if (typeof saved === 'string') {
      try {
        await clipboard.write(saved);
      } catch {
        // Swallow — best-effort restore.
      }
    }
  }
}

// ── OS-permission preflight (parity: hostAdapter.ts L47-54) ──────────────────

/** Granular preflight result; a UI can tell the user exactly which grant is missing. */
export type OsPermissionState =
  | { granted: true }
  | { granted: false; accessibility: boolean; screenRecording: boolean };

/** Probe for the two macOS TCC grants computer-use needs. */
export interface OsPermissionProbe {
  checkAccessibility(): boolean;
  checkScreenRecording(): boolean;
}

/**
 * Preflight the OS permissions, returning a structured state with per-grant flags
 * when not fully granted. Parity: hostAdapter.ts `ensureOsPermissions`.
 */
export function ensureOsPermissions(probe: OsPermissionProbe): OsPermissionState {
  const accessibility = probe.checkAccessibility();
  const screenRecording = probe.checkScreenRecording();
  return accessibility && screenRecording
    ? { granted: true }
    : { granted: false, accessibility, screenRecording };
}

// ── Fail-soft timed app enumeration (parity: mcpServer.ts L25-44) ────────────

/**
 * Race an installed-app enumeration against {@link APP_ENUM_TIMEOUT_MS}, failing
 * soft: a slow/throwing enumeration resolves `undefined` (callers just omit the
 * hint list) while the real enumeration continues in the background with its late
 * rejection swallowed (no unhandled-rejection crash). Parity: mcpServer.ts
 * `tryGetInstalledAppNames`.
 */
export async function tryGetInstalledAppNames(
  listInstalled: () => Promise<string[]>,
  timeoutMs: number = APP_ENUM_TIMEOUT_MS,
): Promise<string[] | undefined> {
  const enumP = listInstalled();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutP = new Promise<undefined>((resolve) => {
    timer = setTimeout(resolve, timeoutMs, undefined);
    (timer as { unref?: () => void }).unref?.();
  });
  const installed = await Promise.race([enumP, timeoutP])
    .catch(toUndefined)
    .finally(() => clearTimeout(timer));
  if (!installed) {
    // Enumeration continues in the background — swallow any late rejection.
    void enumP.catch(toUndefined);
    return undefined;
  }
  return installed;
}

/** Shared rejection→undefined for fire-and-forget enum promises. Exported for coverage. */
export function toUndefined(): undefined { return undefined; }

// ── Executor (parity: executor.ts createCliExecutor) ─────────────────────────

/** The capture/geometry info a screenshot returns. */
export interface ScreenshotResult {
  base64: string;
  width: number;
  height: number;
}

/** Display geometry used to size captures. */
export interface DisplayGeometry {
  width: number;
  height: number;
  scaleFactor: number;
}

/**
 * The computer-use executor surface. Parity: the reference's `ComputerExecutor` (the
 * subset AMI ports — platform-independent orchestration over the native backend).
 */
export interface ComputerUseExecutor {
  moveMouse(x: number, y: number): Promise<void>;
  click(
    x: number,
    y: number,
    button: 'left' | 'right' | 'middle',
    count: 1 | 2 | 3,
    modifiers?: string[],
  ): Promise<void>;
  drag(from: { x: number; y: number } | undefined, to: { x: number; y: number }): Promise<void>;
  scroll(x: number, y: number, dx: number, dy: number): Promise<void>;
  key(keySequence: string, repeat?: number): Promise<void>;
  holdKey(keyNames: string[], durationMs: number): Promise<void>;
  type(text: string, opts: { viaClipboard: boolean }): Promise<void>;
  readClipboard(): Promise<string>;
  writeClipboard(text: string): Promise<void>;
  getCursorPosition(): Promise<{ x: number; y: number }>;
  getFrontmostApp(): Promise<{ bundleId: string; displayName: string } | null>;
  listInstalledApps(): Promise<Array<{ bundleId: string; displayName: string }>>;
  openApp(bundleId: string): Promise<void>;
  /** Compute the target capture dims for a display without native capture (used by
   *  the no-screenshot-backend path to report the intended size). */
  computeCaptureDims(display: DisplayGeometry): [number, number];
  /** The host terminal bundle id, stripped from screenshot allow-lists. */
  readonly terminalBundleId: string | null;
}

/** Options for {@link createComputerUseExecutor}. */
export interface ComputerUseExecutorOptions {
  /** Injected native input backend; defaults to {@link requireComputerUseInput}. */
  input?: ComputerUseInputProvider;
  /** Injected clipboard backend; defaults to {@link createPbClipboard}. */
  clipboard?: ClipboardProvider;
  /** Whether cursor animation is on (parity: gates mouseAnimation). */
  getMouseAnimationEnabled?: () => boolean;
  /** The host terminal bundle id (stripped from screenshot allow-lists). */
  terminalBundleId?: string | null;
  /** Abort signal threaded into bounded native calls. */
  signal?: AbortSignal;
  /** Notify the abort tap of an expected model-synthesized Escape. Parity: `notifyExpectedEscape`. */
  onExpectedEscape?: () => void;
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

// ── Tool definition ──────────────────────────────────────────────────────────

/** Model/UI-facing name of the computer-use tool (snake_case per AMI convention). */
export const COMPUTER_USE_TOOL_NAME = 'computer_use';

/** The actions the tool dispatches. Parity: the reference's computer-use tool action set. */
export const COMPUTER_USE_ACTIONS = [
  'cursor_position',
  'mouse_move',
  'left_click',
  'right_click',
  'middle_click',
  'double_click',
  'triple_click',
  'left_click_drag',
  'scroll',
  'key',
  'hold_key',
  'type',
  'read_clipboard',
  'write_clipboard',
  'get_frontmost_app',
  'list_apps',
  'open_app',
] as const;
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
