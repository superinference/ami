// Split from computer-use.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { sleep, withTimeout } from '../utils/sleep';
import { execFileNoThrow, type ExecFileNoThrowResult } from '../utils/shell';

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

// ── Clipboard backend (parity: executor.ts pbcopy/pbpaste L70-88) ────────────

/** Read/write the system clipboard. Parity: the reference's pbpaste/pbcopy executor pair. */
export interface ClipboardProvider {
  read(): Promise<string>;
  write(text: string): Promise<void>;
}

export type ExecFn = (
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
