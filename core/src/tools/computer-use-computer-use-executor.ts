// Split from computer-use.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { APP_ENUM_TIMEOUT_MS } from './computer-use-plan-animated-move';
import type { ClipboardProvider, ComputerUseInputProvider, OsPermissionState } from './computer-use-plan-animated-move';

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
