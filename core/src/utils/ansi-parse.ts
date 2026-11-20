/**
 * Defensive inbound ANSI / SGR parser.
 *
 * This is the single, lowest-layer home for turning captured terminal / tool
 * output (which may contain arbitrary, malformed, or truncated escape
 * sequences) into structured, styled text. It is the counterpart to AMI's
 * ANSI *emitters* (cli/src/render.ts, common/src/tips.ts) and *exporters*
 * (common/src/ansi-to-png.ts) — those produce ANSI, this consumes it.
 *
 * Parity target: the reference's `parseAnsi()` / `get256Color()`
 * (references/claude/claude-code-main/src/utils/ansiToSvg.ts) plus the
 * graceful-degradation Action model from its termio parser
 * (references/claude/claude-code-main/src/ink/termio/types.ts:L223-236), where
 * an unrecognized escape sequence is preserved verbatim as a typed `unknown`
 * token rather than being silently dropped or leaking through as literal text.
 *
 * Design invariants (the "defensive" in the name):
 *  - NEVER throws on malformed / partial / out-of-range input.
 *  - A truncated extended-color sequence (`38;2;r` with missing channels, or
 *    `38;5` with no index) is ignored, never producing NaN colors.
 *  - Unknown / non-SGR sequences are captured as `{ type: 'unknown' }` so
 *    callers can round-trip or diagnose them; the span builder drops them from
 *    rendered text so they never corrupt the visible output.
 *
 * Layer note: this is a self-contained core primitive (no engine/UI/common
 * dependency). common/src/ansi-to-png.ts consumes it so there is exactly one
 * SGR parser + 256-color palette in the tree. It does NOT duplicate
 * cli/src/termio-tokenize.ts, which is a *streaming stdin reassembler* (text vs
 * raw-sequence) for input handling — a different direction and purpose.
 */

/** An 8-bit-per-channel opaque RGB color. */
export interface AnsiColor {
  r: number;
  g: number;
  b: number;
}

/** One run of same-styled characters within a parsed line. */
export interface AnsiSpan {
  text: string;
  color: AnsiColor;
  bold: boolean;
}

/** A parsed line = an ordered list of styled spans. */
export type AnsiSpanLine = AnsiSpan[];

/**
 * Semantic token produced by {@link tokenizeAnsi}. Mirrors the error-handling
 * boundary of the reference's termio `Action` union: recognized control events
 * are first-class, and anything unrecognized is preserved verbatim.
 */
export type AnsiToken =
  | { type: 'text'; text: string }
  /** Select Graphic Rendition — the raw parameter string (without ESC `[` / `m`). */
  | { type: 'sgr'; params: string }
  /** BEL (`\x07`). */
  | { type: 'bell' }
  /** Full terminal reset (ESC `c`). */
  | { type: 'reset' }
  /** Any escape sequence we do not model — kept verbatim for round-trip/diagnosis. */
  | { type: 'unknown'; sequence: string };

/**
 * Standard + bright 16-color SGR palette (codes 30–37 / 90–97). Matches the
 * reference `ANSI_COLORS` map and most terminal themes.
 */
export const ANSI_COLORS: readonly AnsiColor[] = [
  { r: 0, g: 0, b: 0 }, // 0 black (30)
  { r: 205, g: 49, b: 49 }, // 1 red (31)
  { r: 13, g: 188, b: 121 }, // 2 green (32)
  { r: 229, g: 229, b: 16 }, // 3 yellow (33)
  { r: 36, g: 114, b: 200 }, // 4 blue (34)
  { r: 188, g: 63, b: 188 }, // 5 magenta (35)
  { r: 17, g: 168, b: 205 }, // 6 cyan (36)
  { r: 229, g: 229, b: 229 }, // 7 white (37)
  { r: 102, g: 102, b: 102 }, // 8 bright black / gray (90)
  { r: 241, g: 76, b: 76 }, // 9 bright red (91)
  { r: 35, g: 209, b: 139 }, // 10 bright green (92)
  { r: 245, g: 245, b: 67 }, // 11 bright yellow (93)
  { r: 59, g: 142, b: 234 }, // 12 bright blue (94)
  { r: 214, g: 112, b: 214 }, // 13 bright magenta (95)
  { r: 41, g: 184, b: 219 }, // 14 bright cyan (96)
  { r: 255, g: 255, b: 255 }, // 15 bright white (97)
];

/** Default foreground for un-colored text (reference `DEFAULT_FG`). */
export const DEFAULT_FG: AnsiColor = { r: 229, g: 229, b: 229 };
/** Default background (reference `DEFAULT_BG`). */
export const DEFAULT_BG: AnsiColor = { r: 30, g: 30, b: 30 };

/**
 * Resolve an xterm-256 palette index to an RGB color (reference `get256Color`).
 *
 * - 0–15:   the dim legacy 16-color cube (distinct from the brighter SGR
 *           {@link ANSI_COLORS} theme — this matches real xterm behavior).
 * - 16–231: the 6×6×6 color cube (`55 + n·40` per channel, 0 for n=0).
 * - 232–255: the 24-step grayscale ramp (`(idx-232)·10 + 8`).
 *
 * Out-of-range / non-integer indices fall back to {@link DEFAULT_FG}.
 */
export function get256Color(index: number): AnsiColor {
  if (!Number.isInteger(index) || index < 0 || index > 255) return DEFAULT_FG;
  if (index < 16) {
    const cube16: readonly AnsiColor[] = [
      { r: 0, g: 0, b: 0 },
      { r: 128, g: 0, b: 0 },
      { r: 0, g: 128, b: 0 },
      { r: 128, g: 128, b: 0 },
      { r: 0, g: 0, b: 128 },
      { r: 128, g: 0, b: 128 },
      { r: 0, g: 128, b: 128 },
      { r: 192, g: 192, b: 192 },
      { r: 128, g: 128, b: 128 },
      { r: 255, g: 0, b: 0 },
      { r: 0, g: 255, b: 0 },
      { r: 255, g: 255, b: 0 },
      { r: 0, g: 0, b: 255 },
      { r: 255, g: 0, b: 255 },
      { r: 0, g: 255, b: 255 },
      { r: 255, g: 255, b: 255 },
    ];
    return cube16[index] ?? DEFAULT_FG;
  }
  if (index < 232) {
    const i = index - 16;
    const r = Math.floor(i / 36);
    const g = Math.floor((i % 36) / 6);
    const b = i % 6;
    return {
      r: r === 0 ? 0 : 55 + r * 40,
      g: g === 0 ? 0 : 55 + g * 40,
      b: b === 0 ? 0 : 55 + b * 40,
    };
  }
  const gray = (index - 232) * 10 + 8;
  return { r: gray, g: gray, b: gray };
}

/** Terminal control bytes we classify out of the text stream. */
const ESC = '\x1b';
const BEL = '\x07';

/** A CSI final byte is in the range `@`–`~` (0x40–0x7e). */
function isCsiFinal(ch: string): boolean {
  return ch >= '@' && ch <= '~';
}

/**
 * Tokenize arbitrary (possibly malformed) ANSI text into a flat stream of
 * {@link AnsiToken}s. Never throws; any sequence it does not model is emitted
 * as `{ type: 'unknown' }` with the raw bytes preserved. Plain text — including
 * newlines and carriage returns — is emitted as `text` tokens.
 */
export function tokenizeAnsi(input: string): AnsiToken[] {
  const tokens: AnsiToken[] = [];
  let buf = '';
  const flush = (): void => {
    if (buf) {
      tokens.push({ type: 'text', text: buf });
      buf = '';
    }
  };

  for (let i = 0; i < input.length; i++) {
    const ch = input[i]!;
    if (ch === BEL) {
      flush();
      tokens.push({ type: 'bell' });
      continue;
    }
    if (ch !== ESC) {
      buf += ch;
      continue;
    }
    // An escape sequence begins here.
    flush();
    const next = input[i + 1];
    if (next === undefined) {
      // Dangling ESC at end of input — preserve it as unknown.
      tokens.push({ type: 'unknown', sequence: ESC });
      break;
    }
    if (next === 'c') {
      // ESC c — full terminal reset.
      tokens.push({ type: 'reset' });
      i += 1;
      continue;
    }
    if (next === '[') {
      // CSI: ESC '[' params... finalByte(@–~).
      let j = i + 2;
      let params = '';
      while (j < input.length && !isCsiFinal(input[j]!)) {
        params += input[j];
        j++;
      }
      const final = input[j];
      if (final === undefined) {
        // Unterminated CSI (truncated stream) — preserve verbatim.
        tokens.push({ type: 'unknown', sequence: input.slice(i) });
        break;
      }
      if (final === 'm') {
        tokens.push({ type: 'sgr', params });
      } else {
        tokens.push({ type: 'unknown', sequence: input.slice(i, j + 1) });
      }
      i = j;
      continue;
    }
    // Any other two-byte escape (ESC X, ESC ], etc.) — unknown.
    tokens.push({ type: 'unknown', sequence: ESC + next });
    i += 1;
  }
  flush();
  return tokens;
}

/**
 * Apply one SGR parameter string to the running [color, bold] state, returning
 * the new state. Recognizes reset (0), bold on/off (1/22), default fg (39),
 * standard/bright fg (30–37 / 90–97), 256-color (`38;5;n`) and 24-bit truecolor
 * (`38;2;r;g;b`). Truncated extended-color forms and unknown codes are ignored
 * (state unchanged) rather than producing NaN colors.
 */
function applySgr(
  params: string,
  color: AnsiColor,
  bold: boolean,
  defaultFg: AnsiColor,
): [AnsiColor, boolean] {
  // Empty SGR (`ESC[m`) means reset, exactly like `ESC[0m`.
  const codes = (params === '' ? ['0'] : params.split(';')).map((p) => {
    const n = parseInt(p, 10);
    return Number.isNaN(n) ? 0 : n;
  });
  for (let k = 0; k < codes.length; k++) {
    const c = codes[k]!;
    if (c === 0) {
      color = defaultFg;
      bold = false;
    } else if (c === 1) {
      bold = true;
    } else if (c === 22) {
      bold = false;
    } else if (c === 39) {
      color = defaultFg;
    } else if (c >= 30 && c <= 37) {
      color = ANSI_COLORS[c - 30]!;
    } else if (c >= 90 && c <= 97) {
      color = ANSI_COLORS[c - 90 + 8]!;
    } else if (c === 38) {
      const mode = codes[k + 1];
      if (mode === 5 && codes[k + 2] !== undefined) {
        color = get256Color(codes[k + 2]!);
        k += 2;
      } else if (
        mode === 2 &&
        codes[k + 2] !== undefined &&
        codes[k + 3] !== undefined &&
        codes[k + 4] !== undefined
      ) {
        color = { r: codes[k + 2]!, g: codes[k + 3]!, b: codes[k + 4]! };
        k += 4;
      }
      // else: truncated extended-color — leave state untouched.
    }
  }
  return [color, bold];
}

/**
 * Parse ANSI-escaped text into lines of styled spans.
 *
 * Carriage returns are stripped, `\n` splits lines, SGR sequences drive a
 * color/bold state machine, and every non-SGR / unknown escape sequence is
 * dropped from the rendered text (never leaked verbatim). An empty source line
 * yields an empty span list so line structure is preserved. `defaultFg` sets
 * the color used for un-styled text and after a reset.
 */
export function parseAnsi(input: string, defaultFg: AnsiColor = DEFAULT_FG): AnsiSpanLine[] {
  const lines: AnsiSpanLine[] = [];
  let cur: AnsiSpanLine = [];
  let color = defaultFg;
  let bold = false;
  let buf = '';

  const flush = (): void => {
    if (buf) {
      cur.push({ text: buf, color, bold });
      buf = '';
    }
  };
  const newline = (): void => {
    flush();
    lines.push(cur);
    cur = [];
  };

  for (const token of tokenizeAnsi(input)) {
    if (token.type === 'text') {
      for (const ch of token.text) {
        if (ch === '\n') newline();
        else if (ch === '\r') continue;
        else buf += ch;
      }
    } else if (token.type === 'sgr') {
      flush();
      [color, bold] = applySgr(token.params, color, bold, defaultFg);
    } else if (token.type === 'reset') {
      flush();
      color = defaultFg;
      bold = false;
    }
    // bell / unknown: dropped from rendered text (parity with reference).
  }
  newline();
  return lines;
}

/**
 * Strip every ANSI / terminal control sequence from `input`, returning only the
 * visible text (newlines and carriage returns preserved). Built on the same
 * defensive tokenizer so unterminated/unknown sequences are removed cleanly
 * rather than partially leaking.
 */
export function stripAnsi(input: string): string {
  let out = '';
  for (const token of tokenizeAnsi(input)) {
    if (token.type === 'text') out += token.text;
  }
  return out;
}
