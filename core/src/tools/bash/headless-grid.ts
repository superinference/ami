/**
 * In-memory headless terminal grid for the bash tool.
 *
 * AMI's bash tool returns the raw captured bytes, so a `\r`-driven progress bar
 * arrives as hundreds of superimposed redraws ("ANSI soup") and a curses TUI
 * arrives as a scrollback of partial repaints. A real terminal never shows that:
 * it maintains a character grid and applies control codes to it, so the user
 * only ever sees the final rendered screen.
 *
 * This is a dependency-free emulator of the VT100/ECMA-48 subset that matters
 * for that collapse — the same mechanism Orca's `HeadlessEmulator` gets from
 * `@xterm/headless` + `SerializeAddon`, implemented in AMI terms so public core
 * stays dependency-light and fully coverable:
 *
 *   - printable text, line wrap, `\r` (carriage return), `\n`/`\v`/`\f`
 *     (line feed), `\b` (backspace), `\t` (tab to 8-col stops)
 *   - CSI cursor motion (CUU/CUD/CUF/CUB, CUP/HVP, CHA, VPA), erase-in-line
 *     (EL) and erase-in-display (ED), save/restore cursor (DECSC/DECRC),
 *     cursor-forward-tab (CHT)
 *   - SGR colour/style and other unrecognised CSI are parsed and dropped (the
 *     model wants clean text, not colour bytes)
 *   - the alternate screen buffer (DECSET/DECRST 47/1047/1049), so a full-screen
 *     TUI flattens to its final frame without leaking into scrollback
 *
 * Rows that scroll off the top are retained as plain-text scrollback so the
 * plain-text view keeps the full transcript while the rendered screen shows the
 * final frame.
 */

const ESC = '\x1b';

export type HeadlessGridOptions = {
  cols?: number;
  rows?: number;
  /** Max scrollback lines retained (older lines are dropped). */
  scrollback?: number;
};

const DEFAULT_COLS = 80;
const DEFAULT_ROWS = 24;
const DEFAULT_SCROLLBACK = 2000;
const TAB_WIDTH = 8;

type Screen = {
  grid: string[][];
  cursorX: number;
  cursorY: number;
};

export class HeadlessTerminalGrid {
  readonly cols: number;
  readonly rows: number;
  private readonly maxScrollback: number;

  private grid: string[][];
  private cursorX = 0;
  private cursorY = 0;
  private savedX = 0;
  private savedY = 0;
  private scrollbackLines: string[] = [];

  // Alternate screen state (entered by curses TUIs); the primary buffer is
  // stashed on enter and restored on exit so a TUI leaves no repaint litter.
  private altActive = false;
  private primary: Screen | null = null;

  // A control byte split across two write() calls (e.g. a chunk ending mid-CSI)
  // is carried so the next chunk completes it.
  private pending = '';

  constructor(opts: HeadlessGridOptions = {}) {
    this.cols = Math.max(1, opts.cols ?? DEFAULT_COLS);
    this.rows = Math.max(1, opts.rows ?? DEFAULT_ROWS);
    this.maxScrollback = Math.max(0, opts.scrollback ?? DEFAULT_SCROLLBACK);
    this.grid = this.blankGrid();
  }

  private blankGrid(): string[][] {
    return Array.from({ length: this.rows }, () => new Array<string>(this.cols).fill(' '));
  }

  write(data: string): void {
    const input = this.pending + data;
    this.pending = '';
    let i = 0;
    while (i < input.length) {
      const ch = input[i];
      if (ch === ESC) {
        const consumed = this.handleEscape(input, i);
        if (consumed === -1) {
          // Incomplete escape at the end of the chunk — carry it forward.
          this.pending = input.slice(i);
          return;
        }
        i = consumed;
        continue;
      }
      this.putChar(ch);
      i += 1;
    }
  }

  private putChar(ch: string): void {
    const code = ch.charCodeAt(0);
    if (code === 0x0d) {
      this.cursorX = 0;
      return;
    }
    if (code === 0x0a || code === 0x0b || code === 0x0c) {
      this.lineFeed();
      return;
    }
    if (code === 0x08) {
      this.cursorX = Math.max(0, this.cursorX - 1);
      return;
    }
    if (code === 0x09) {
      const next = Math.floor(this.cursorX / TAB_WIDTH) * TAB_WIDTH + TAB_WIDTH;
      this.cursorX = Math.min(this.cols - 1, next);
      return;
    }
    if (code < 0x20) {
      // Other C0 control bytes (bell, etc.) are not rendered.
      return;
    }
    if (this.cursorX >= this.cols) {
      // Deferred wrap: writing past the last column moves to the next line.
      this.cursorX = 0;
      this.lineFeed();
    }
    this.grid[this.cursorY][this.cursorX] = ch;
    this.cursorX += 1;
  }

  /**
   * Line feed with newline semantics (column reset). Bash output is captured
   * from a PTY (ONLCR maps `\n`→`\r\n`) or from pipes where `\n` is a true line
   * separator, so the model expects `\n` to start a fresh line at column 0.
   */
  private lineFeed(): void {
    if (this.cursorY < this.rows - 1) {
      this.cursorY += 1;
    } else {
      this.scrollUp();
    }
    this.cursorX = 0;
  }

  private scrollUp(): void {
    const evicted = this.grid.shift()!;
    if (!this.altActive) {
      this.pushScrollback(evicted.join('').replace(/\s+$/, ''));
    }
    this.grid.push(new Array<string>(this.cols).fill(' '));
  }

  private pushScrollback(line: string): void {
    if (this.maxScrollback === 0) {
      return;
    }
    this.scrollbackLines.push(line);
    if (this.scrollbackLines.length > this.maxScrollback) {
      this.scrollbackLines.shift();
    }
  }

  /**
   * Handle an escape sequence beginning at `start`. Returns the index just past
   * the consumed sequence, or -1 if the sequence is incomplete (needs more
   * bytes).
   */
  private handleEscape(input: string, start: number): number {
    const next = input[start + 1];
    if (next === undefined) {
      return -1;
    }
    if (next === '[') {
      return this.handleCsi(input, start);
    }
    if (next === ']') {
      return this.handleOsc(input, start);
    }
    if (next === '7') {
      this.saveCursor();
      return start + 2;
    }
    if (next === '8') {
      this.restoreCursor();
      return start + 2;
    }
    // Charset-designation (`ESC ( B`) and DEC `ESC # n` are 3-byte; consume the
    // trailing byte too (and wait for it if the chunk split there).
    if (next === '(' || next === ')' || next === '*' || next === '+' || next === '#') {
      return input[start + 2] === undefined ? -1 : start + 3;
    }
    // Other 2-byte escapes are consumed and ignored.
    return start + 2;
  }

  /** CSI: `ESC [ params... final`. */
  private handleCsi(input: string, start: number): number {
    let i = start + 2;
    let priv = '';
    if (input[i] === '?' || input[i] === '>' || input[i] === '=') {
      priv = input[i];
      i += 1;
    }
    const paramStart = i;
    while (i < input.length) {
      const c = input[i];
      if (c >= '\x40' && c <= '\x7e') {
        const params = input.slice(paramStart, i);
        this.applyCsi(priv, params, c);
        return i + 1;
      }
      i += 1;
    }
    return -1; // final byte not yet arrived
  }

  private parseParams(params: string): number[] {
    if (params === '') {
      return [];
    }
    return params.split(';').map((p) => (p === '' ? 0 : parseInt(p, 10) || 0));
  }

  private applyCsi(priv: string, params: string, final: string): void {
    const p = this.parseParams(params);
    const n = (idx: number, def: number): number => {
      const v = p[idx];
      return v === undefined || v === 0 ? def : v;
    };
    if (priv === '?') {
      this.applyPrivateMode(p, final);
      return;
    }
    switch (final) {
      case 'A': // CUU
        this.cursorY = Math.max(0, this.cursorY - n(0, 1));
        break;
      case 'B': // CUD
        this.cursorY = Math.min(this.rows - 1, this.cursorY + n(0, 1));
        break;
      case 'C': // CUF
        this.cursorX = Math.min(this.cols - 1, this.cursorX + n(0, 1));
        break;
      case 'D': // CUB
        this.cursorX = Math.max(0, this.cursorX - n(0, 1));
        break;
      case 'E': // CNL
        this.cursorY = Math.min(this.rows - 1, this.cursorY + n(0, 1));
        this.cursorX = 0;
        break;
      case 'F': // CPL
        this.cursorY = Math.max(0, this.cursorY - n(0, 1));
        this.cursorX = 0;
        break;
      case 'G': // CHA — column
        this.cursorX = Math.min(this.cols - 1, Math.max(0, n(0, 1) - 1));
        break;
      case 'd': // VPA — row
        this.cursorY = Math.min(this.rows - 1, Math.max(0, n(0, 1) - 1));
        break;
      case 'H': // CUP
      case 'f': // HVP
        this.cursorY = Math.min(this.rows - 1, Math.max(0, n(0, 1) - 1));
        this.cursorX = Math.min(this.cols - 1, Math.max(0, n(1, 1) - 1));
        break;
      case 'I': // CHT — forward tab stops
        this.cursorForwardTabs(n(0, 1));
        break;
      case 'J': // ED
        this.eraseDisplay(p[0] ?? 0);
        break;
      case 'K': // EL
        this.eraseLine(p[0] ?? 0);
        break;
      case 's': // save cursor
        this.saveCursor();
        break;
      case 'u': // restore cursor
        this.restoreCursor();
        break;
      default:
        // SGR ('m') and every other CSI is parsed and dropped.
        break;
    }
  }

  private applyPrivateMode(p: number[], final: string): void {
    // DECSET (h) / DECRST (l) for the alternate-screen buffer modes.
    const enable = final === 'h';
    if (final !== 'h' && final !== 'l') {
      return;
    }
    for (const mode of p) {
      if (mode === 47 || mode === 1047 || mode === 1049) {
        this.setAlternateScreen(enable);
      }
    }
  }

  private setAlternateScreen(enable: boolean): void {
    if (enable === this.altActive) {
      return;
    }
    if (enable) {
      this.primary = { grid: this.grid, cursorX: this.cursorX, cursorY: this.cursorY };
      this.grid = this.blankGrid();
      this.cursorX = 0;
      this.cursorY = 0;
      this.altActive = true;
    } else {
      this.altActive = false;
      if (this.primary) {
        this.grid = this.primary.grid;
        this.cursorX = this.primary.cursorX;
        this.cursorY = this.primary.cursorY;
        this.primary = null;
      }
    }
  }

  private cursorForwardTabs(count: number): void {
    for (let k = 0; k < count; k += 1) {
      const next = Math.floor(this.cursorX / TAB_WIDTH) * TAB_WIDTH + TAB_WIDTH;
      this.cursorX = Math.min(this.cols - 1, next);
    }
  }

  private eraseLine(mode: number): void {
    const row = this.grid[this.cursorY];
    if (mode === 1) {
      for (let x = 0; x <= this.cursorX && x < this.cols; x += 1) row[x] = ' ';
    } else if (mode === 2) {
      for (let x = 0; x < this.cols; x += 1) row[x] = ' ';
    } else {
      for (let x = this.cursorX; x < this.cols; x += 1) row[x] = ' ';
    }
  }

  private eraseDisplay(mode: number): void {
    const blankRow = (y: number): void => {
      for (let x = 0; x < this.cols; x += 1) this.grid[y][x] = ' ';
    };
    if (mode === 2 || mode === 3) {
      for (let y = 0; y < this.rows; y += 1) blankRow(y);
      return;
    }
    if (mode === 1) {
      for (let y = 0; y < this.cursorY; y += 1) blankRow(y);
      this.eraseLine(1);
      return;
    }
    // mode 0: cursor to end of screen
    this.eraseLine(0);
    for (let y = this.cursorY + 1; y < this.rows; y += 1) blankRow(y);
  }

  private saveCursor(): void {
    this.savedX = this.cursorX;
    this.savedY = this.cursorY;
  }

  private restoreCursor(): void {
    this.cursorX = this.savedX;
    this.cursorY = this.savedY;
  }

  /** OSC sequences are consumed here but scanned for metadata elsewhere. */
  private handleOsc(input: string, start: number): number {
    let i = start + 2;
    while (i < input.length) {
      const c = input[i];
      if (c === '\x07') {
        return i + 1;
      }
      if (c === ESC && input[i + 1] === '\\') {
        return i + 2;
      }
      i += 1;
    }
    return -1;
  }

  /** The current on-screen grid, trailing blank columns and rows trimmed. */
  renderScreen(): string {
    const lines = this.grid.map((row) => row.join('').replace(/\s+$/, ''));
    let end = lines.length;
    while (end > 0 && lines[end - 1] === '') {
      end -= 1;
    }
    return lines.slice(0, end).join('\n');
  }

  /** Full transcript: evicted scrollback followed by the rendered screen. */
  getPlainText(): string {
    const screen = this.renderScreen();
    if (this.scrollbackLines.length === 0) {
      return screen;
    }
    const scroll = this.scrollbackLines.join('\n');
    return screen === '' ? scroll : `${scroll}\n${screen}`;
  }

  /** Visible rows as an array (trailing whitespace trimmed per row). */
  getVisibleLines(): string[] {
    return this.grid.map((row) => row.join('').replace(/\s+$/, ''));
  }

  get isAlternateScreen(): boolean {
    return this.altActive;
  }

  getCursor(): { x: number; y: number } {
    return { x: this.cursorX, y: this.cursorY };
  }
}
