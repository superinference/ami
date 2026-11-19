import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { HeadlessTerminalGrid } from '../src/tools/bash/headless-grid';

describe('HeadlessTerminalGrid', () => {
  it('collapses a \\r-driven progress bar to its final state', () => {
    const g = new HeadlessTerminalGrid({ cols: 40, rows: 4 });
    // Each frame overwrites the same line via a carriage return.
    g.write('Progress:   0%\r');
    g.write('Progress:  50%\r');
    g.write('Progress: 100%');
    assert.equal(g.renderScreen(), 'Progress: 100%');
  });

  it('applies a partial \\r overwrite (shorter frame leaves the tail)', () => {
    const g = new HeadlessTerminalGrid({ cols: 40, rows: 2 });
    g.write('abcdefgh\r');
    g.write('XYZ');
    // Only the first 3 columns are overwritten.
    assert.equal(g.renderScreen(), 'XYZdefgh');
  });

  it('handles newlines and multi-line layout', () => {
    const g = new HeadlessTerminalGrid({ cols: 20, rows: 5 });
    g.write('line one\nline two\nline three');
    assert.equal(g.renderScreen(), 'line one\nline two\nline three');
  });

  it('applies backspace by moving the cursor left', () => {
    const g = new HeadlessTerminalGrid({ cols: 20, rows: 2 });
    g.write('abc\b\bX');
    assert.equal(g.renderScreen(), 'aXc');
  });

  it('expands tabs to 8-column stops', () => {
    const g = new HeadlessTerminalGrid({ cols: 40, rows: 2 });
    g.write('a\tb');
    assert.equal(g.renderScreen(), 'a       b');
  });

  it('wraps text past the last column onto the next row', () => {
    const g = new HeadlessTerminalGrid({ cols: 4, rows: 3 });
    g.write('abcdef');
    assert.equal(g.renderScreen(), 'abcd\nef');
  });

  it('scrolls old rows into plain-text scrollback while screen shows the tail', () => {
    const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
    g.write('one\ntwo\nthree');
    // rows=2 so 'one' scrolled off into scrollback.
    assert.equal(g.renderScreen(), 'two\nthree');
    assert.equal(g.getPlainText(), 'one\ntwo\nthree');
  });

  it('drops scrollback beyond the retention limit', () => {
    const g = new HeadlessTerminalGrid({ cols: 10, rows: 1, scrollback: 2 });
    g.write('a\nb\nc\nd\ne');
    // rows=1, scrollback=2 -> only the last 2 evicted lines retained + screen.
    assert.equal(g.getPlainText(), 'c\nd\ne');
  });

  it('retains no scrollback when the limit is zero', () => {
    const g = new HeadlessTerminalGrid({ cols: 10, rows: 1, scrollback: 0 });
    g.write('a\nb\nc');
    assert.equal(g.getPlainText(), 'c');
  });

  describe('CSI cursor motion', () => {
    it('CUP positions the cursor (row;col, 1-based)', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[2;3Hx');
      assert.equal(g.getVisibleLines()[1], '  x');
    });

    it('CUU/CUD/CUF/CUB move relative', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 5 });
      g.write('\x1b[3;3H'); // row 3 col 3
      g.write('\x1b[1A'); // up one
      g.write('\x1b[2C'); // right two
      g.write('Z');
      assert.equal(g.getVisibleLines()[1], '    Z');
      // move back: B (down), D (left)
      g.write('\x1b[1B\x1b[10DQ');
      assert.equal(g.getVisibleLines()[2], 'Q');
    });

    it('CNL / CPL move to column 0 of another line', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 4 });
      g.write('xx\x1b[1EA'); // CNL -> next line col0
      assert.equal(g.getVisibleLines()[1], 'A');
      g.write('\x1b[1FB'); // CPL -> prev line col0 (overwrites first char of 'xx')
      assert.equal(g.getVisibleLines()[0], 'Bx');
    });

    it('CHA sets absolute column and VPA sets absolute row', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('abcd\x1b[1GX'); // CHA col1
      assert.equal(g.getVisibleLines()[0], 'Xbcd');
      g.write('\x1b[3dY'); // VPA row3
      assert.equal(g.getVisibleLines()[2], ' Y');
    });

    it('HVP (f) positions like CUP', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[2;2fk');
      assert.equal(g.getVisibleLines()[1], ' k');
    });

    it('CHT advances forward tab stops', () => {
      const g = new HeadlessTerminalGrid({ cols: 40, rows: 2 });
      g.write('a\x1b[2Ib');
      assert.equal(g.renderScreen(), 'a               b');
    });

    it('clamps motion at the grid edges', () => {
      const g = new HeadlessTerminalGrid({ cols: 5, rows: 3 });
      g.write('\x1b[99A\x1b[99D'); // way up+left -> clamp to 0,0
      g.write('T');
      assert.equal(g.getVisibleLines()[0], 'T');
      g.write('\x1b[99B\x1b[99Cb'); // way down+right -> clamp
      assert.equal(g.getVisibleLines()[2], '    b');
    });
  });

  describe('erase', () => {
    it('EL 0 erases to end of line, EL 1 to start, EL 2 whole line', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('abcdefg\x1b[4G\x1b[0K'); // cursor col4, erase to end
      assert.equal(g.getVisibleLines()[0], 'abc');
      g.write('\x1b[2;1Habcdefg\x1b[2;4H\x1b[1K'); // erase to start (incl cursor)
      assert.equal(g.getVisibleLines()[1], '    efg');
      g.write('\x1b[3;1Habc\x1b[2K'); // whole line
      assert.equal(g.getVisibleLines()[2], '');
    });

    it('ED 0 clears to end of screen, ED 1 to start, ED 2 whole screen', () => {
      const g = new HeadlessTerminalGrid({ cols: 6, rows: 3 });
      g.write('aaa\nbbb\nccc');
      g.write('\x1b[2;2H\x1b[0J'); // from row2col2 to end
      assert.equal(g.getVisibleLines().join('|'), 'aaa|b|');
      g.write('\x1b[2;1Hbbb\x1b[3;1Hccc\x1b[2;2H\x1b[1J'); // erase start-of-screen..cursor(incl)
      assert.equal(g.getVisibleLines().join('|'), '|  b|ccc');
      g.write('\x1b[2J\x1b[1;1HZ'); // ED2 does not move the cursor; home it explicitly
      assert.equal(g.getVisibleLines().join('|'), 'Z||');
    });

    it('ED 3 clears the whole screen like ED 2', () => {
      const g = new HeadlessTerminalGrid({ cols: 6, rows: 2 });
      g.write('aa\nbb\x1b[3J');
      assert.equal(g.renderScreen(), '');
    });
  });

  describe('save/restore cursor', () => {
    it('DECSC/DECRC (ESC 7 / ESC 8) round-trips the cursor', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[2;3H\x1b7'); // move + save
      g.write('\x1b[1;1Htop'); // move away + write
      g.write('\x1b8X'); // restore + write
      assert.equal(g.getVisibleLines()[0], 'top');
      assert.equal(g.getVisibleLines()[1], '  X');
    });

    it('CSI s / CSI u also save and restore', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[3;2H\x1bs?'); // note: use CSI s
      g.write('\x1b[1;1H\x1b[s'); // save col0
      g.write('\x1b[2;5Hmid');
      g.write('\x1b[uY'); // restore to col0 row0
      assert.equal(g.getVisibleLines()[0], 'Y');
    });
  });

  describe('alternate screen', () => {
    it('flattens a curses TUI to its final frame without scrollback litter', () => {
      const g = new HeadlessTerminalGrid({ cols: 12, rows: 3 });
      g.write('shell line before\n');
      g.write('\x1b[?1049h'); // enter alt screen
      assert.equal(g.isAlternateScreen, true);
      g.write('\x1b[2J\x1b[1;1HTUI FRAME 1');
      g.write('\x1b[2J\x1b[1;1HTUI FRAME 2'); // repaint
      g.write('\x1b[?1049l'); // exit alt screen
      assert.equal(g.isAlternateScreen, false);
      // Primary buffer restored; TUI frames did not pollute it.
      assert.match(g.renderScreen(), /shell line/);
      assert.doesNotMatch(g.getPlainText(), /TUI FRAME/);
    });

    it('47 and 1047 also toggle the alternate screen; redundant toggles are no-ops', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b[?47h');
      assert.equal(g.isAlternateScreen, true);
      g.write('\x1b[?47h'); // redundant enable
      assert.equal(g.isAlternateScreen, true);
      g.write('\x1b[?47l');
      assert.equal(g.isAlternateScreen, false);
      g.write('\x1b[?47l'); // redundant disable
      assert.equal(g.isAlternateScreen, false);
    });
  });

  describe('robustness', () => {
    it('drops SGR colour codes from the rendered text', () => {
      const g = new HeadlessTerminalGrid({ cols: 20, rows: 2 });
      g.write('\x1b[31mred\x1b[0m done');
      assert.equal(g.renderScreen(), 'red done');
    });

    it('ignores non-rendered C0 controls (bell)', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('a\x07b');
      assert.equal(g.renderScreen(), 'ab');
    });

    it('carries a CSI split across two write() calls', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[2;'); // incomplete CSI
      g.write('3Hx'); // completes it
      assert.equal(g.getVisibleLines()[1], '  x');
    });

    it('carries a lone trailing ESC across writes', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('ab\x1b');
      g.write('[1GX');
      assert.equal(g.getVisibleLines()[0], 'Xb');
    });

    it('consumes an OSC sequence without rendering it', () => {
      const g = new HeadlessTerminalGrid({ cols: 20, rows: 2 });
      g.write('\x1b]0;my title\x07hi');
      assert.equal(g.renderScreen(), 'hi');
    });

    it('consumes an OSC terminated by ST (ESC backslash)', () => {
      const g = new HeadlessTerminalGrid({ cols: 20, rows: 2 });
      g.write('\x1b]7;file:///tmp\x1b\\done');
      assert.equal(g.renderScreen(), 'done');
    });

    it('carries an incomplete OSC across writes', () => {
      const g = new HeadlessTerminalGrid({ cols: 20, rows: 2 });
      g.write('x\x1b]0;parti'); // OSC not terminated
      g.write('al\x07y');
      assert.equal(g.renderScreen(), 'xy');
    });

    it('ignores a private-mode sequence that is not an alt-screen mode', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b[?25lhi\x1b[?25h'); // hide/show cursor
      assert.equal(g.renderScreen(), 'hi');
      assert.equal(g.isAlternateScreen, false);
    });

    it('ignores a private-mode sequence with a non-h/l final', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b[?1049$pX'); // DECRQM-style query, not h/l
      assert.equal(g.renderScreen(), 'X');
      assert.equal(g.isAlternateScreen, false);
    });

    it('ignores an unknown 2-byte escape', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b(Bhi'); // select charset
      assert.equal(g.renderScreen(), 'hi');
    });

    it('ignores a DEC line-size escape (ESC # n)', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b#6wide'); // DECDWL
      assert.equal(g.renderScreen(), 'wide');
    });

    it('carries a 3-byte charset escape split across writes', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('a\x1b('); // charset designator missing its final byte
      g.write('Bb'); // 'B' completes it, 'b' renders
      assert.equal(g.renderScreen(), 'ab');
    });

    it('ignores a CSI with a > or = private prefix (device queries)', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 2 });
      g.write('\x1b[>0c\x1b[=1;2chi'); // secondary/tertiary DA — not rendered
      assert.equal(g.renderScreen(), 'hi');
    });

    it('reports the cursor position', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 3 });
      g.write('\x1b[2;4H');
      assert.deepEqual(g.getCursor(), { x: 3, y: 1 });
    });

    it('vertical tab and form feed behave as line feeds (column reset)', () => {
      const g = new HeadlessTerminalGrid({ cols: 10, rows: 4 });
      g.write('a\x0bb\x0cc');
      assert.equal(g.getVisibleLines()[0], 'a');
      assert.equal(g.getVisibleLines()[1], 'b');
      assert.equal(g.getVisibleLines()[2], 'c');
    });

    it('clamps grid dimensions to at least 1x1', () => {
      const g = new HeadlessTerminalGrid({ cols: 0, rows: 0 });
      assert.equal(g.cols, 1);
      assert.equal(g.rows, 1);
      g.write('abc'); // wraps + scrolls each char
      assert.equal(g.getPlainText(), 'a\nb\nc');
    });
  });
});
