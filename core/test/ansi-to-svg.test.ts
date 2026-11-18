/**
 * Unit tests for core/core/src/utils/ansi-to-svg — the terminal-text → SVG
 * renderer. Covers parity with Claude Code's ansiToSvg (references/claude/
 * claude-code-main/src/utils/ansiToSvg.ts): the options schema + documented
 * defaults, per-line <text>/<tspan fill> emission with bold via a `.b` class,
 * the layout heuristics (trailing-blank trim, width estimate, baseline y), and
 * the XML-escaping / empty-input hardening AMI adds on top.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  ansiToSvg,
  DEFAULT_SVG_BACKGROUND,
  type AnsiToSvgOptions,
} from '../src/utils/ansi-to-svg';
import { DEFAULT_BG } from '../src/utils/ansi-parse';

describe('ansi-to-svg: document structure + defaults', () => {
  it('emits a valid root svg with xmlns, viewBox and a background rect', () => {
    const svg = ansiToSvg('hello');
    assert.match(svg, /^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg"/);
    assert.match(svg, /viewBox="0 0 \d+ \d+"/);
    assert.match(svg, /<rect width="100%" height="100%"/);
    assert.ok(svg.trimEnd().endsWith('</svg>'));
  });

  it('applies the documented default option values', () => {
    const svg = ansiToSvg('x');
    // fontFamily / fontSize defaults land in the <style> block.
    assert.match(svg, /font-family: Menlo, Monaco, monospace;/);
    assert.match(svg, /font-size: 14px;/);
    // Default background is derived from the shared core terminal-chrome tone.
    assert.equal(DEFAULT_SVG_BACKGROUND, `rgb(${DEFAULT_BG.r}, ${DEFAULT_BG.g}, ${DEFAULT_BG.b})`);
    assert.ok(svg.includes(`fill="${DEFAULT_SVG_BACKGROUND}"`));
    // Default borderRadius.
    assert.match(svg, /rx="8" ry="8"/);
    // Default bold class present in the style block.
    assert.match(svg, /\.b \{ font-weight: bold; \}/);
  });

  it('honors every override in the options schema', () => {
    const opts: AnsiToSvgOptions = {
      fontFamily: 'Courier New, monospace',
      fontSize: 20,
      lineHeight: 30,
      paddingX: 40,
      paddingY: 10,
      backgroundColor: 'rgb(1, 2, 3)',
      borderRadius: 0,
    };
    const svg = ansiToSvg('abc', opts);
    assert.match(svg, /font-family: Courier New, monospace;/);
    assert.match(svg, /font-size: 20px;/);
    assert.ok(svg.includes('fill="rgb(1, 2, 3)"'));
    assert.match(svg, /rx="0" ry="0"/);
    // Baseline y = paddingY + (0+1)*lineHeight - (lineHeight-fontSize)/2
    //            = 10 + 30 - (30-20)/2 = 35; x = paddingX = 40.
    assert.match(svg, /<text x="40" y="35" xml:space="preserve">/);
  });
});

describe('ansi-to-svg: color + bold spans', () => {
  it('renders one <tspan fill> per colored run', () => {
    const svg = ansiToSvg('\x1b[31mred\x1b[0mplain');
    assert.ok(svg.includes('<tspan fill="rgb(205, 49, 49)">red</tspan>'));
    // Reset returns to the default foreground (light gray 229,229,229).
    assert.ok(svg.includes('<tspan fill="rgb(229, 229, 229)">plain</tspan>'));
  });

  it('marks bold runs with the shared .b class', () => {
    const svg = ansiToSvg('\x1b[1mBOLD\x1b[0m');
    assert.match(svg, /<tspan fill="rgb\(229, 229, 229\)" class="b">BOLD<\/tspan>/);
  });

  it('resolves 24-bit true color through the shared core parser', () => {
    const svg = ansiToSvg('\x1b[38;2;10;20;30mtc');
    assert.ok(svg.includes('<tspan fill="rgb(10, 20, 30)">tc</tspan>'));
  });
});

describe('ansi-to-svg: layout heuristics', () => {
  it('emits one <text> element per rendered line', () => {
    const svg = ansiToSvg('line1\nline2\nline3');
    assert.equal((svg.match(/<text /g) ?? []).length, 3);
  });

  it('computes default baseline y per line', () => {
    const svg = ansiToSvg('a\nb');
    // defaults: paddingY=24, lineHeight=22, fontSize=14 → (lineHeight-fontSize)/2 = 4
    // line 0: 24 + 22 - 4 = 42 ; line 1: 24 + 44 - 4 = 64
    assert.match(svg, /<text x="24" y="42" xml:space="preserve">/);
    assert.match(svg, /<text x="24" y="64" xml:space="preserve">/);
  });

  it('sizes width from the longest line and height from the line count', () => {
    const svg = ansiToSvg('abcdef'); // 6 chars
    // width = ceil(6 * 14*0.6 + 24*2) = ceil(50.4 + 48) = ceil(98.4) = 99
    // height = 1 * 22 + 24*2 = 70
    assert.match(svg, /width="99" height="70"/);
  });

  it('trims trailing all-whitespace lines but keeps interior blanks', () => {
    const svg = ansiToSvg('a\n\nb\n   \n\n');
    // Interior blank between a and b is kept (3 lines: a, blank, b); the two
    // trailing blank lines are trimmed.
    assert.equal((svg.match(/<text /g) ?? []).length, 3);
    assert.match(svg, /height="114"/); // 3*22 + 48
  });

  it('never produces NaN dimensions for empty / all-blank input', () => {
    const svg = ansiToSvg('\n\n   \n');
    assert.doesNotMatch(svg, /NaN/);
    assert.match(svg, /width="48" height="70"/); // 1 synthetic blank line
  });
});

describe('ansi-to-svg: XML hardening', () => {
  it('escapes markup in the transcript text so it cannot break out of <tspan>', () => {
    const svg = ansiToSvg('a </text><script>b');
    assert.ok(!svg.includes('<script>'));
    assert.ok(svg.includes('&lt;script&gt;'));
    assert.ok(svg.includes('&lt;/text&gt;'));
  });

  it('escapes hostile attribute options (fontFamily / backgroundColor)', () => {
    const svg = ansiToSvg('x', {
      fontFamily: 'a"/><inject',
      backgroundColor: 'b"/><inject',
    });
    assert.ok(!svg.includes('<inject'));
    assert.ok(svg.includes('&quot;'));
  });
});
