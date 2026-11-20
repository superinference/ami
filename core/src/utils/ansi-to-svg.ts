/**
 * Render ANSI-escaped terminal text to a standalone, shareable SVG string.
 *
 * AMI's counterpart to the reference's `ansiToSvg()` share affordance
 * (references/claude/claude-code-main/src/utils/ansiToSvg.ts): a terminal
 * surface (a `/stats` dashboard, a conversation transcript, any captured tool
 * output) is turned into a self-contained vector image that renders identically
 * everywhere and stays crisp at any zoom — the text-based sibling of the
 * raster {@link ../../../../common/src/ansi-to-png.ansiToPng} exporter.
 *
 * Dedup / layering (AGENTS.md No-Duplication Rule + §0d):
 *  - The SGR parser, 16-color palette and xterm-256 resolver are the canonical
 *    core primitives in {@link ./ansi-parse}; this renderer CONSUMES them so
 *    there is exactly ONE ANSI parser in the tree (same source `ansiToPng`
 *    already consumes). It adds no second parser and no second color table.
 *  - XML escaping reuses the canonical {@link ../xml-escape} helpers
 *    (`escapeXmlText` for element content, `escapeXmlAttr` for attribute values)
 *    rather than a local `escapeXml` — so every tspan/attribute is hardened the
 *    same way, and untrusted captured output (OSC titles, injected `</text>`)
 *    cannot break out of the markup or carry XML-illegal bytes.
 *  - It is a self-contained primitive with no dependency on any upper layer, so
 *    it lives in core/ (unlike `ansi-to-png`, whose only current consumer made
 *    common/ convenient). The `/export svg` slash-command handler in
 *    common/src/slash-commands.ts imports it.
 *
 * Layout heuristics mirror the reference so the output geometry matches: trailing
 * all-whitespace lines are trimmed, width is estimated from the longest line
 * (monospace char width ≈ `fontSize * 0.6`), and each line is one `<text>` with a
 * `<tspan>` per colored run so the renderer handles character spacing natively.
 */

import {
  parseAnsi,
  DEFAULT_BG,
  DEFAULT_FG,
  type AnsiColor,
  type AnsiSpanLine,
} from './ansi-parse';
import { escapeXmlAttr, escapeXmlText } from '../xml-escape';

/**
 * Tunable rendering knobs for {@link ansiToSvg}. Every field is optional; the
 * documented defaults below match the reference's `AnsiToSvgOptions` so an exported
 * image reads the same as the reference share affordance.
 */
export interface AnsiToSvgOptions {
  /** CSS font-family stack. Default `'Menlo, Monaco, monospace'`. */
  fontFamily?: string;
  /** Font size in px. Default `14`. */
  fontSize?: number;
  /** Vertical advance per line in px. Default `22`. */
  lineHeight?: number;
  /** Horizontal padding in px. Default `24`. */
  paddingX?: number;
  /** Vertical padding in px. Default `24`. */
  paddingY?: number;
  /** Background fill (any CSS color). Default the terminal-chrome dark gray. */
  backgroundColor?: string;
  /** Background rounded-corner radius in px (0 disables). Default `8`. */
  borderRadius?: number;
}

/** The default background, derived from the shared core terminal-chrome tone. */
export const DEFAULT_SVG_BACKGROUND = `rgb(${DEFAULT_BG.r}, ${DEFAULT_BG.g}, ${DEFAULT_BG.b})`;

/** Render one parsed span's color as an SVG-safe `rgb(...)` string. */
function colorToRgb(color: AnsiColor): string {
  return `rgb(${color.r}, ${color.g}, ${color.b})`;
}

/**
 * Convert ANSI-escaped terminal text to a complete SVG document string.
 *
 * The ANSI colors are preserved (it is an image, not executable terminal text),
 * so escapes are parsed — not stripped. Trailing all-whitespace lines are
 * trimmed; a transcript that trims to nothing still yields a valid (single
 * blank line) SVG rather than a degenerate `NaN`-sized document.
 */
export function ansiToSvg(ansiText: string, options: AnsiToSvgOptions = {}): string {
  const {
    fontFamily = 'Menlo, Monaco, monospace',
    fontSize = 14,
    lineHeight = 22,
    paddingX = 24,
    paddingY = 24,
    backgroundColor = DEFAULT_SVG_BACKGROUND,
    borderRadius = 8,
  } = options;

  const lines: AnsiSpanLine[] = parseAnsi(ansiText, DEFAULT_FG);

  // Trim trailing all-whitespace lines (parity with the reference). An empty
  // line is `[]` from the core parser; `[].every(...)` is `true`, so blank and
  // whitespace-only lines are both trimmed here.
  while (
    lines.length > 0 &&
    lines[lines.length - 1]!.every((span) => span.text.trim() === '')
  ) {
    lines.pop();
  }
  // A transcript that trims away entirely still produces a valid, non-degenerate
  // image (avoids `Math.max(...[]) === -Infinity` → NaN dimensions).
  if (lines.length === 0) lines.push([]);

  // Estimate width from the longest line. For monospace, a character is roughly
  // `fontSize * 0.6` wide; this drives the SVG viewport only (the renderer still
  // positions glyphs natively via <tspan>).
  const charWidthEstimate = fontSize * 0.6;
  const maxLineLength = Math.max(
    ...lines.map((spans) => spans.reduce((acc, s) => acc + s.text.length, 0)),
  );
  const width = Math.ceil(maxLineLength * charWidthEstimate + paddingX * 2);
  const height = lines.length * lineHeight + paddingY * 2;

  // Attribute-interpolated options are escaped so a hostile fontFamily /
  // backgroundColor (or the derived default) can never break out of the tag.
  const fontFamilyAttr = escapeXmlAttr(fontFamily);
  const backgroundAttr = escapeXmlAttr(backgroundColor);

  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">\n`;
  svg += `  <rect width="100%" height="100%" fill="${backgroundAttr}" rx="${borderRadius}" ry="${borderRadius}"/>\n`;
  svg += `  <style>\n`;
  svg += `    text { font-family: ${fontFamilyAttr}; font-size: ${fontSize}px; white-space: pre; }\n`;
  svg += `    .b { font-weight: bold; }\n`;
  svg += `  </style>\n`;

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
    const spans = lines[lineIndex]!;
    const y = paddingY + (lineIndex + 1) * lineHeight - (lineHeight - fontSize) / 2;

    // One <text> per line; xml:space="preserve" keeps leading/inner whitespace.
    svg += `  <text x="${paddingX}" y="${y}" xml:space="preserve">`;
    for (const span of spans) {
      if (!span.text) continue;
      const boldClass = span.bold ? ' class="b"' : '';
      svg += `<tspan fill="${colorToRgb(span.color)}"${boldClass}>${escapeXmlText(span.text)}</tspan>`;
    }
    svg += `</text>\n`;
  }

  svg += `</svg>`;
  return svg;
}
