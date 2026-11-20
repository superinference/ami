/**
 * Generic nested-object -> ASCII tree renderer.
 *
 * A dependency-free building block (core must not import cli/common or a theme):
 * it turns an arbitrary, possibly self-referential runtime object into an indented
 * box-drawing tree (`├ └ │`). Colour is supplied by the caller through the optional
 * {@link TreeifyOptions.colorize} seam so the renderer stays pure and testable while
 * the CLI can still paint tree-chars/keys/values with its palette.
 *
 * Safety/affordance behaviours (so any introspection surface — config dumps, MCP
 * server graphs, skill/plugin hierarchies, nested diagnostics — can stringify
 * arbitrary runtime values without crashing):
 *  - WeakSet-based circular-reference guard, emitting `[Circular]` at BOTH the
 *    node-entry and child-descent sites (never recurses into a repeated object);
 *  - arrays summarised as `[Array(n)]` rather than walked;
 *  - functions rendered `[Function]` (or filtered out entirely via `hideFunctions`);
 *  - an `(empty)` sentinel for an empty object;
 *  - a single empty/whitespace-string key renders as just the value (no key line);
 *  - blank/whitespace keys suppress the key text and the trailing colon.
 *
 * Based on the algorithm of https://github.com/notatestuser/treeify.
 */

/** A value a {@link TreeNode} key may hold. Arbitrary runtime values are accepted so
 *  the renderer can defend against them (arrays, functions, numbers, null, cycles). */
export type TreeValue =
  | TreeNode
  | string
  | number
  | boolean
  | null
  | undefined
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | ((...args: any[]) => any)
  | unknown[];

/** A nested structure to render as a tree. */
export type TreeNode = {
  [key: string]: TreeValue;
};

/** Which element a {@link TreeifyOptions.colorize} call is painting. */
export const TREE_COLOR_ROLES = ['treeChar', 'key', 'value'] as const;
export type TreeColorRole = (typeof TREE_COLOR_ROLES)[number];

export type TreeifyOptions = {
  /** Render leaf values (default true). When false only keys/structure are shown. */
  showValues?: boolean;
  /** Drop keys whose value is a function (and never render `[Function]`). Default false. */
  hideFunctions?: boolean;
  /**
   * Optional colour seam. Called for every tree-char / key / value fragment; return
   * the (possibly ANSI-wrapped) text. Omitted -> identity (plain text), which keeps
   * `core` free of any theme/ANSI dependency. The CLI passes a palette-aware impl.
   */
  colorize?: (text: string, role: TreeColorRole) => string;
};

type TreeCharacters = {
  branch: string;
  lastBranch: string;
  line: string;
  empty: string;
};

/** Box-drawing glyphs for the tree spine (matches the reference `├ └ │`). */
const DEFAULT_TREE_CHARS: TreeCharacters = {
  branch: '├', // '├'
  lastBranch: '└', // '└'
  line: '│', // '│'
  empty: ' ',
};

/**
 * Render {@link obj} as an indented box-drawing tree string.
 *
 * @param obj     the nested structure to render.
 * @param options {@link TreeifyOptions} knobs.
 * @returns the multi-line tree (or the `(empty)` sentinel for an empty object).
 */
export function treeify(obj: TreeNode, options: TreeifyOptions = {}): string {
  const { showValues = true, hideFunctions = false, colorize: colorizeOpt } = options;

  const lines: string[] = [];
  const visited = new WeakSet<object>();

  const colorize = (text: string, role: TreeColorRole): string =>
    colorizeOpt ? colorizeOpt(text, role) : text;

  function growBranch(node: TreeValue, prefix: string, depth = 0): void {
    if (typeof node === 'string') {
      lines.push(prefix + colorize(node, 'value'));
      return;
    }

    if (typeof node !== 'object' || node === null) {
      if (showValues) {
        lines.push(prefix + colorize(String(node), 'value'));
      }
      return;
    }

    // Circular-reference guard (node-entry site).
    if (visited.has(node)) {
      lines.push(prefix + colorize('[Circular]', 'value'));
      return;
    }
    visited.add(node);

    const record = node as TreeNode;
    const keys = Object.keys(record).filter((key) => {
      if (hideFunctions && typeof record[key] === 'function') return false;
      return true;
    });

    keys.forEach((key, index) => {
      const value = record[key];
      const isLastKey = index === keys.length - 1;
      // Suppress the leading prefix for the very first root child only.
      const nodePrefix = depth === 0 && index === 0 ? '' : prefix;

      const treeChar = isLastKey ? DEFAULT_TREE_CHARS.lastBranch : DEFAULT_TREE_CHARS.branch;
      const coloredTreeChar = colorize(treeChar, 'treeChar');
      const coloredKey = key.trim() === '' ? '' : colorize(key, 'key');

      let line = nodePrefix + coloredTreeChar + (coloredKey ? ' ' + coloredKey : '');
      // Blank/whitespace keys suppress the trailing colon.
      const shouldAddColon = key.trim() !== '';
      const separator = (): string => (shouldAddColon ? ': ' : line ? ' ' : '');

      if (value && typeof value === 'object' && visited.has(value)) {
        // Circular-reference guard (child-descent site).
        lines.push(line + separator() + colorize('[Circular]', 'value'));
      } else if (value && typeof value === 'object' && !Array.isArray(value)) {
        lines.push(line);
        // Non-last siblings keep their vertical spine; the last drops it.
        const continuationChar = isLastKey ? DEFAULT_TREE_CHARS.empty : DEFAULT_TREE_CHARS.line;
        const nextPrefix = nodePrefix + colorize(continuationChar, 'treeChar') + ' ';
        growBranch(value as TreeNode, nextPrefix, depth + 1);
      } else if (Array.isArray(value)) {
        lines.push(line + separator() + '[Array(' + value.length + ')]');
      } else if (showValues) {
        const valueStr = typeof value === 'function' ? '[Function]' : String(value);
        line += separator() + colorize(valueStr, 'value');
        lines.push(line);
      } else {
        lines.push(line);
      }
    });
  }

  const keys = Object.keys(obj);
  if (keys.length === 0) {
    return colorize('(empty)', 'value');
  }

  // Single empty/whitespace-string key -> render just the value (no key line).
  const firstKey = keys[0];
  if (keys.length === 1 && firstKey !== undefined && firstKey.trim() === '' && typeof obj[firstKey] === 'string') {
    const coloredTreeChar = colorize(DEFAULT_TREE_CHARS.lastBranch, 'treeChar');
    const coloredValue = colorize(obj[firstKey] as string, 'value');
    return coloredTreeChar + ' ' + coloredValue;
  }

  growBranch(obj, '', 0);
  return lines.join('\n');
}
