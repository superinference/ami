/**
 * OSC-sequence scanning for the headless bash render path.
 *
 * Two escape families carry side-band terminal metadata that never appears in
 * the visible grid but is high value to the model:
 *
 *   - OSC 7 (`ESC ] 7 ; file://HOST/PATH ST`) — the shell's report of its
 *     current working directory. A command that `cd`s around (or a tool that
 *     emits OSC 7 itself) lets AMI learn the final cwd for free, instead of
 *     re-shelling a `pwd`.
 *   - OSC 0/1/2 (`ESC ] {0,1,2} ; TITLE ST`) — the window/icon title, which many
 *     TUIs and long jobs use to publish progress ("building 42/128").
 *
 * The parser is a faithful port of Orca's `osc7-uri-extraction.ts` scanner
 * (BEL- or ST-terminated, tolerant of a truncated tail), generalised to also
 * capture title OSCs in a single pass so a live chunk is scanned once.
 */

const ESC = 0x1b;
const BEL = 0x07;
const BACKSLASH = 0x5c;

/** OSC introducer `ESC ]`. */
const OSC_INTRO = '\x1b]';

export type OscScanResult = {
  /** Last OSC 7 cwd (decoded filesystem path), or null when none seen. */
  cwd: string | null;
  /** Last OSC 0/1/2 title, or null when none seen. */
  title: string | null;
};

type OscBody =
  | { kind: 'body'; body: string; nextIndex: number }
  | { kind: 'incomplete' };

/**
 * Read the OSC body starting just after the `ESC ]` introducer at `bodyStart`.
 * Terminates on BEL (`\x07`) or ST (`ESC \`). Returns `incomplete` when the
 * terminator has not arrived yet (a chunk split mid-sequence), so the caller can
 * stop rather than emit a partial value.
 */
function readOscBody(data: string, bodyStart: number): OscBody {
  for (let cursor = bodyStart; cursor < data.length; cursor += 1) {
    const code = data.charCodeAt(cursor);
    if (code === BEL) {
      return { kind: 'body', body: data.slice(bodyStart, cursor), nextIndex: cursor + 1 };
    }
    if (code === ESC) {
      if (data.charCodeAt(cursor + 1) === BACKSLASH) {
        return { kind: 'body', body: data.slice(bodyStart, cursor), nextIndex: cursor + 2 };
      }
      // A bare ESC that is not ST aborts this OSC (malformed); resume after it.
      return { kind: 'body', body: data.slice(bodyStart, cursor), nextIndex: cursor + 1 };
    }
  }
  return { kind: 'incomplete' };
}

/** Decode an OSC 7 `file://HOST/PATH` URI to a local filesystem path. */
export function osc7UriToPath(uri: string): string | null {
  if (!uri.startsWith('file://')) {
    return null;
  }
  const afterScheme = uri.slice('file://'.length);
  // `file://HOST/path` — the authority runs up to the first '/'. An empty or
  // `localhost` authority is the common local case; a non-empty remote host is
  // still reported as its absolute path.
  const slash = afterScheme.indexOf('/');
  const rawPath = slash === -1 ? '' : afterScheme.slice(slash);
  if (rawPath === '') {
    return null;
  }
  try {
    return decodeURIComponent(rawPath);
  } catch {
    // Malformed percent-encoding — surface the undecoded path rather than drop it.
    return rawPath;
  }
}

/**
 * Scan a chunk for OSC 7 cwd + OSC 0/1/2 title, returning the LAST of each.
 * Pure: no state is retained between calls (callers that need carry-over fold
 * the previous result in themselves).
 */
export function scanOscCwdTitle(data: string): OscScanResult {
  let cwd: string | null = null;
  let title: string | null = null;
  if (!data.includes(OSC_INTRO)) {
    return { cwd, title };
  }
  let searchStart = 0;
  while (searchStart < data.length) {
    const intro = data.indexOf(OSC_INTRO, searchStart);
    if (intro === -1) {
      break;
    }
    const bodyStart = intro + OSC_INTRO.length;
    const parsed = readOscBody(data, bodyStart);
    if (parsed.kind === 'incomplete') {
      break;
    }
    const semi = parsed.body.indexOf(';');
    if (semi !== -1) {
      const ps = parsed.body.slice(0, semi);
      const payload = parsed.body.slice(semi + 1);
      if (ps === '7') {
        const path = osc7UriToPath(payload);
        if (path !== null) {
          cwd = path;
        }
      } else if (ps === '0' || ps === '1' || ps === '2') {
        title = payload;
      }
    }
    searchStart = parsed.nextIndex;
  }
  return { cwd, title };
}
