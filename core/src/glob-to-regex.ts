// Glob-pattern -> RegExp source translation, extracted to a dependency-free leaf
// module so both permissions.ts and tools/read-ignore.ts can consume it WITHOUT a
// read-ignore -> permissions import cycle. Pure string->string; no other imports.

/**
 * Compile a glob-like pattern to a regex source, honouring backslash escapes so
 * a pattern can match a LITERAL asterisk or backslash (the reference's matchWildcardPattern
 * escape handling, shellRuleMatching.ts): an UNescaped `*` is the wildcard
 * (`.*`), `\*` is a literal `*`, and `\\` is a literal `\`. A lone backslash
 * before any other character stays a literal backslash (back-compat). Every
 * other regex metacharacter is escaped.
 */
export function globToRegexSource(pattern: string): string {
  let out = '';
  let unescapedStars = 0;
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i]!;
    if (ch === '\\') {
      const next = pattern[i + 1];
      if (next === '*') { out += '\\*'; i++; continue; }   // escaped → literal asterisk
      if (next === '\\') { out += '\\\\'; i++; continue; } // escaped → literal backslash
      out += '\\\\'; continue;                             // lone backslash → literal
    }
    if (ch === '*') { out += '.*'; unescapedStars++; continue; } // wildcard
    if (/[.+^${}()|[\]?]/.test(ch)) { out += '\\' + ch; continue; } // escape other metachars
    out += ch;
  }
  // Trailing ` *` prefix semantics: when the ONLY unescaped wildcard is a sole
  // trailing ` *` (space + wildcard), treat it like a legacy `:*` prefix rule —
  // `git *` matches the bare `git` command AND `git add`, aligning wildcard rules
  // with prefix (`git:*`) semantics — by making the trailing segment optional.
  // Multi-wildcard patterns (e.g. `* run *`) are deliberately excluded so they
  // keep their strict meaning. Mirrors the reference's shellRuleMatching trailing-
  // wildcard-optional rewrite.
  if (unescapedStars === 1 && pattern.endsWith(' *') && out.endsWith(' .*')) {
    out = out.slice(0, -3) + '( .*)?';
  }
  return out;
}
