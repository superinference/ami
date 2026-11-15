// Parity hardening tests for xml-escape.ts — additive superset coverage for the
// correctness/security gaps found during upstream changelog triage (2026-11):
//   1. XML-illegal control characters / noncharacters / lone surrogates are not
//      stripped before escaping, so untrusted content could emit a document a
//      conforming parser must reject (DoS / smuggling).
//   2. Attribute values do not escape TAB/LF/CR, which XML attribute-value
//      normalization folds to spaces — corrupting round-trips and masking payloads.
//   3. No round-trip unescaper existed.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  escapeXmlAttr,
  escapeXmlText,
  safeXmlKey,
  stripInvalidXmlChars,
  unescapeXml,
} from '../src/xml-escape';

describe('stripInvalidXmlChars', () => {
  it('removes a NUL byte (an XML-illegal char that cannot even be escaped)', () => {
    // RED: before the fix this helper did not exist / chars survived.
    assert.equal(stripInvalidXmlChars('a\u0000b'), 'ab');
  });

  it('removes other disallowed C0 control chars but keeps TAB/LF/CR', () => {
    assert.equal(stripInvalidXmlChars('a\u0001\u0008\u000B\u000C\u001Fb'), 'ab');
    assert.equal(stripInvalidXmlChars('a\tb\nc\rd'), 'a\tb\nc\rd');
  });

  it('removes the U+FFFE / U+FFFF noncharacters', () => {
    assert.equal(stripInvalidXmlChars('a￾￿b'), 'ab');
  });

  it('removes a lone surrogate but preserves a well-formed astral pair', () => {
    assert.equal(stripInvalidXmlChars('a\uD800b'), 'ab'); // lone high
    assert.equal(stripInvalidXmlChars('a\uDC00b'), 'ab'); // lone low
    assert.equal(stripInvalidXmlChars('a\u{1F600}b'), 'a\u{1F600}b'); // emoji kept
  });

  it('preserves DEL (legal in XML 1.0)', () => {
    assert.equal(stripInvalidXmlChars('a\u007Fb'), 'a\u007Fb');
  });
});

describe('escapeXmlText — invalid-char hardening', () => {
  it('strips XML-illegal chars in addition to escaping &<>', () => {
    // RED: before the fix the NUL survived in the output.
    assert.equal(escapeXmlText('a\u0000<b>&'), 'a&lt;b&gt;&amp;');
  });

  it('keeps existing behavior for &, < and > and leaves quotes untouched', () => {
    assert.equal(escapeXmlText(`a&b<c>d"e'f`), `a&amp;b&lt;c&gt;d"e'f`);
  });
});

describe('escapeXmlAttr — whitespace + invalid-char hardening', () => {
  it('escapes TAB/LF/CR as numeric refs so attribute-value normalization cannot fold them', () => {
    // RED: before the fix a raw \t\n\r survived and would be folded to spaces on parse.
    assert.equal(escapeXmlAttr('a\tb\nc\rd'), 'a&#9;b&#10;c&#13;d');
  });

  it('strips XML-illegal chars in attribute context', () => {
    assert.equal(escapeXmlAttr('a\u0000b￿c'), 'abc');
  });

  it('still escapes all five attribute-dangerous characters', () => {
    assert.equal(escapeXmlAttr(`a&b<c>d"e'f`), 'a&amp;b&lt;c&gt;d&quot;e&apos;f');
  });
});

describe('unescapeXml — round trip', () => {
  it('decodes the five predefined named entities', () => {
    assert.equal(unescapeXml('a&amp;b&lt;c&gt;d&quot;e&apos;f'), `a&b<c>d"e'f`);
  });

  it('round-trips through escapeXmlAttr for the full danger set', () => {
    const s = `a&b<c>d"e'f`;
    assert.equal(unescapeXml(escapeXmlAttr(s)), s);
  });

  it('round-trips the numeric refs that escapeXmlAttr emits for whitespace', () => {
    const s = 'a\tb\nc\rd';
    assert.equal(unescapeXml(escapeXmlAttr(s)), s);
  });

  it('decodes decimal and hexadecimal numeric character references', () => {
    assert.equal(unescapeXml('&#60;&#x3C;&#X3e;'), '<<>');
    assert.equal(unescapeXml('&#128512;'), '\u{1F600}');
  });

  it('does not over-decode a double-escaped ampersand (single left-to-right pass)', () => {
    // escapeXmlAttr('&lt;') === '&amp;lt;'; unescape must give back the literal text '&lt;'.
    assert.equal(unescapeXml(escapeXmlAttr('&lt;')), '&lt;');
    assert.equal(unescapeXml('&amp;lt;'), '&lt;');
  });

  it('leaves unknown / malformed / out-of-range entities verbatim', () => {
    assert.equal(unescapeXml('&foo;'), '&foo;');
    assert.equal(unescapeXml('&amp'), '&amp'); // unterminated
    assert.equal(unescapeXml('&#x110000;'), '&#x110000;'); // beyond U+10FFFF
    assert.equal(unescapeXml('a & b'), 'a & b'); // bare ampersand
  });

  it('coerces non-strings', () => {
    assert.equal(unescapeXml(42 as unknown as string), '42');
  });
});

describe('regression — existing public API unchanged', () => {
  it('safeXmlKey still guards names', () => {
    assert.equal(safeXmlKey('server.name_1'), 'server.name_1');
    assert.equal(safeXmlKey('bad key"/>', 'fallback'), 'fallback');
  });
});
