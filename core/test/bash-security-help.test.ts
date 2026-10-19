import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectUnsafeHelpCommand } from '../src/tools/bash-security';

describe('detectUnsafeHelpCommand', () => {
  // --- Safe commands (should return false) ---
  it('allows plain man <page>', () => {
    assert.equal(detectUnsafeHelpCommand('man ls'), false);
  });

  it('allows man with section number', () => {
    assert.equal(detectUnsafeHelpCommand('man 3 printf'), false);
  });

  it('allows plain git help <topic>', () => {
    assert.equal(detectUnsafeHelpCommand('git help commit'), false);
  });

  it('allows git help with no arguments', () => {
    assert.equal(detectUnsafeHelpCommand('git help'), false);
  });

  it('allows bash builtin help', () => {
    assert.equal(detectUnsafeHelpCommand('help echo'), false);
  });

  it('allows man with -k (apropos search)', () => {
    assert.equal(detectUnsafeHelpCommand('man -k network'), false);
  });

  it('allows man with -f (whatis)', () => {
    assert.equal(detectUnsafeHelpCommand('man -f printf'), false);
  });

  it('allows info without dangerous flags', () => {
    assert.equal(detectUnsafeHelpCommand('info coreutils'), false);
  });

  it('allows info --subnodes without pipe (no exfil)', () => {
    assert.equal(detectUnsafeHelpCommand('info --subnodes coreutils'), false);
  });

  // --- Unsafe commands (should return true) ---

  // man -l: process local file
  it('blocks man -l <file>', () => {
    assert.equal(detectUnsafeHelpCommand('man -l /tmp/evil.man'), true);
  });

  it('blocks man --local-file <file>', () => {
    assert.equal(detectUnsafeHelpCommand('man --local-file /tmp/evil.man'), true);
  });

  it('blocks man -l combined with other flags', () => {
    assert.equal(detectUnsafeHelpCommand('man -al /tmp/evil.man'), true);
  });

  // man -H: open in browser
  it('blocks man -H <page>', () => {
    assert.equal(detectUnsafeHelpCommand('man -H commit'), true);
  });

  it('blocks man --html', () => {
    assert.equal(detectUnsafeHelpCommand('man --html bash'), true);
  });

  // man --pager: arbitrary command execution
  it('blocks man --pager=<cmd>', () => {
    assert.equal(detectUnsafeHelpCommand('man --pager="rm -rf /" ls'), true);
  });

  it('blocks man --pager= with spaces', () => {
    assert.equal(detectUnsafeHelpCommand('man --pager = /bin/sh ls'), true);
  });

  it('blocks man -P <cmd>', () => {
    assert.equal(detectUnsafeHelpCommand('man -P less ls'), true);
  });

  // git help -w: open in browser
  it('blocks git help -w <topic>', () => {
    assert.equal(detectUnsafeHelpCommand('git help -w commit'), true);
  });

  it('blocks git help --web', () => {
    assert.equal(detectUnsafeHelpCommand('git help --web commit'), true);
  });

  it('blocks git help -w combined with other flags', () => {
    assert.equal(detectUnsafeHelpCommand('git help -aw commit'), true);
  });

  // info --subnodes piped
  it('blocks info --subnodes piped to command', () => {
    assert.equal(detectUnsafeHelpCommand('info --subnodes coreutils | grep password'), true);
  });

  // --- Edge cases ---
  it('does not false-positive on "manual" or "manage"', () => {
    assert.equal(detectUnsafeHelpCommand('echo manual'), false);
  });

  it('does not false-positive on "information"', () => {
    assert.equal(detectUnsafeHelpCommand('echo information'), false);
  });

  it('handles invisible unicode in command', () => {
    // Zero-width space before -l
    assert.equal(detectUnsafeHelpCommand('man ​-l /tmp/evil.man'), true);
  });

  it('handles empty command', () => {
    assert.equal(detectUnsafeHelpCommand(''), false);
  });
});
