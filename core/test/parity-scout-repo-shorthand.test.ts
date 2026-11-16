import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { scoutTool, scoutHooks, resolveScoutRepoUrl } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/scout-shorthand', abortSignal: new AbortController().signal };
}

const orig = { ...scoutHooks };
afterEach(() => { Object.assign(scoutHooks, orig); });

// --- resolveScoutRepoUrl: accept every remote form git clone understands --------
describe('scout – resolveScoutRepoUrl', () => {
  it('expands a bare owner/repo shorthand to a GitHub HTTPS URL', () => {
    assert.equal(resolveScoutRepoUrl('owner/repo'), 'https://github.com/owner/repo');
  });

  it('passes an scp-style SSH remote through verbatim (no github mangling)', () => {
    // Without the scp carve-out this becomes https://github.com/git@github.com:owner/repo.git
    assert.equal(
      resolveScoutRepoUrl('git@github.com:owner/repo.git'),
      'git@github.com:owner/repo.git',
    );
    assert.equal(
      resolveScoutRepoUrl('git@gitlab.example.com:group/sub/project.git'),
      'git@gitlab.example.com:group/sub/project.git',
    );
  });

  it('passes full-URL and local forms through verbatim', () => {
    for (const u of [
      'https://github.com/o/r.git',
      'ssh://git@github.com/o/r.git',
      'git://host/o/r',
      'http://host:8080/o/r.git',
      '/local/abs/path',
      './rel/path',
    ]) {
      assert.equal(resolveScoutRepoUrl(u), u);
    }
  });
});

// --- E2E: scp-form source reaches git clone intact -----------------------------
describe('scout – scp-form clone (E2E through scoutTool)', () => {
  it('clones the scp SSH remote verbatim instead of a mangled github URL', async () => {
    let clonedUrl: string | undefined;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      if (args[0] === 'clone') clonedUrl = args[args.length - 2]; // [...,'clone','--depth','1',URL,localPath]
      return '' as unknown as Buffer;
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;

    const r = await scoutTool.execute({ repo: 'git@github.com:owner/repo.git' }, ctx());
    assert.equal(r.isError, undefined, `scp-form scout should succeed: ${r.output}`);
    assert.equal(clonedUrl, 'git@github.com:owner/repo.git', 'scp remote must be cloned verbatim');
    assert.doesNotMatch(String(clonedUrl), /github\.com\/git@/, 'scp remote must not be mangled into a github URL');
  });
});

// --- introspection: the repo input advertises the accepted forms ---------------
describe('scout – repo description advertises accepted forms', () => {
  it('lists scp/ssh/git/local forms, not just owner/repo', () => {
    const props = scoutTool.inputSchema.properties as Record<string, { description?: string }>;
    const desc = props.repo?.description ?? '';
    assert.match(desc, /owner\/repo/i, 'shorthand form documented');
    assert.match(desc, /git@/i, 'scp-style SSH form documented');
    assert.match(desc, /ssh:\/\/|git:\/\//i, 'ssh/git URL forms documented');
  });
});
