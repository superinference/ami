import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  WindowsToWSLConverter,
  checkWSLDistroMatch,
  manualWindowsToWSL,
  createWSLConverterFromEnv,
  WSL_UNC_PATH_RE,
  execHooks,
  type IDEPathConverter,
} from '../src/utils/ide-path-conversion';

// A stand-in for execFileSync that records calls and returns a scripted value or
// throws, so the wslpath-success / wslpath-missing arms run deterministically
// without a real `wslpath` binary (absent on Linux CI).
function mockWslpath(impl: (cmd: string, args: string[]) => string | never) {
  return mock.method(
    execHooks,
    'execFileSync',
    ((cmd: string, args: string[]) => impl(cmd, args)) as typeof execHooks.execFileSync,
  );
}

describe('ide-path-conversion', () => {
  afterEach(() => mock.restoreAll());

  describe('checkWSLDistroMatch', () => {
    it('returns true for a same-distro wsl.localhost UNC path', () => {
      assert.equal(
        checkWSLDistroMatch('\\\\wsl.localhost\\Ubuntu\\home\\me', 'Ubuntu'),
        true,
      );
    });

    it('returns true for a same-distro legacy wsl$ UNC path', () => {
      assert.equal(checkWSLDistroMatch('\\\\wsl$\\Ubuntu\\home\\me', 'Ubuntu'), true);
    });

    it('returns false for a different-distro UNC path', () => {
      assert.equal(
        checkWSLDistroMatch('\\\\wsl.localhost\\Debian\\home\\me', 'Ubuntu'),
        false,
      );
    });

    it('returns false for a different-distro legacy wsl$ UNC path', () => {
      assert.equal(checkWSLDistroMatch('\\\\wsl$\\Alpine\\srv', 'Ubuntu'), false);
    });

    it('returns true for a non-UNC Windows drive path (no distro to mismatch)', () => {
      assert.equal(checkWSLDistroMatch('C:\\Users\\me\\project', 'Ubuntu'), true);
    });

    it('returns true for a plain POSIX path', () => {
      assert.equal(checkWSLDistroMatch('/mnt/c/Users/me', 'Ubuntu'), true);
    });
  });

  describe('WSL_UNC_PATH_RE', () => {
    it('captures the distro name and remainder', () => {
      const m = '\\\\wsl.localhost\\Ubuntu\\home\\me'.match(WSL_UNC_PATH_RE);
      assert.ok(m);
      assert.equal(m![1], 'Ubuntu');
      assert.equal(m![2], '\\home\\me');
    });

    it('does not match a non-UNC path', () => {
      assert.equal('C:\\x'.match(WSL_UNC_PATH_RE), null);
    });
  });

  describe('manualWindowsToWSL', () => {
    it('maps a drive path to /mnt/<lower>', () => {
      assert.equal(manualWindowsToWSL('C:\\Users\\me'), '/mnt/c/Users/me');
    });

    it('lowercases the drive letter regardless of case', () => {
      assert.equal(manualWindowsToWSL('D:\\A\\B'), '/mnt/d/A/B');
    });

    it('flips slashes for a path without a drive letter', () => {
      assert.equal(manualWindowsToWSL('relative\\sub\\file'), 'relative/sub/file');
    });
  });

  describe('WindowsToWSLConverter.toLocalPath', () => {
    it('returns the empty string unchanged without spawning wslpath', () => {
      const exec = mockWslpath(() => {
        throw new Error('should not be called');
      });
      const conv: IDEPathConverter = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toLocalPath(''), '');
      assert.equal(exec.mock.callCount(), 0);
    });

    it('uses wslpath -u and trims the result on success', () => {
      const exec = mockWslpath((cmd, args) => {
        assert.equal(cmd, 'wslpath');
        assert.deepEqual(args, ['-u', 'C:\\Users\\me\\p']);
        return '/mnt/c/Users/me/p\n';
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toLocalPath('C:\\Users\\me\\p'), '/mnt/c/Users/me/p');
      assert.equal(exec.mock.callCount(), 1);
    });

    it('falls back to the manual mapping when wslpath throws', () => {
      mockWslpath(() => {
        throw new Error('wslpath: not found');
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toLocalPath('C:\\Users\\me'), '/mnt/c/Users/me');
    });

    it('returns a different-distro UNC path unchanged without spawning wslpath', () => {
      const exec = mockWslpath(() => {
        throw new Error('should not be called');
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      const foreign = '\\\\wsl.localhost\\Debian\\home\\me';
      assert.equal(conv.toLocalPath(foreign), foreign);
      assert.equal(exec.mock.callCount(), 0);
    });

    it('converts a same-distro UNC path via wslpath', () => {
      const exec = mockWslpath(() => '/home/me/x');
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toLocalPath('\\\\wsl.localhost\\Ubuntu\\home\\me\\x'), '/home/me/x');
      assert.equal(exec.mock.callCount(), 1);
    });

    it('skips the distro check entirely when no distro name is configured', () => {
      const exec = mockWslpath(() => '/converted');
      const conv = new WindowsToWSLConverter(undefined);
      // Even a foreign-looking UNC path is passed to wslpath when distro is unset.
      assert.equal(conv.toLocalPath('\\\\wsl.localhost\\Debian\\x'), '/converted');
      assert.equal(exec.mock.callCount(), 1);
    });
  });

  describe('WindowsToWSLConverter.toIDEPath', () => {
    it('returns the empty string unchanged without spawning wslpath', () => {
      const exec = mockWslpath(() => {
        throw new Error('should not be called');
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toIDEPath(''), '');
      assert.equal(exec.mock.callCount(), 0);
    });

    it('uses wslpath -w and trims the result on success', () => {
      const exec = mockWslpath((cmd, args) => {
        assert.equal(cmd, 'wslpath');
        assert.deepEqual(args, ['-w', '/mnt/c/Users/me']);
        return 'C:\\Users\\me\r\n';
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toIDEPath('/mnt/c/Users/me'), 'C:\\Users\\me');
      assert.equal(exec.mock.callCount(), 1);
    });

    it('returns the original path unchanged when wslpath throws', () => {
      mockWslpath(() => {
        throw new Error('wslpath: not found');
      });
      const conv = new WindowsToWSLConverter('Ubuntu');
      assert.equal(conv.toIDEPath('/mnt/c/Users/me'), '/mnt/c/Users/me');
    });
  });

  describe('round-trip', () => {
    it('toLocalPath then toIDEPath restores the Windows path via wslpath', () => {
      mockWslpath((_cmd, args) =>
        args[0] === '-u' ? '/mnt/c/proj' : 'C:\\proj',
      );
      const conv = new WindowsToWSLConverter('Ubuntu');
      const local = conv.toLocalPath('C:\\proj');
      assert.equal(local, '/mnt/c/proj');
      assert.equal(conv.toIDEPath(local), 'C:\\proj');
    });
  });

  describe('createWSLConverterFromEnv', () => {
    it('reads the distro name from WSL_DISTRO_NAME', () => {
      const exec = mockWslpath(() => {
        throw new Error('should not be called');
      });
      const conv = createWSLConverterFromEnv({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv);
      // Foreign distro → early return, no spawn, proving the distro name was wired in.
      const foreign = '\\\\wsl$\\Debian\\x';
      assert.equal(conv.toLocalPath(foreign), foreign);
      assert.equal(exec.mock.callCount(), 0);
    });

    it('treats an empty WSL_DISTRO_NAME as no distro (no distro check)', () => {
      const exec = mockWslpath(() => '/converted');
      const conv = createWSLConverterFromEnv({ WSL_DISTRO_NAME: '' } as NodeJS.ProcessEnv);
      assert.equal(conv.toLocalPath('\\\\wsl$\\Debian\\x'), '/converted');
      assert.equal(exec.mock.callCount(), 1);
    });

    it('defaults to process.env when no env is given', () => {
      // Just prove it constructs a usable converter against the ambient env.
      const conv = createWSLConverterFromEnv();
      assert.ok(conv instanceof WindowsToWSLConverter);
    });
  });
});
