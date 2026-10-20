import tsParser from '@typescript-eslint/parser';
import tsPlugin from '@typescript-eslint/eslint-plugin';
import securityPlugin from 'eslint-plugin-security';

export default [
  {
    ignores: ['dist/**', 'node_modules/**', 'test/**'],
  },
  {
    linterOptions: {
      reportUnusedDisableDirectives: 'off',
    },
    files: ['src/**/*.ts'],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 2020,
        sourceType: 'module',
      },
    },
    plugins: {
      '@typescript-eslint': tsPlugin,
      security: securityPlugin,
    },
    rules: {
      ...securityPlugin.configs.recommended.rules,
      'security/detect-eval-with-expression': 'error',
      'security/detect-non-literal-fs-filename': 'off',
      'security/detect-non-literal-require': 'error',
      'security/detect-possible-timing-attacks': 'error',
      'security/detect-child-process': 'off',
      'security/detect-non-literal-regexp': 'off',
      'security/detect-unsafe-regex': 'error',
      'security/detect-buffer-noassert': 'error',
      'security/detect-no-csrf-before-method-override': 'error',
      'security/detect-object-injection': 'off',
      'no-eval': 'error',
      'no-implied-eval': 'error',
      'no-new-func': 'error',
    },
  },
];
