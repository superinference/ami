/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/webview/**/*.{js,ts,jsx,tsx}",
    "./src/**/*.{js,ts,jsx,tsx}"
  ],
  theme: {
    extend: {
      colors: {
        'vscode-bg': 'var(--vscode-editor-background)',
        'vscode-fg': 'var(--vscode-editor-foreground)',
        'vscode-sidebar-bg': 'var(--vscode-sideBar-background)',
        'vscode-sidebar-fg': 'var(--vscode-sideBar-foreground)',
        'vscode-border': 'var(--vscode-panel-border)',
        'vscode-button-bg': 'var(--vscode-button-background)',
        'vscode-button-fg': 'var(--vscode-button-foreground)',
        'vscode-button-hover': 'var(--vscode-button-hoverBackground)',
        'vscode-input-bg': 'var(--vscode-input-background)',
        'vscode-input-border': 'var(--vscode-input-border)',
        'vscode-focus-border': 'var(--vscode-focusBorder)',
        'vscode-description': 'var(--vscode-descriptionForeground)',
        'vscode-error': 'var(--vscode-errorForeground)',
        'vscode-success': 'var(--vscode-charts-green)',
        'vscode-warning': 'var(--vscode-inputValidation-warningBorder)'
      },
      fontFamily: {
        'editor': 'var(--vscode-editor-font-family)',
        'ui': 'var(--vscode-font-family)'
      },
      fontSize: {
        'editor': 'var(--vscode-editor-font-size)',
        'ui': 'var(--vscode-font-size)'
      },
      spacing: {
        'vscode-padding': '8px',
        'vscode-margin': '4px'
      },
      borderRadius: {
        'vscode': '3px'
      },
      animation: {
        'fade-in': 'fadeIn 0.2s ease-in-out',
        'slide-up': 'slideUp 0.2s ease-out',
        'pulse-soft': 'pulseSoft 2s ease-in-out infinite'
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' }
        },
        slideUp: {
          '0%': { transform: 'translateY(10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' }
        },
        pulseSoft: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.7' }
        }
      }
    }
  },
  plugins: [],
  darkMode: 'class'
} 