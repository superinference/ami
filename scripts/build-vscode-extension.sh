#!/bin/bash
# Copyright (C) 2025 SuperInference contributors
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see https://www.gnu.org/licenses/.

# SuperInference Extension Auto-Install Script
# This script builds and installs the extension automatically

set -e

# Get script directory and navigate to vscode folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VSCODE_DIR="$(cd "$SCRIPT_DIR/../vscode" && pwd)"

# Change to vscode directory
cd "$VSCODE_DIR"

# Check Node.js version
echo "🔍 Checking Node.js version..."
NODE_VERSION=$(node --version | sed 's/v//')
echo "✅ Node.js version $NODE_VERSION detected"

# Simple version check - just warn if too old but don't fail
MAJOR_VERSION=$(echo $NODE_VERSION | cut -d. -f1)
if [ "$MAJOR_VERSION" -lt 18 ]; then
    echo "⚠️  Warning: Node.js version $NODE_VERSION is older than recommended (18.x)"
    echo "📋 Some features may not work properly"
    echo "💡 Consider upgrading to Node.js 18+ for best compatibility"
else
    echo "✅ Node.js version is compatible"
fi

echo "📦 Installing Node.js dependencies..."
npm install

echo "🧹 Cleaning previous builds..."
rm -rf dist/ node_modules/.cache/ || true

echo "📋 Checking for regenerated fonts..."
# Check multiple possible locations for regenerated fonts
FONT_SOURCE=""

# Check root vscode directory
if [ -f "$VSCODE_DIR/superinference.ttf" ] && [ -f "$VSCODE_DIR/superinference.woff" ] && [ -f "$VSCODE_DIR/superinference.eot" ] && [ -f "$VSCODE_DIR/superinference.svg" ]; then
    FONT_SOURCE="$VSCODE_DIR"
    echo "📦 Found regenerated fonts in root directory"
# Check parent directory (ami/)
elif [ -f "$SCRIPT_DIR/../superinference.ttf" ] && [ -f "$SCRIPT_DIR/../superinference.woff" ]; then
    FONT_SOURCE="$SCRIPT_DIR/.."
    echo "📦 Found regenerated fonts in parent directory"
# Check if fonts exist but are old
elif [ -f "$VSCODE_DIR/src/webview/codicons/superinference.ttf" ]; then
    OLD_SIZE=$(stat -c%s "$VSCODE_DIR/src/webview/codicons/superinference.ttf" 2>/dev/null || echo "0")
    OLD_DATE=$(stat -c%y "$VSCODE_DIR/src/webview/codicons/superinference.ttf" 2>/dev/null | cut -d' ' -f1)
    echo "📄 Current fonts in src/webview/codicons/: $OLD_SIZE bytes, modified: $OLD_DATE"
    
    # If font is very small (< 2KB), it's likely old/corrupted
    if [ "$OLD_SIZE" -lt 2000 ]; then
        echo "⚠️  Warning: Fonts appear to be old or corrupted (size: $OLD_SIZE bytes)"
        echo "💡 Please regenerate fonts and place them in: $VSCODE_DIR/src/webview/codicons/"
    fi
fi

# Copy fonts if found in alternative location
if [ -n "$FONT_SOURCE" ]; then
    echo "📦 Copying regenerated fonts from $FONT_SOURCE to src/webview/codicons/..."
    cp -v "$FONT_SOURCE/superinference.ttf" "$VSCODE_DIR/src/webview/codicons/" 2>/dev/null || true
    cp -v "$FONT_SOURCE/superinference.woff" "$VSCODE_DIR/src/webview/codicons/" 2>/dev/null || true
    cp -v "$FONT_SOURCE/superinference.eot" "$VSCODE_DIR/src/webview/codicons/" 2>/dev/null || true
    cp -v "$FONT_SOURCE/superinference.svg" "$VSCODE_DIR/src/webview/codicons/" 2>/dev/null || true
    echo "✅ Fonts copied to src/webview/codicons/"
fi

echo "📋 Verifying font files exist..."
if [ -f "$VSCODE_DIR/src/webview/codicons/superinference.ttf" ]; then
    FONT_SIZE=$(stat -c%s "$VSCODE_DIR/src/webview/codicons/superinference.ttf" 2>/dev/null || echo "0")
    FONT_DATE=$(stat -c%y "$VSCODE_DIR/src/webview/codicons/superinference.ttf" 2>/dev/null | cut -d' ' -f1)
    echo "📄 Current font: $FONT_SIZE bytes, modified: $FONT_DATE"
    
    # Warn if font seems too small (likely old/corrupted)
    if [ "$FONT_SIZE" -lt 2000 ]; then
        echo "⚠️  WARNING: Font file is very small ($FONT_SIZE bytes) - may be corrupted or incomplete"
        echo "💡 Please regenerate fonts and place them in: $VSCODE_DIR/src/webview/codicons/"
    fi
else
    echo "⚠️  Warning: superinference.ttf not found in src/webview/codicons/"
    echo "💡 Make sure you've regenerated fonts and placed them in src/webview/codicons/"
fi

echo "🔨 Building SuperInference extension..."
npm run build-vsix

echo "✅ Verifying fonts were copied to dist..."
if [ -f "$VSCODE_DIR/dist/superinference.ttf" ]; then
    echo "✅ Fonts copied successfully to dist/"
else
    echo "⚠️  Warning: Fonts may not have been copied to dist/"
fi

echo "📦 Installing extension in VS Code..."
code --install-extension "$VSCODE_DIR/superinference.vsix"

echo "🔄 Reloading VS Code windows..."
# Send reload command to all VS Code windows
osascript -e 'tell application "Visual Studio Code" to activate' 2>/dev/null || true
code --command workbench.action.reloadWindow 2>/dev/null || true

echo "✅ SuperInference extension installed and reloaded!"
echo "💡 You may need to manually reload VS Code windows (Ctrl+R / Cmd+R)" 