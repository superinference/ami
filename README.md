# SuperInference

We introduce SUPERINFERENCE, a
feedback-augmented open-source architecture for
large-language-model (LLM) agents designed for
complex programming and multi-step reasoning.

## Quick Start

### 1. Start the MCP Server

The MCP server provides the AI backend for the VS Code extension.

```bash
# HTTP mode (default, recommended)
./scripts/start-mcp-server.sh

# STDIO mode
./scripts/start-mcp-server.sh stdio
```

The server runs on port `3000` by default and automatically kills any existing process on that port.

### 2. Install the VS Code Extension

Build and install the extension with a single command:

```bash
./scripts/build-vscode-extension.sh
```

This script:
- Installs dependencies
- Builds the `.vsix` package
- Installs it in VS Code
- Reloads VS Code windows

**Requirements:** Node.js 18+

### 3. Run Benchmarks

Run the DABStep benchmark to evaluate performance:

```bash
# Basic usage (1 problem, both difficulties)
./scripts/run-benchmark.sh

# Custom configuration
./scripts/run-benchmark.sh [problems] [difficulty] [mcp-port] [output-dir]
```

**Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `problems` | `1` | Number of problems per difficulty level |
| `difficulty` | `both` | `easy`, `hard`, or `both` |
| `mcp-port` | `3000` | MCP server port |
| `output-dir` | auto | Timestamped directory in `benchmark/dabstep/results/` |

**Example:**
```bash
./scripts/run-benchmark.sh 5 hard 3000 ./my-results
```

**Requirements:** Python 3, `datasets` and `huggingface_hub` packages

## Configuration

Create a `.env` file in the project root (see `.env.example`):

```bash
# Default provider
DEFAULT_PROVIDER=gemini

# API Keys
GEMINI_API_KEY=your_key
OPENAI_API_KEY=your_key
DEEPSEEK_API_KEY=your_key
```

## License

GNU General Public License v3.0 - see LICENSE file for details.
