# SuperInference for VS Code

SuperInference integration for Visual Studio Code with advanced AI chat capabilities powered by **multiple AI providers** (Gemini, OpenAI, DeepSeek) and automatic embeddings-based context management.

## ✨ Features

### 🧠 **NEW: Automatic Embeddings & Smart Context**
- **Auto-indexing**: Automatically indexes all workspace files when you open a folder
- **Real-time updates**: Updates embeddings whenever you save file changes  
- **Smart context selection**: AI uses semantic search to find the most relevant code for your questions
- **Comprehensive coverage**: Indexes TypeScript, JavaScript, Python, Java, C++, Rust, Go, PHP, Ruby, Swift, Kotlin, HTML, CSS, JSON, YAML, Markdown, and more
- **Intelligent filtering**: Skips binary files, node_modules, .git, build outputs, and other non-essential directories
- **Performance optimized**: Processes files in batches with size limits to avoid overwhelming your system

### 🚀 **Core AI Capabilities**
- **Multi-Provider AI Support**: Choose between Gemini, OpenAI, and DeepSeek providers
- **Real-time AI Chat**: Stream conversations with your preferred AI provider
- **Dynamic Provider Switching**: Switch between providers on-the-fly
- **Intelligent Code Generation**: Generate code with full project context
- **Smart Code Editing**: AI-powered code modifications and improvements
- **Context-Aware Analysis**: AI understands your entire codebase through embeddings
- **Code Review & Optimization**: Get suggestions for better, more efficient code
- **Test Generation**: Automatically create comprehensive tests for your code

### 🎯 **Code Actions**
- **Explain Code**: Get detailed explanations of selected code segments
- **Fix Code**: Automatically detect and fix bugs or issues
- **Review Code**: Get comprehensive code reviews with improvement suggestions
- **Generate Tests**: Create unit tests for your functions and classes
- **Optimize Code**: Get performance and readability improvements
- **Edit Code**: Make specific modifications with natural language instructions

## 🛠️ **Embeddings Management Commands**

Access these through the VS Code Command Palette (`Ctrl/Cmd + Shift + P`):

- `SuperInference: Index Workspace for AI Context` - Manually trigger workspace indexing
- `SuperInference: Show Embeddings Status` - View current embeddings statistics
- `SuperInference: Clear All Embeddings` - Reset the embeddings database
- `SuperInference: Reindex Current File` - Force reindex the currently open file

## 📖 **How It Works**

### Automatic Indexing
1. **On Workspace Open**: SuperInference automatically scans and indexes all eligible files in your workspace
2. **On File Save**: When you save changes, the file is automatically reindexed with updated content
3. **Smart Filtering**: Only indexes text-based source files, skipping binaries and build artifacts
4. **Performance Limits**: Files larger than 500KB are skipped to maintain performance

### Smart Context Selection
- When you ask questions, SuperInference uses semantic search to find the most relevant code
- The AI gets context from files that are semantically related to your query, not just recently opened files
- This results in much more accurate and contextually relevant responses

### File Types Supported
- **Languages**: TypeScript, JavaScript, Python, Java, C++, C, C#, Go, Rust, PHP, Ruby, Swift, Kotlin
- **Web**: HTML, CSS, SCSS, JSON, YAML, XML
- **Documentation**: Markdown, plain text
- **Config**: Various configuration file formats

### Excluded Directories
- `node_modules/`, `.git/`, `dist/`, `build/`, `out/`, `target/`
- `.vscode/`, `coverage/`, `.next/`, `.nuxt/`, `vendor/`, `__pycache__`

## 🚀 **Getting Started**

1. **Install the Extension**: Install SuperInference from the VS Code marketplace
2. **Open a Workspace**: Open any folder in VS Code - SuperInference will automatically start indexing
3. **Wait for Indexing**: You'll see a notification when indexing is complete
4. **Start Chatting**: Click the SuperInference icon in the right sidebar or use `Ctrl/Cmd + Alt + P`

The AI will now have comprehensive understanding of your codebase through the embedded context!

## ⚙️ **Configuration**

The extension works out of the box with sensible defaults:
- **Auto-indexing**: Enabled by default when opening workspaces
- **File size limit**: 500KB per file (configurable)
- **Batch processing**: 10 files per batch to avoid API overload
- **Update frequency**: On save for open files, on change for workspace files

## 🔧 **Commands**

### Main Commands
- `Ctrl/Cmd + Alt + P` - Open SuperInference Chat
- Right-click context menu - Access SuperInference actions for selected code

### Chat Commands
- **Explain** - Get detailed code explanations
- **Fix** - Automatically fix code issues  
- **Review** - Get improvement suggestions
- **Generate** - Create new code based on requirements
- **Test** - Generate comprehensive tests
- **Optimize** - Performance and readability improvements

## 📊 **Embeddings Status**

Monitor your embeddings database:
- View total indexed files and entries
- See breakdown by file type
- Check indexing status and progress
- Monitor smart context effectiveness

## 🔄 **Backend Integration**

SuperInference connects to a local backend server (`http://localhost:3000`) that provides:
- **Multi-provider AI integration** (Gemini, OpenAI, DeepSeek)
- **Dynamic provider switching**
- Embeddings creation and management
- Vector similarity search
- Smart context selection

Make sure the SuperInference backend server is running for full functionality.

## ⚙️ **AI Provider Configuration**

Configure your AI providers by creating a `.env` file in the project root:
(or use `.env.example` as a reference)

```bash
# Default provider to use
DEFAULT_PROVIDER=gemini

# Gemini Configuration
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.5-flash
GEMINI_BASE_URL=https://generativelanguage.googleapis.com/v1beta
# Optional: dedicated small critic model
GEMINI_CRITIC_MODEL=gemini-pro

# OpenAI Configuration  
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4
OPENAI_BASE_URL=https://api.openai.com/v1
# Optional: dedicated small critic model
OPENAI_CRITIC_MODEL=gpt-3.5-turbo

# DeepSeek Configuration
DEEPSEEK_API_KEY=your_deepseek_api_key_here
DEEPSEEK_MODEL=deepseek-chat
DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
# Optional: dedicated small critic model
DEEPSEEK_CRITIC_MODEL=deepseek-chat

# Critic routing and strictness
# Choose which provider to use for the critic (gemini|openai|deepseek)
CRITIC_PROVIDER=gemini
# Approval threshold 0..1 (higher = stricter)
CRITIC_ACCEPT_THRESHOLD=0.6
```

### Provider Management Tools

- **Switch Provider**: `switch_provider(provider_name="openai")`
- **Get Status**: `get_provider_status()`
- **Test Provider**: `test_provider(provider_name="gemini")`
- **Update Config**: `update_generation_config(temperature=0.5)`

See [PROVIDERS.md](PROVIDERS.md) for detailed configuration options.

## 🤝 **Contributing**

We welcome contributions! Please see our contributing guidelines for more information.

## 📄 **License**

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Powered by Google Gemini AI with advanced embeddings-based context understanding**
