/**
 * Copyright (C) 2025 SuperInference contributors
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

import * as vscode from 'vscode';
import { getNonce } from './extension/utils';
import { sharedChatServiceImpl, ChatServiceClient } from './extension/chat/chatServiceImpl';
import { ExtensionHostServiceManager } from './common/ipc/extensionHost';
import { IChatViewService, CHAT_VIEW_SERVICE_NAME } from './common/chatService';
import { MessageItemModel } from './common/chatService/model';
import * as path from 'path';
import { MultiStepProcessor, BackendService } from './reasoning/multi-step-processor';
import { getInitializedMCPClient } from './common/mcp-client';
import { resetMCPClient } from './common/mcp-client';
import { getSnapshotManager } from './extension/snapshotManager';

// Helper function to clean diff format and extract actual code
function extractCleanCodeFromDiff(content: string): string {
	// Check if content contains diff markers
	if (content.includes('--- a/') || content.includes('+++ b/') || content.includes('@@')) {
		console.log('🔧 Detected diff format, extracting clean code...');

		// Try to extract code blocks first (look for ```code``` blocks)
		const codeBlockMatch = content.match(/```(?:\w+)?\n([\s\S]*?)```/);
		if (codeBlockMatch) {
			console.log('🔧 Found code block in diff, using that');
			return codeBlockMatch[1].trim();
		}

		// If no code blocks, parse the diff format
		const lines = (content || '').split('\n');
		const cleanLines: string[] = [];
		let inCodeSection = false;

		for (let i = 0; i < lines.length; i++) {
			const line = lines[i];

			// Skip diff headers completely
			if (line.startsWith('--- ') || line.startsWith('+++ ') || line.startsWith('@@')) {
				continue;
			}

			// Look for shebang or import statements to identify start of actual code
			if (line.startsWith('#!/') || line.trim().startsWith('import ') || line.trim().startsWith('from ')) {
				inCodeSection = true;
			}

			// If we're in a code section, process the line
			if (inCodeSection) {
				// Handle diff prefixes
				if (line.startsWith('+')) {
					// Added line - include without the + prefix
					cleanLines.push(line.substring(1));
				} else if (line.startsWith('-')) {
					// Removed line - skip it
					continue;
				} else if (line.startsWith(' ')) {
					// Context line - include without the space prefix
					cleanLines.push(line.substring(1));
				} else {
					// Regular line - include as is
					cleanLines.push(line);
				}
			} else {
				// Not in code section yet, but check if this line looks like code
				if (line.trim() && !line.includes('diff --git') && !line.includes('index ')) {
					cleanLines.push(line);
				}
			}
		}

		// If we extracted some lines, return them
		if (cleanLines.length > 0) {
			const result = cleanLines.join('\n').trim();
			console.log('🔧 Extracted', cleanLines.length, 'lines from diff');
			return result;
		}
	}

	// If it's not diff format or extraction failed, return original content
	console.log('🔧 No diff format detected, using original content');
	return content;
}

// Helper to streamline analysis content and remove redundant headings
function sanitizeAnalysisMarkdown(input: string): string {
	try {
		let text = input || '';
		// Remove common redundant headings/phrases (case-insensitive)
		const patterns = [
			/^\s{0,3}#{1,6}\s*chain\s+of\s+thoughts?\s+analysis\s*:?.*$/gim,
			/^\s{0,3}[-*_]\s*chain\s+of\s+thoughts?\s+analysis\s*:?.*$/gim,
			/^\s*🧠\s*chain\s+of\s+thoughts?\s+analysis\s*.*$/gim,
			/^\s*analy[sz]ing\s+your\s+request\s*\.\.\.\s*$/gim,
			/^\s*analysis\s+complete\s*[-–—]*\s*.*$/gim,
			/^\s*analysis\s+complete\s*$/gim,
			/^\s*analysis\s*$/gim
		];
		patterns.forEach(re => { text = text.replace(re, ''); });
		// Collapse multiple blank lines
		text = text.replace(/\n{3,}/g, '\n\n');
		return text.trimStart();
	} catch {
		return input || '';
	}
}

export class SuperInferenceChatProvider implements vscode.WebviewViewProvider, ChatServiceClient, BackendService {
	static readonly viewType = "superinference-assistant-view";

	private _view: vscode.WebviewView | null = null;
	private _context: vscode.ExtensionContext;
	private _serviceManager: ExtensionHostServiceManager | null = null;
	private currentAbortController: AbortController | null = null;
	private _multiStepProcessor: MultiStepProcessor;
	private _currentMessageId?: string;
	private _currentContextFiles?: any[];
	private _reasoningMode: 'pre_loop' = 'pre_loop'; // ALWAYS use PRE loop mode (paper compliance)
	private _preLoopAnalysis?: string; // Store PRE loop analysis content
	private _messageIdCounter: number = 0; // Proper message ID management

	constructor(context: vscode.ExtensionContext, multiStepProcessor: MultiStepProcessor) {
		this._context = context;
		this._multiStepProcessor = multiStepProcessor;

		// ALWAYS use PRE loop mode (paper compliance - no simple mode)
		this._reasoningMode = 'pre_loop';
		context.globalState.update('superinference.reasoningMode', 'pre_loop');
		console.log('🧠 ChatProvider: PRE loop mode enforced (paper compliance)');

		// Set up real-time reasoning callbacks
		this.setupReasoningCallbacks();
	}

	private async generateFinalResponse(originalPrompt: string, messageId: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🚀 Making final backend call for user response with context files:', contextFiles?.length || 0);

			// Refresh context files to get latest content
			const refreshedContextFiles = contextFiles ? await this.refreshAllContextFiles(contextFiles) : [];
			console.log('🔷 Refreshed context files for final response:', refreshedContextFiles.length);

			// Mark the analysis as completed
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: messageId,
					streamingPhase: 'response'
				}
			});

			// Helper to strip structured reasoning tags if they appear in final stream
			const stripReasoningBlocks = (text: string): string => {
				const tags = ['thinking', 'analysis', 'planning', 'execution', 'validation'];
				let cleaned = text;
				for (const t of tags) {
					const re = new RegExp(`<${t}>[\\s\\S]*?<\\/${t}>`, 'gi');
					cleaned = cleaned.replace(re, '');
				}
				// Strip common analysis headings accidentally emitted in final answer
				const headings = [
					/^\s*#{0,3}\s*chain of thought[s]? analysis\s*:?.*$/gim,
					/^\s*ai reasoning\s*&\s*analysis process.*$/gim,
					/^\s*analysis complete\s*-?.*$/gim,
					/^\s*analysis\s*:?.*$/gim
				];
				headings.forEach((h) => cleaned = cleaned.replace(h, ''));
				return cleaned;
			};

			// Make direct backend call with SSE streaming for final response
			console.log('🔌 Making direct backend call for final response (streaming)...');

			try {
				// Use MCP client for chat instead of direct HTTP
				const mcpClient = await getInitializedMCPClient();
				console.log('🔧 Making MCP chat request via client...');

				const chatResponse = await mcpClient.streamChat(
					originalPrompt,
					refreshedContextFiles || [],
					[], // chat history
					'default', // conversation ID
					messageId
				);

				console.log(`✅ Got MCP response: ${chatResponse.length} characters`);

				// Create a mock response object for compatibility with existing streaming code
				const mockResponseText = `data: ${JSON.stringify({ content: chatResponse })}\n\n`;
				const encoder = new TextEncoder();
				const mockChunk = encoder.encode(mockResponseText);

				const response = ({
					ok: true,
					body: {
						getReader: () => {
							let sent = false;
							return {
								read: async () => {
									if (!sent) {
										sent = true;
										return { done: false, value: mockChunk };
									} else {
										return { done: true, value: undefined };
									}
								}
							};
						}
					}
				}) as any;

				const reader = (response as any).body?.getReader();
				if (!reader) {
					throw new Error('No response reader available');
				}

				const decoder = new TextDecoder();
				let finalContent = '';
				const chatService = sharedChatServiceImpl();
				let buffer = '';

				while (true) {
					const { done, value } = await reader.read();
					if (done) break;

					const chunk = decoder.decode(value, { stream: true });
					buffer += chunk;

					// Process complete SSE events separated by double newlines
					let sepIndex: number;
					while ((sepIndex = buffer.indexOf('\n\n')) !== -1) {
						const eventBlock = buffer.slice(0, sepIndex);
						buffer = buffer.slice(sepIndex + 2);

						// Concatenate multiple data: lines
						const dataLines = eventBlock
							.split('\n')
							.filter(l => l.trim().startsWith('data:'))
							.map(l => l.replace(/^data:\s?/, ''));
						if (dataLines.length === 0) continue;

						const jsonStr = dataLines.join('');
						try {
							const data = JSON.parse(jsonStr);

							if (data.type === 'codeEdit') {
								console.log('🔧 Forwarding codeEdit to chat service for:', data.codeEdit?.fileName);
								await chatService.addMessage(
									data.content || `Code changes for ${data.codeEdit?.fileName}`,
									true,
									data.codeEdit,
									'file-edit'
								);
							} else if (!data.type && data.content) {
								finalContent += data.content;
								this._view?.webview.postMessage({
									type: 'messageChange',
									message: {
										id: messageId,
										contents: finalContent,
										isFinished: false,
										streamingPhase: 'response'
									}
								});
							} else if (data.content && ['analysis_start', 'analysis', 'analysis_complete', 'execution_start'].indexOf(data.type) === -1) {
								finalContent += data.content;
								this._view?.webview.postMessage({
									type: 'messageChange',
									message: {
										id: messageId,
										contents: finalContent,
										isFinished: false,
										streamingPhase: 'response'
									}
								});
							}
						} catch (e) {
							// ignore malformed partials
						}
					}
				}

				// Finalize
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: messageId,
						contents: finalContent.trim() || 'Analysis completed.',
						isFinished: true,
						streamingPhase: undefined
					}
				});

				console.log('✅ Updated analysis message with final response (streamed)');
			} catch (backendError) {
				console.error('❌ Direct backend call failed:', backendError);
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: messageId,
						contents: `Error generating response: ${backendError instanceof Error ? backendError.message : 'Unknown error'}`,
						isFinished: true,
						streamingPhase: undefined
					}
				});
			}

			console.log('✅ Final response generation flow completed');

		} catch (error) {
			console.error('❌ Error generating final response:', error);
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: messageId,
					contents: `Error generating response: ${error instanceof Error ? error.message : 'Unknown error'}`,
					isFinished: true,
					streamingPhase: undefined
				}
			});
		}
	}

	private setupReasoningCallbacks(): void {
		// Set up proper callbacks using the new public methods
		this._multiStepProcessor.setCallbacks(
			(step) => {
				// Update the analysis content in real-time as steps stream tokens
				if (this._currentMessageId) {
					const plan = this._multiStepProcessor.getCurrentPlan();
					if (plan && step.output !== undefined) {
						const stepNum = plan.steps.findIndex((s: any) => s.id === step.id) + 1;
						const statusIcon = step.status === 'completed' ? '✅' : step.status === 'in_progress' ? '🔄' : step.status === 'failed' ? '❌' : '⏳';
						const header = `### ${stepNum}. ${statusIcon} ${step.title}`;
						const body = sanitizeAnalysisMarkdown(step.output || '');

						(plan as any)._stepBodies = (plan as any)._stepBodies || {};
						(plan as any)._stepBodies[step.id] = `${header}\n\n${body}\n\n`;
						plan.analysisContent = Object.values((plan as any)._stepBodies).join('');

						this._view?.webview.postMessage({
							type: 'messageChange',
							message: {
								id: this._currentMessageId,
								analysisContent: plan.analysisContent,
								streamingPhase: 'analysis'
							}
						});
					}
				}

				this.sendReasoningUpdate({
					type: 'stepUpdate',
					step: step
				});
			},
			(plan) => {
				this.sendReasoningUpdate({
					type: 'planUpdate',
					plan: plan
				});

				if (this._currentMessageId && plan.status === 'completed') {
					// Do not append extra completion headings; just proceed to final response
					this._view?.webview.postMessage({
						type: 'messageChange',
						message: {
							id: this._currentMessageId,
							streamingPhase: 'execution'
						}
					});

					this.generateFinalResponse(plan.originalInstruction, this._currentMessageId, this._currentContextFiles);
				}
			}
		);
	}

	public resolveWebviewView(
		webviewView: vscode.WebviewView,
		_context: vscode.WebviewViewResolveContext,
		_token: vscode.CancellationToken
	) {
		this._view = webviewView;

		const { extensionUri } = this._context;
		const { webview } = webviewView;
		const baseUri = vscode.Uri.joinPath(extensionUri, "dist");

		webview.options = {
			enableScripts: true,
			localResourceRoots: [baseUri, extensionUri],
		};

		webview.html = this._getHtmlForWebview(webview, baseUri);

		// Initialize chat service
		const chatService = sharedChatServiceImpl();
		chatService.attachClient(this);

		// Setup service manager for communication
		const serviceManager = new ExtensionHostServiceManager(webview);
		serviceManager.registerService(chatService);
		this._serviceManager = serviceManager;

		// Handle messages from webview
		webview.onDidReceiveMessage(async (message) => {
			await this.handleWebviewMessage(message);
		});

		// Send initial reasoning mode to webview after a short delay
		setTimeout(() => {
			console.log('🔷 ChatProvider: Sending initial reasoning mode to webview:', this._reasoningMode);
			webview.postMessage({
				type: 'reasoningModeSync',
				mode: this._reasoningMode
			});
		}, 1000);

		// Register enhanced chat view service
		class ChatViewServiceImpl implements IChatViewService {
			constructor(private provider: SuperInferenceChatProvider) { }

			get name(): string {
				return CHAT_VIEW_SERVICE_NAME;
			}

			async setIsBusy(isBusy: boolean): Promise<void> {
				webviewView.webview.postMessage({
					type: 'setBusy',
					isBusy: isBusy
				});
			}

			async setHasSelection(hasSelection: boolean): Promise<void> {
				webviewView.webview.postMessage({
					type: 'setHasSelection',
					hasSelection: hasSelection
				});
			}

			async addMessage(msg: MessageItemModel): Promise<void> {
				webviewView.webview.postMessage({
					type: 'addMessage',
					message: msg
				});
			}

			async updateMessage(msg: MessageItemModel): Promise<void> {
				webviewView.webview.postMessage({
					type: 'updateMessage',
					message: msg
				});
			}

			async clearMessage(): Promise<void> {
				webviewView.webview.postMessage({
					type: 'clearMessage'
				});
			}

			async focus(): Promise<void> {
				webviewView.show();
			}

			async insertCodeSnippet(contents: string): Promise<void> {
				const editor = vscode.window.activeTextEditor;
				if (editor) {
					const position = editor.selection.active;
					await editor.edit(editBuilder => {
						editBuilder.insert(position, contents);
					});
				}
			}

			async openFile(uri: string): Promise<void> {
				const fileUri = vscode.Uri.parse(uri);
				await vscode.window.showTextDocument(fileUri);
			}

			async getWorkspaceContext(): Promise<any> {
				const editor = vscode.window.activeTextEditor;
				if (!editor) return null;

				return {
					activeFile: editor.document.fileName,
					language: editor.document.languageId,
					selection: {
						start: {
							line: editor.selection.start.line,
							character: editor.selection.start.character
						},
						end: {
							line: editor.selection.end.line,
							character: editor.selection.end.character
						}
					},
					selectedText: editor.document.getText(editor.selection),
					workspace: vscode.workspace.workspaceFolders?.[0]?.uri.fsPath
				};
			}

			async applyFileEdit(filePath: string, newContent: string, startLine?: number, endLine?: number): Promise<boolean> {
				try {
					// Clean the content in case it contains diff format
					const cleanContent = extractCleanCodeFromDiff(newContent);

					console.log('🔧 ApplyFileEdit - Original content length:', newContent.length);
					console.log('🔧 ApplyFileEdit - Clean content length:', cleanContent.length);
					console.log('🔧 ApplyFileEdit - Contains diff markers:', newContent.includes('--- a/') || newContent.includes('+++ b/'));

					// Handle both file paths and URIs
					let fileUri: vscode.Uri;
					if (filePath.startsWith('file://')) {
						// It's already a URI
						fileUri = vscode.Uri.parse(filePath);
					} else if (filePath.startsWith('/')) {
						// It's an absolute path
						fileUri = vscode.Uri.file(filePath);
					} else {
						// It's a relative path, resolve it relative to workspace
						const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
						if (workspaceFolder) {
							fileUri = vscode.Uri.joinPath(workspaceFolder.uri, filePath);
						} else {
							fileUri = vscode.Uri.file(filePath);
						}
					}

					const document = await vscode.workspace.openTextDocument(fileUri);

					// Show the document in an editor
					const editor = await vscode.window.showTextDocument(document);

					// Apply the edit using the cleaned content
					const success = await editor.edit(editBuilder => {
						if (startLine !== undefined && endLine !== undefined) {
							// Replace specific lines
							const startPos = new vscode.Position(startLine, 0);
							const endPos = new vscode.Position(endLine, document.lineAt(endLine).text.length);
							const range = new vscode.Range(startPos, endPos);
							editBuilder.replace(range, cleanContent);
						} else {
							// Replace entire file content
							const fullRange = new vscode.Range(
								document.positionAt(0),
								document.positionAt(document.getText().length)
							);
							editBuilder.replace(fullRange, cleanContent);
						}
					});

					if (success) {
						vscode.window.showInformationMessage(`Applied changes to ${fileUri.fsPath}`);
						return true;
					} else {
						vscode.window.showErrorMessage(`Failed to apply changes to ${fileUri.fsPath}`);
						return false;
					}
				} catch (error) {
					console.error('Error applying file edit:', error);
					vscode.window.showErrorMessage(`Error applying changes: ${error}`);
					return false;
				}
			}

			async showFileDiff(filePath: string, _originalContent: string, _newContent: string): Promise<void> {
				try {
					// Create temporary files for diff view
					const originalUri = vscode.Uri.parse(`untitled:${filePath}.original`);
					const newUri = vscode.Uri.parse(`untitled:${filePath}.new`);

					// Open diff view
					await vscode.commands.executeCommand(
						'vscode.diff',
						originalUri,
						newUri,
						`${filePath} (Original ↔ Proposed Changes)`
					);
				} catch (error) {
					console.error('Error showing file diff:', error);
				}
			}
		}

		serviceManager.registerService(new ChatViewServiceImpl(this));
	}

	private async handleWebviewMessage(message: any): Promise<void> {
		console.log('🔷 ChatProvider: Received message from webview:', message);
		const { type, ...data } = message;

		switch (type) {
			case 'streamChat':
				console.log('🔷 ChatProvider: Handling streamChat with prompt:', data.prompt);
				await this.handleStreamChat(data.prompt, data.contextFiles);
				break;
			case 'streamEdit':
				console.log('🔷 ChatProvider: Handling streamEdit with prompt:', data.prompt);
				console.log('🔷 ChatProvider: streamEdit contextFiles:', data.contextFiles?.length || 0, 'files');
				await this.handleStreamEdit(data.prompt, data.filePath, data.selection, data.contextFiles);
				break;
			case 'streamGenerate':
				console.log('🔷 ChatProvider: Handling streamGenerate with prompt:', data.prompt);
				await this.handleStreamGenerate(data.prompt, data.contextFiles);
				break;
			case 'streamCreate':
				console.log('🔷 ChatProvider: Handling streamCreate with prompt:', data.prompt);
				await this.handleStreamCreate(data.prompt, data.contextFiles);
				break;
			case 'applyCodeEdit':
				console.log('🔷 ChatProvider: Handling applyCodeEdit');
				await this.handleApplyCodeEdit(data.messageId, data.codeEdit);
				break;
			// Removed: updateReasoningMode - PRE loop is now ALWAYS enforced (paper compliance)
			case 'updateReasoningMode':
				// Ignore mode change requests - PRE loop is mandatory
				console.log('🔷 ChatProvider: Ignoring reasoning mode change request - PRE loop is mandatory for paper compliance');
				vscode.window.showInformationMessage(
					'SuperInference: PRE loop mode is mandatory for paper compliance and cannot be changed.'
				);
				break;
			case 'rejectCodeEdit':
				console.log('🔷 ChatProvider: Handling rejectCodeEdit');
				await this.handleRejectCodeEdit(data.messageId);
				break;
			case 'restoreFileSnapshots':
				console.log('🔷 ChatProvider: Handling restoreFileSnapshots for checkpoint restoration');
				// Now handled by VS Code native snapshot manager
				await this.handleRestoreFileSnapshots(data.snapshotId);
				break;
			case 'checkpointRestored':
				console.log('🔷 ChatProvider: Handling checkpointRestored notification');
				await this.handleCheckpointRestored(data.snapshotId);
				break;
			case 'getWorkspaceContext':
				console.log('🔷 ChatProvider: Handling getWorkspaceContext');
				await this.sendWorkspaceContext();
				break;
			case 'addFileToContext':
				console.log('🔷 ChatProvider: Handling addFileToContext');
				await this.handleAddFileToContext();
				break;
			case 'showFilePicker':
				console.log('🔷 ChatProvider: Handling showFilePicker');
				await this.handleShowFilePicker();
				break;
			case 'addAllOpenFiles':
				console.log('🔷 ChatProvider: Handling addAllOpenFiles');
				await this.handleAddAllOpenFiles();
				break;
			case 'searchFiles':
				console.log('🔷 ChatProvider: Handling searchFiles');
				await this.handleSearchFiles(data.query);
				break;
			case 'compareCheckpoint':
				console.log('🔷 ChatProvider: Handling compareCheckpoint');
				await this.handleCompareCheckpoint(data.checkpointId, data.conversationId);
				break;

			case 'addFileByUri':
				console.log('🔷 ChatProvider: Handling addFileByUri');
				await this.handleAddFileByUri(data.uri);
				break;
			case 'refreshContextFiles':
				console.log('🔷 ChatProvider: Handling refreshContextFiles');
				await this.handleRefreshContextFiles();
				break;
			case 'executeCommand':
				console.log('🔷 ChatProvider: *** IMPORTANT *** Handling executeCommand:', data.command);
				console.log('🔷 ChatProvider: executeCommand args:', data.args);
				await this.handleExecuteCommand(data.command, data.args);
				break;
			case 'command':
				console.log('🔷 ChatProvider: *** IMPORTANT *** Handling command:', data.command);
				console.log('🔷 ChatProvider: command args:', data.args);
				await this.handleExecuteCommand(data.command, data.args);
				break;
			case 'clearMessage':
				console.log('🔷 ChatProvider: Handling clearMessage');
				this.handleClearMessage();
				break;
			// NEW: Embeddings message handlers
			case 'getEmbeddingsStatus':
				console.log('🔷 ChatProvider: Handling getEmbeddingsStatus');
				await this.handleGetEmbeddingsStatus();
				break;
			case 'indexWorkspace':
				console.log('🔷 ChatProvider: Handling indexWorkspace');
				await this.handleIndexWorkspace();
				break;
			case 'clearEmbeddings':
				console.log('🔷 ChatProvider: Handling clearEmbeddings');
				await this.handleClearEmbeddings();
				break;
			case 'reindexCurrentFile':
				console.log('🔷 ChatProvider: Handling reindexCurrentFile');
				await this.handleReindexCurrentFile();
				break;
			case 'updateEmbeddingsSettings':
				console.log('🔷 ChatProvider: Handling updateEmbeddingsSettings');
				await this.handleUpdateEmbeddingsSettings(data.settings);
				break;
			case 'updateExtensionSettings':
				console.log('🔷 ChatProvider: Handling updateExtensionSettings');
				await this.handleUpdateExtensionSettings(data.settings);
				break;
			default:
				console.log('🔷 ChatProvider: Unknown message type:', type);
		}
	}

	private async handleStreamChat(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Starting streamChat with prompt:', prompt);
			console.log('🔷 ChatProvider: Current reasoning mode:', this._reasoningMode);
			console.log('🔷 ChatProvider: Received contextFiles:', contextFiles?.length || 0, 'files');

			this._currentContextFiles = contextFiles;

			// 🧠 UNIFIED APPROACH: Use handleUniversalPRELoop for ALL interactions in both modes
			console.log('🧠 ChatProvider: Using unified handleUniversalPRELoop approach for all interactions');
			console.log('🧠 ChatProvider: Current reasoning mode:', this._reasoningMode);
			console.log('🧠 ChatProvider: Calling handleUniversalPRELoop with prompt:', prompt.substring(0, 100));
			await this.handleUniversalPRELoop(prompt, contextFiles);
		return;

		} catch (error) {
			console.error('🔷 ChatProvider: Error in handleStreamChat:', error);
			vscode.window.showErrorMessage(`Failed to process chat request: ${error instanceof Error ? error.message : 'Unknown error'}`);
		} finally {
			this._currentMessageId = undefined;
		}
	}

	private sendReasoningUpdate(update: any): void {
		if (this._view) {
			this._view.webview.postMessage({
				type: 'reasoningUpdate',
				...update
			});
		}
	}

	private buildAnalysisContent_DEPRECATED(plan: any): string {
		// Return sanitized accumulated content
		if (!plan || !plan.analysisContent) return '';
		return sanitizeAnalysisMarkdown(plan.analysisContent);
	}

	private async refreshAllContextFiles(contextFiles: any[]): Promise<any[]> {
		const refreshedFiles: any[] = [];

		for (const file of contextFiles) {
			try {
				// Parse the URI to get a proper file URI
				let fileUri: vscode.Uri;
				if (file.uri.startsWith('file://')) {
					fileUri = vscode.Uri.parse(file.uri);
				} else {
					fileUri = vscode.Uri.file(file.uri);
				}

				// Read the current content from disk
				const fileBytes = await vscode.workspace.fs.readFile(fileUri);
				const currentContent = Buffer.from(fileBytes).toString('utf8');

				// Create refreshed file object
				const refreshedFile = {
					...file,
					content: currentContent
				};

				refreshedFiles.push(refreshedFile);
				console.log('🔷 ChatProvider: Refreshed context file:', file.name, 'with', currentContent.length, 'characters');

			} catch (error) {
				console.error('🔷 ChatProvider: Failed to refresh context file:', file.uri, error);
				// If we can't read the file, use the original content
				refreshedFiles.push(file);
			}
		}

		return refreshedFiles;
	}

	private async handleStreamEdit(prompt: string, filePath: string, selection?: any, contextFiles?: any[], skipUserMessage: boolean = false): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Starting streamEdit for file:', filePath);
			console.log('🔷 ChatProvider: Current reasoning mode:', this._reasoningMode);

				// ALWAYS use PRE loop (paper compliance)
			console.log('🧠 ChatProvider: PRE loop mode enforced - using theoretical framework for ALL edits');
			await this.handlePRELoopEdit(prompt, filePath, selection, contextFiles, skipUserMessage);

		} catch (error) {
			console.error('🔷 ChatProvider: Error in handleStreamEdit:', error);
			vscode.window.showErrorMessage(`Failed to edit file: ${error instanceof Error ? error.message : 'Unknown error'}`);
		}
	}

	private async handleStreamGenerate(prompt: string, contextFiles?: any[]): Promise<void> {
		// 🧠 UNIFIED APPROACH: Use handleUniversalPRELoop for consistency
		console.log('🧠 ChatProvider: handleStreamGenerate routing to unified handler');
		await this.handleUniversalPRELoop(prompt, contextFiles);
	}

	// DEPRECATED: This method is replaced by handleUniversalPRELoop for consistency
	private async handlePRELoopGeneration_DEPRECATED(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 ChatProvider: Starting PRE loop generation with theoretical framework');
			
			const mcpClient = await getInitializedMCPClient();
			const activeDoc = vscode.window.activeTextEditor?.document;
			const currentFileContent = activeDoc ? activeDoc.getText() : undefined;
			const languageId = activeDoc?.languageId || 'python';
			const workspacePath = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || '';

		// CRITICAL FIX: Don't create duplicate user message if handleUniversalPRELoop already created one
		// This prevents message override issues when both PRE Loop methods are called
		console.log('🧠 CRITICAL: handlePRELoopGeneration - NOT creating user message to avoid duplicates');
		const chatService = sharedChatServiceImpl();
		// const userMessageId = await chatService.addMessage(`Generate: ${prompt}`, false); // DISABLED

			// Start assistant response with PRE loop analysis
			const assistantMessageId = await chatService.addMessage('', true);
			this._currentMessageId = assistantMessageId;

			// Show initial analysis phase
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					analysisContent: '### 🧠 PRE Loop Analysis\n\nInitializing event-driven planning...\n',
					streamingPhase: 'analysis'
				}
			});

			// Execute the theoretical PRE loop
			const result = await mcpClient.planExecute(
				prompt,
				currentFileContent,
				languageId,
				workspacePath,
				contextFiles
			);

			console.log('🧠 PRE loop result:', result);

			// Display the plan and steps
			let analysisContent = '### 🧠 Event-Driven PRE Loop Analysis\n\n';
			
			if (result.events_fired) {
				analysisContent += `**Events Fired:** ${result.events_fired}\n`;
			}
			
			if (result.steps && Array.isArray(result.steps)) {
				analysisContent += '\n**Planned Steps:**\n';
				result.steps.forEach((step: any, idx: number) => {
					const statusIcon = step.status === 'completed' ? '✅' : step.status === 'failed' ? '❌' : '⏳';
					const probability = step.successProbability ? ` (p=${step.successProbability.toFixed(2)})` : '';
					analysisContent += `${idx + 1}. ${statusIcon} **${step.title}**${probability}\n`;
					if (step.description) {
						analysisContent += `   ${step.description}\n`;
					}
					if (step.output && step.status === 'completed') {
						analysisContent += `   Output: ${step.output.substring(0, 100)}${step.output.length > 100 ? '...' : ''}\n`;
					}
					if (step.error && step.status === 'failed') {
						analysisContent += `   Error: ${step.error}\n`;
					}
					analysisContent += '\n';
				});
			}

			// Add theoretical metrics
			if (result.execution_time) {
				analysisContent += `**Execution Time:** ${result.execution_time.toFixed(2)}s\n`;
			}

			// Update the analysis content
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					analysisContent: analysisContent,
					streamingPhase: 'response'
				}
			});

			// Generate final response from approved artifacts
			let finalResponse = '';
			if (result.approved_artifacts && Array.isArray(result.approved_artifacts) && result.approved_artifacts.length > 0) {
				finalResponse = result.approved_artifacts.join('\n\n');
			} else {
				// Fallback to simple generation if no artifacts were approved
				console.log('🧠 No approved artifacts, falling back to simple generation');
				finalResponse = await mcpClient.streamGenerate(prompt, currentFileContent, languageId, workspacePath);
			}

			// Stream the final response
			this.streamResponseContent(finalResponse, assistantMessageId);

			// Store successful PRE loop pattern for future use
			if (result.approved_artifacts && result.approved_artifacts.length > 0) {
				try {
					await mcpClient.createEmbeddings(
						`PRE Loop Success: ${prompt}\nApproved Artifacts: ${result.approved_artifacts.length}\nEvents: ${result.events_fired}`,
						{
							type: 'pre_loop_success',
							prompt: prompt,
							events_fired: result.events_fired,
							execution_time: result.execution_time
						}
					);
				} catch (error) {
					console.debug('Failed to store PRE loop success pattern:', error);
				}
			}

		} catch (error) {
			console.error('🧠 ChatProvider: Error in PRE loop generation:', error);
			// Fallback to simple generation on error
		const chatService = sharedChatServiceImpl();
		await chatService.streamGenerate(prompt, contextFiles);
		}
	}

	private generateMessageId(): string {
		return (++this._messageIdCounter).toString();
	}

	private async handleUniversalPRELoop(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 ChatProvider: Starting universal handler for prompt:', prompt);
			console.log('🧠 ChatProvider: Reasoning mode:', this._reasoningMode);
			
			// Create messages with proper timestamps to avoid conflicts (following working pattern)
			const baseTimestamp = Date.now();
			
		// User message - CRITICAL: This must be created first and never overridden
		const userMessageId = `user-${baseTimestamp}`;
		console.log('🧠 CRITICAL: Creating user message with ID:', userMessageId, 'Content:', prompt.substring(0, 100));
		this._view?.webview.postMessage({
			type: 'newMessage',
			message: {
				id: userMessageId,
				contents: prompt,
				isReply: false,
				isFinished: true,
				timestamp: baseTimestamp
			}
		});
		console.log('🧠 CRITICAL: User message sent to webview');

		// Analysis message (with offset timestamp to prevent conflicts)
		const analysisMessageId = `analysis-${baseTimestamp + 100}`;
		console.log('🧠 CRITICAL: Creating analysis message with ID:', analysisMessageId);
		console.log('🧠 CRITICAL: Analysis message should NOT override user message ID:', userMessageId);

		this._view?.webview.postMessage({
			type: 'newMessage',
			message: {
				id: analysisMessageId,
				contents: '',
				isReply: true,
				isFinished: false,
				streamingPhase: 'analysis',
				analysisContent: '### 🧠 SuperInference PRE Loop Framework\n\nInitializing event-driven reasoning...\n',
				timestamp: baseTimestamp + 100
			}
		});
		console.log('🧠 CRITICAL: Analysis message sent to webview with separate ID');

		const mcpClient = await getInitializedMCPClient();
		const activeDoc = vscode.window.activeTextEditor?.document;
		const currentFileContent = activeDoc ? activeDoc.getText() : undefined;
		const languageId = activeDoc?.languageId || 'python';
		const workspacePath = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || '';

		let result: any;
		let analysisContent: string;

		// 🧠 ALWAYS USE PRE LOOP (Paper Compliance)
		console.log('🧠 Using PRE Loop theoretical framework (mandatory)');
		
		// Execute the theoretical PRE loop for analysis
		result = await mcpClient.planExecute(
			prompt,
			currentFileContent,
			languageId,
			workspacePath,
			contextFiles
		);

		console.log('🧠 PRE Loop result:', result);

		// Build comprehensive analysis showing theoretical framework in action
		analysisContent = '### 🧠 SuperInference PRE Loop Framework\n\n';
		analysisContent += '*Event-driven reasoning with dynamic tool orchestration*\n\n';

		console.log('🧠 Universal handler result:', result);
		console.log('🧠 Result has steps:', result?.steps?.length || 0);
		console.log('🧠 Result steps details:', result?.steps);
		console.log('🧠 Result events_fired:', result?.events_fired);
		console.log('🧠 Result approved_artifacts:', result?.approved_artifacts?.length || 0);
		console.log('🧠 Result execution_time:', result?.execution_time);
		
		// Show POMDP metrics (always PRE Loop)
		if (result.events_fired !== undefined) {
			analysisContent += `**Events Fired (𝒞):** ${result.events_fired}\n`;
		}
		
		if (result.execution_time !== undefined) {
			analysisContent += `**Execution Time:** ${result.execution_time.toFixed(2)}s\n`;
		}

		// Show steps (always present in PRE Loop)
		if (result.steps && Array.isArray(result.steps)) {
			console.log('🧠 Building analysis content for', result.steps.length, 'steps');
			analysisContent += '\n**Event-Driven Reasoning Steps:**\n';
			result.steps.forEach((step: any, idx: number) => {
					console.log(`🧠 Processing step ${idx + 1}:`, step.title, 'Status:', step.status, 'Tools:', step.tools);
					try {
						const statusIcon = step.status === 'completed' ? '✅' : step.status === 'failed' ? '❌' : '⏳';
						const probability = step.successProbability ? ` (belief: p=${step.successProbability.toFixed(2)})` : '';
						const title = step.title || 'Unknown Step';
						analysisContent += `${idx + 1}. ${statusIcon} **${title}**${probability}\n`;
						
						if (step.description) {
							analysisContent += `   *${step.description}*\n`;
						}
						
						// Show which tools were used for this step
						if (step.tools && Array.isArray(step.tools) && step.tools.length > 0) {
							analysisContent += `   🔧 **Tools Used:** ${step.tools.join(', ')}\n`;
						}
						
						if (step.output && step.status === 'completed') {
							const output = String(step.output);
							analysisContent += `   📦 **Critic-Approved Artifact:** ${output.substring(0, 150)}${output.length > 150 ? '...' : ''}\n`;
						}
						
						if (step.error && step.status === 'failed') {
							analysisContent += `   ❌ **Critic Rejection:** ${step.error}\n`;
						}
						
						analysisContent += '\n';
					} catch (stepError) {
						console.error('🧠 Error processing step in analysis:', stepError);
						analysisContent += `${idx + 1}. ❌ **Step Error** - Could not display step details\n\n`;
					}
				});
			}

			// Add framework status
			if (result.benchmark_mode) {
				analysisContent += '**Framework Mode:** Benchmark (optimized parameters)\n';
			}
			
			// Show all tools that were used across all steps
			const allToolsUsed = new Set<string>();
			if (result.steps && Array.isArray(result.steps)) {
				result.steps.forEach((step: any) => {
					if (step.tools && Array.isArray(step.tools)) {
						step.tools.forEach((tool: string) => allToolsUsed.add(tool));
					}
				});
			}
			if (allToolsUsed.size > 0) {
				analysisContent += `\n**🔧 MCP Tools Orchestrated:** ${Array.from(allToolsUsed).join(', ')}\n`;
			}

			// Update the analysis message with complete analysis (separate ID prevents override)
			console.log('🧠 Final analysis content length:', analysisContent.length);
			console.log('🧠 Final analysis content preview:', analysisContent.substring(0, 500));
			console.log('🧠 Sending analysis to message ID:', analysisMessageId);
			
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: analysisMessageId,  // Use analysis message ID, not assistant message ID
					analysisContent: analysisContent,
					streamingPhase: 'response'
				}
			});

			// Add a delay to ensure analysis is visible before any code edits
			await new Promise(resolve => setTimeout(resolve, 1000));

			// Mark analysis message as complete
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: analysisMessageId,
					isFinished: true,
					streamingPhase: 'complete'
				}
			});

			// CRITICAL: Only create code edit if there are completed steps with actual code output
			// This ensures the theoretical framework requirement that code changes are proposed as steps
			let codeOutputFromSteps = '';
			let hasCodeSteps = false;

			if (result.steps && Array.isArray(result.steps)) {
				for (const step of result.steps) {
					if (step.status === 'completed' && step.output) {
						const output = String(step.output);
						// Simplified detection - trust PRE loop's tool selection
						// If step used modification/generation tools, it's code
						const usedCodeTools = step.tools && step.tools.some((tool: string) => 
							tool.includes('edit') || 
							tool.includes('generate') || 
							tool.includes('remove_print') ||
							tool.includes('analyze_code')
						);
						
						// Fallback to basic heuristics if no tool info
						const hasSubstantialContent = output.length > 50;
						const titleSuggestsCode = step.title && (
							step.title.toLowerCase().includes('remove') ||
							step.title.toLowerCase().includes('edit') ||
							step.title.toLowerCase().includes('modify') ||
							step.title.toLowerCase().includes('delete') ||
							step.title.toLowerCase().includes('update') ||
							step.title.toLowerCase().includes('fix') ||
							step.title.toLowerCase().includes('change') ||
							step.title.toLowerCase().includes('generate')
						);

						if (usedCodeTools || (hasSubstantialContent && titleSuggestsCode)) {
							codeOutputFromSteps = output;
							hasCodeSteps = true;
							console.log('🧠 Found code output in completed step:', step.title, '- Tools used:', step.tools || 'none');
							break;
						}
					}
				}
			}

			// Use MCP server's AI intent analysis to determine if this should create a code edit
			let shouldCreateCodeEdit = false;
			if (contextFiles && contextFiles.length > 0) {
				try {
					const currentFile = vscode.window.activeTextEditor?.document.uri.fsPath;
					const analysis = await mcpClient.analyzeRequestIntent(prompt, contextFiles, currentFile);
					console.log('🧠 PRE loop intent analysis result:', analysis);
					
					// Use AI analysis AND require completed steps with code output (theoretical compliance)
					const aiSuggestsCodeEdit = (analysis.action_type === 'edit' && analysis.confidence > 0.6) ||
											  (analysis.action_type === 'generate' && analysis.confidence > 0.7 && analysis.target_files?.length > 0);
					
					// THEORETICAL REQUIREMENT: Code changes must come from completed PRE loop steps
					shouldCreateCodeEdit = aiSuggestsCodeEdit && hasCodeSteps;
					
					console.log('🧠 Code edit decision:', {
						aiSuggestsCodeEdit,
						hasCodeSteps,
						shouldCreateCodeEdit,
						confidence: analysis.confidence,
						actionType: analysis.action_type,
						prompt: prompt.substring(0, 100),
						contextFilesCount: contextFiles?.length || 0,
						stepsCount: result.steps?.length || 0,
						codeOutputLength: codeOutputFromSteps?.length || 0
					});
				} catch (error) {
					console.log('🧠 Intent analysis failed, using fallback detection:', error);
					// Enhanced fallback to simple keyword detection if AI analysis fails
					const promptLower = prompt.toLowerCase();
					shouldCreateCodeEdit = promptLower.includes('remove') || 
										  promptLower.includes('edit') || 
										  promptLower.includes('modify') || 
										  promptLower.includes('change') ||
										  promptLower.includes('delete') ||
										  promptLower.includes('fix') ||
										  promptLower.includes('update') ||
										  promptLower.includes('replace') ||
										  // Specific patterns for code modification
										  promptLower.includes('print') ||
										  promptLower.includes('console.log') ||
										  promptLower.includes('debug') ||
										  promptLower.includes('comment') ||
										  promptLower.includes('statement');
					
					// If we have code steps from PRE loop, be more permissive
					if (hasCodeSteps && !shouldCreateCodeEdit) {
						console.log('🧠 PRE loop produced code steps, enabling code edit despite keyword detection');
						shouldCreateCodeEdit = true;
					}
				}
			}

			// Additional safety net: If we have context files and the request clearly involves code modification,
			// create code edit even if other conditions aren't perfectly met
			if (!shouldCreateCodeEdit && contextFiles && contextFiles.length > 0) {
				const promptLower = prompt.toLowerCase();
				const isCodeModificationRequest = 
					promptLower.includes('remove') && (promptLower.includes('print') || promptLower.includes('statement')) ||
					promptLower.includes('delete') && (promptLower.includes('print') || promptLower.includes('statement')) ||
					promptLower.includes('fix') || promptLower.includes('clean') || promptLower.includes('refactor');
				
				if (isCodeModificationRequest) {
					console.log('🧠 Safety net: Enabling code edit for clear code modification request');
					shouldCreateCodeEdit = true;
				}
			}

			if (shouldCreateCodeEdit && contextFiles && contextFiles.length > 0) {
				console.log('🧠 PRE loop creating code edit interface based on AI intent analysis');
				
				// Use MCP server's proper diff generation tools (like simple mode)
				const targetFile = contextFiles[0];
				const originalContent = targetFile.content;
				
				// THEORETICAL COMPLIANCE: Use code output from completed PRE loop steps
				let editedContent = '';
				if (codeOutputFromSteps) {
					// Use the code output from completed steps (theoretical requirement)
					editedContent = codeOutputFromSteps;
					console.log('🧠 Using code output from completed PRE loop step');
				} else if (result.approved_artifacts && result.approved_artifacts.length > 0) {
					// Use approved artifacts as fallback
					editedContent = result.approved_artifacts[0];
					console.log('🧠 Using approved artifacts as fallback');
				} else {
					// Final fallback: use MCP streamEdit to generate the changes
					editedContent = await mcpClient.streamEdit(
						originalContent,
						prompt,
						targetFile.name || 'file',
						this.getLanguageFromPath(targetFile.uri || '')
					);
					console.log('🧠 Using MCP streamEdit as final fallback');
				}

				if (editedContent !== originalContent) {
					console.log('🧠 Using MCP generateFileDiff for proper diff calculation');
					
					// Use MCP server's generateFileDiff tool for proper diff calculation
					const diffResult = await mcpClient.generateFileDiff(
						targetFile.uri,
						originalContent,
						editedContent,
						3 // context lines
					);

					console.log('🧠 MCP diff result:', diffResult);

					// Create separate message for code edit (with unique timestamp to prevent override)
					const codeEditMessageId = `codeedit-${baseTimestamp + 200}`;
					console.log('🧠 Created separate code edit message with ID:', codeEditMessageId);

					this._view?.webview.postMessage({
						type: 'newMessage',
						message: {
							id: codeEditMessageId,
							contents: '',
							isReply: true,
							isFinished: false,
							messageType: 'file-edit',
							timestamp: baseTimestamp + 200
						}
					});

					// Create code edit with MCP-generated diff information
					const codeEdit = {
						filePath: targetFile.uri,
						originalCode: originalContent,
						newCode: editedContent,
						startLine: 1,
						endLine: (originalContent || '').split('\n').length,
						explanation: `PRE Loop: ${prompt}`,
						language: this.getLanguageFromPath(targetFile.uri || ''),
						fileName: targetFile.name || 'file',
						linesChanged: diffResult?.stats?.total_changes || 0,
						// Include MCP diff information
						diff: diffResult?.diff || '',
						stats: diffResult?.stats || {}
					};

					// Create snapshot before showing code edit
					await this.createSnapshotForCodeEdit(codeEditMessageId, `PRE Loop: ${prompt}`);

					// Update the separate code edit message (analysis message stays intact)
					this._view?.webview.postMessage({
						type: 'messageChange',
						message: {
							id: codeEditMessageId,
							codeEdit: codeEdit,
							streamingPhase: 'complete',
							isFinished: true,
							messageType: 'file-edit'
						}
					});
					return; // Exit early - code edit UI handles the rest
				}
			}

			// For non-code-edit requests, create separate response message (with unique timestamp)
			const responseMessageId = `response-${baseTimestamp + 200}`;
			console.log('🧠 Created separate response message with ID:', responseMessageId);

			this._view?.webview.postMessage({
				type: 'newMessage',
				message: {
					id: responseMessageId,
					contents: '',
					isReply: true,
					isFinished: false,
					timestamp: baseTimestamp + 200
				}
			});

		// PRE Loop response generation (always)
		let finalResponse = '';
		
		if (result.approved_artifacts && Array.isArray(result.approved_artifacts) && result.approved_artifacts.length > 0) {
			// Use approved artifacts from PRE loop
			finalResponse = result.approved_artifacts.join('\n\n');
			this.streamResponseContent(finalResponse, responseMessageId);
		} else {
			// Use real streaming if no approved artifacts
			await this.streamChatResponseRealTime(
				prompt,
				contextFiles || [],
				responseMessageId
			);
		}
		return; // Exit early since streaming is handled

		} catch (error) {
			console.error('🧠 ChatProvider: Error in universal PRE loop:', error);
			// Fallback to MCP client directly
			try {
				const mcpClient = await getInitializedMCPClient();
				const fallbackResponse = await mcpClient.streamChat(prompt, contextFiles || []);
				this.streamResponseContent(fallbackResponse, this._currentMessageId || 'fallback');
			} catch (fallbackError) {
				console.error('🧠 Fallback also failed:', fallbackError);
				this.streamResponseContent('Error: Could not process request.', this._currentMessageId || 'error');
			}
		}
	}

	// Removed: routeToSimpleModeWithAnalysis - now handling everything directly in handleUniversalPRELoop

	// DEPRECATED: This method is replaced by handleUniversalPRELoop for consistency
	private async handleStreamEditWithPREAnalysis_DEPRECATED(prompt: string, filePath: string, selection?: any, _contextFiles?: any[], skipUserMessage: boolean = false): Promise<void> {
		try {
			console.log('🧠 StreamEdit with PRE analysis - using proven simple mode workflow');
			
			// First, add a message with PRE loop analysis
			if (this._preLoopAnalysis) {
				const chatService = sharedChatServiceImpl();
				const analysisMessageId = await chatService.addMessage('', true);
				
				// Show the PRE loop analysis
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: analysisMessageId,
						analysisContent: this._preLoopAnalysis,
						streamingPhase: 'analysis'
					}
				});

				// Small delay to ensure analysis is visible
				await new Promise(resolve => setTimeout(resolve, 300));
			}

			// Then use the exact same streamEdit logic as simple mode
			const chatService = sharedChatServiceImpl();
			if (skipUserMessage && 'streamEditDirect' in chatService) {
				console.log('🧠 Using proven streamEditDirect method');
				await (chatService as any).streamEditDirect(prompt, filePath, selection);
			} else {
				console.log('🧠 Using proven streamEdit method');
				await chatService.streamEdit(prompt, filePath, selection);
			}

		} catch (error) {
			console.error('🧠 Error in streamEdit with PRE analysis:', error);
			// Fallback to simple mode
			const chatService = sharedChatServiceImpl();
			await chatService.streamEdit(prompt, filePath, selection);
		}
	}

	// DEPRECATED: This method is replaced by handleUniversalPRELoop for consistency
	private async handleStreamGenerateWithPREAnalysis_DEPRECATED(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			// Show PRE loop analysis first, then use simple mode
			if (this._preLoopAnalysis) {
				const chatService = sharedChatServiceImpl();
				const analysisMessageId = await chatService.addMessage('', true);
				
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: analysisMessageId,
						analysisContent: this._preLoopAnalysis,
						streamingPhase: 'analysis'
					}
				});
				await new Promise(resolve => setTimeout(resolve, 300));
			}

			// Use simple mode streamGenerate
			const chatService = sharedChatServiceImpl();
			await chatService.streamGenerate(prompt, contextFiles);
		} catch (error) {
			console.error('🧠 Error in streamGenerate with PRE analysis:', error);
			const chatService = sharedChatServiceImpl();
			await chatService.streamGenerate(prompt, contextFiles);
		}
	}

	// DEPRECATED: This method is replaced by handleUniversalPRELoop for consistency
	private async handleStreamCreateWithPREAnalysis_DEPRECATED(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			// Show PRE loop analysis first, then use simple mode
			if (this._preLoopAnalysis) {
				const chatService = sharedChatServiceImpl();
				const analysisMessageId = await chatService.addMessage('', true);
				
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: analysisMessageId,
						analysisContent: this._preLoopAnalysis,
						streamingPhase: 'analysis'
					}
				});
				await new Promise(resolve => setTimeout(resolve, 300));
			}

			// Use simple mode streamCreate
			const chatService = sharedChatServiceImpl();
			await chatService.streamCreate(prompt, contextFiles);
		} catch (error) {
			console.error('🧠 Error in streamCreate with PRE analysis:', error);
		const chatService = sharedChatServiceImpl();
		await chatService.streamCreate(prompt, contextFiles);
		}
	}

	// DEPRECATED: This method is replaced by handleUniversalPRELoop for consistency
	private async handleStreamChatWithPREAnalysis_DEPRECATED(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 StreamChat with PRE analysis - using MCP client directly');
			
			// Add user message
			const userMessageId = `user-${Date.now()}`;
			this._view?.webview.postMessage({
				type: 'newMessage',
				message: { id: userMessageId, contents: prompt, isReply: false, isFinished: true }
			});

			// Add assistant message with PRE analysis
			const assistantMessageId = `response-${Date.now()}`;
			this._currentMessageId = assistantMessageId;
			
			this._view?.webview.postMessage({
				type: 'newMessage',
				message: { 
					id: assistantMessageId, 
					contents: '', 
					isReply: true, 
					isFinished: false,
					streamingPhase: 'analysis', 
					analysisContent: this._preLoopAnalysis || ''
				}
			});

			// Use MCP client directly to avoid HTTP errors
			const mcpClient = await getInitializedMCPClient();
			const response = await mcpClient.streamChat(
				prompt,
				contextFiles || [],
				[], // chat history
				'default', // conversation ID
				assistantMessageId
			);

			// Stream the response
			this.streamResponseContent(response, assistantMessageId);

		} catch (error) {
			console.error('🧠 Error in streamChat with PRE analysis:', error);
			// Simple fallback - just show the analysis
			if (this._preLoopAnalysis) {
				this.streamResponseContent(this._preLoopAnalysis + '\n\nError: Could not complete request.', this._currentMessageId || 'error');
			}
		}
	}

	private async handlePRELoopOutput(result: any, messageId: string, originalPrompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 Analyzing PRE loop output for code changes...', result);
			
			// First check approved artifacts (critic-approved changes)
			if (result.approved_artifacts && Array.isArray(result.approved_artifacts) && result.approved_artifacts.length > 0) {
				for (const artifact of result.approved_artifacts) {
					if (this.shouldCreateCodeEditFromContent_DEPRECATED(artifact, originalPrompt, contextFiles)) {
						await this.createCodeEditFromArtifact(artifact, messageId, originalPrompt, contextFiles);
						return;
					}
				}
				// If approved artifacts don't need code edits, stream as response
				this.streamResponseContent(result.approved_artifacts.join('\n\n'), messageId);
				return;
			}

			// CRITICAL FIX: If no approved artifacts, check step outputs for proposed changes
			// This handles the case where critic rejected changes but we still want to propose them
			if (result.steps && Array.isArray(result.steps)) {
				console.log('🧠 No approved artifacts, checking step outputs for proposed changes...');
				
				// For removal tasks, always generate the code edit directly
				if (originalPrompt.toLowerCase().includes('remove') && contextFiles && contextFiles.length > 0) {
					console.log('🧠 Detected removal task - generating code edit directly');
					await this.createRemovalCodeEdit(originalPrompt, messageId, contextFiles);
					return;
				}
				
				for (const step of result.steps) {
					if (step.output && this.shouldCreateCodeEditFromContent_DEPRECATED(step.output, originalPrompt, contextFiles)) {
						console.log('🧠 Found proposed code change in step output:', step.title);
						
						// Create code edit from step output (even if critic rejected it)
						await this.createCodeEditFromStepOutput(step, messageId, originalPrompt, contextFiles);
						return;
					}
				}

				// If steps exist but no code changes, show what was attempted
				const stepSummary = result.steps.map((s: any) => 
					`• ${s.status === 'completed' ? '✅' : s.status === 'failed' ? '❌' : '⏳'} ${s.title}`
				).join('\n');
				
				this.streamResponseContent(
					`PRE loop completed ${result.steps.length} reasoning steps:\n\n${stepSummary}\n\n` +
					`${result.steps.filter((s: any) => s.status === 'failed').length > 0 ? 
						'Some steps were rejected by the critic. You may need to refine the request.' : 
						'All steps completed successfully.'}`
					, messageId);
				return;
			}

			// Fallback if no steps or artifacts
			this.streamResponseContent('PRE loop completed - no artifacts or steps generated.', messageId);

		} catch (error) {
			console.error('🧠 Error handling PRE loop output:', error);
			this.streamResponseContent('PRE loop execution completed.', messageId);
		}
	}

	// DEPRECATED: Hardwired code detection - should use MCP analyze_code_structure
	private looksLikeCode_DEPRECATED(content: string): boolean {
		if (!content || typeof content !== 'string') {
			return false;
		}
		
		const codeIndicators = [
			'def ', 'class ', 'import ', 'from ', 'function ', 'const ', 'let ', 'var ',
			'{', '}', '(', ')', 'if (', 'for (', 'while (', 'return ', 'print(',
			'#include', 'public class', 'private ', 'async ', 'await '
		];
		return codeIndicators.some(indicator => content.includes(indicator));
	}

	// DEPRECATED: Hardwired code edit detection - should use MCP analyze_request_intent
	private shouldCreateCodeEdit_DEPRECATED(content: string, prompt: string): boolean {
		// Create code edit if the prompt suggests file modification
		const editKeywords = ['edit', 'modify', 'change', 'update', 'fix', 'add to', 'remove from', 'implement in'];
		return editKeywords.some(keyword => prompt.toLowerCase().includes(keyword)) && this.looksLikeCode_DEPRECATED(content);
	}

	// DEPRECATED: Hardwired code edit detection - should use MCP analyze_request_intent
	private shouldCreateCodeEditFromContent_DEPRECATED(content: string, prompt: string, contextFiles?: any[]): boolean {
		// Enhanced logic to detect when content should become a code edit
		
		if (!content || !prompt) {
			return false;
		}
		
		// 1. Check if prompt suggests file modification
		const editKeywords = ['remove', 'delete', 'add', 'modify', 'change', 'update', 'fix', 'implement', 'insert'];
		const hasEditIntent = editKeywords.some(keyword => prompt.toLowerCase().includes(keyword));
		
		// 2. Check if we have context files (target for changes)
		const hasTargetFiles = contextFiles && contextFiles.length > 0;
		
		// 3. Check if content looks like code or file content
		const looksLikeFileContent = this.looksLikeCode_DEPRECATED(content) || 
			content.includes('#!/') || // Shell scripts
			content.includes('echo ') || // Shell commands
			content.includes('cd ') || // Directory changes
			content.length > 50; // Substantial content
		
		// 4. Special case: For "remove print statements" - always create code edit
		const isRemovalTask = prompt.toLowerCase().includes('remove') && hasTargetFiles;
		
		console.log('🧠 Code edit detection:', { hasEditIntent, hasTargetFiles, looksLikeFileContent, isRemovalTask });
		
		return Boolean((hasEditIntent && hasTargetFiles && looksLikeFileContent) || isRemovalTask);
	}

	private async createCodeEditFromStepOutput(step: any, messageId: string, prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 Creating code edit from step output:', step.title);
			
			// Determine target file
			let targetFile = '';
			let originalContent = '';

			// Try to get target file from context files first
			if (contextFiles && contextFiles.length > 0) {
				targetFile = contextFiles[0].uri;
				originalContent = contextFiles[0].content || '';
			} else {
				// Fallback to active editor
				const activeEditor = vscode.window.activeTextEditor;
				if (activeEditor) {
					targetFile = activeEditor.document.uri.fsPath;
					originalContent = activeEditor.document.getText();
				}
			}

			if (!targetFile) {
				console.log('🧠 No target file found, streaming as text response');
				this.streamResponseContent(step.output, messageId);
				return;
			}

			// For removal tasks, generate the modified content
			let newContent = step.output;
			
			// Note: Print statement removal is now handled by PRE loop tool orchestration
			// The PRE loop will automatically detect removal tasks and use appropriate MCP tools
			if (prompt.toLowerCase().includes('remove') && prompt.toLowerCase().includes('print')) {
				console.log('🧠 Print statement removal will be handled by PRE loop tool orchestration');
				// Let the PRE loop handle this through its tool selection logic
			}

			// Create code edit with clear description
			const codeEdit = {
				filePath: targetFile,
				originalContent: originalContent,
				newCode: newContent, // Fix: Use newCode instead of newContent
				description: `PRE Loop Step: ${step.title} (${step.status === 'failed' ? 'Critic Rejected - Review Needed' : 'Approved'})`,
				language: this.getLanguageFromPath(targetFile)
			};

			// Create snapshot before showing code edit (matching simple mode behavior)
			await this.createSnapshotForCodeEdit(messageId, `PRE Loop Step: ${step.title}`);

			// Update message with code edit UI, preserving analysis content
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: messageId,
					codeEdit: codeEdit,
					streamingPhase: 'complete',
					preserveAnalysis: true, // Keep the analysis content visible
					isFinished: true // Mark as finished to trigger snapshot
				}
			});

		} catch (error) {
			console.error('🧠 Error creating code edit from step:', error);
			this.streamResponseContent(step.output || 'Step completed', messageId);
		}
	}

	private async createSnapshotForCodeEdit(messageId: string, description: string): Promise<void> {
		try {
			console.log('🔖 Creating snapshot for PRE loop code edit:', description);
			
			const snapshotManager = getSnapshotManager();
			const snapshotId = await snapshotManager.createSnapshot(
				`${description} - ${Date.now()}`,
				description
			);
			
			console.log('✅ Created PRE loop snapshot:', snapshotId);
			
			// Notify webview about snapshot creation (matching simple mode pattern)
			this._view?.webview.postMessage({
				type: 'snapshotCreated',
					messageId: messageId,
				snapshotId: snapshotId,
				description: description
			});
			
		} catch (error) {
			console.error('❌ Failed to create PRE loop snapshot:', error);
		}
	}

	private async createRemovalCodeEdit(prompt: string, messageId: string, contextFiles: any[]): Promise<void> {
		try {
			console.log('🧠 Creating removal code edit for prompt:', prompt);
			
			// Get the target file from context
			const targetFile = contextFiles[0];
			if (!targetFile || !targetFile.content) {
				console.error('🧠 No valid target file for removal task');
				this.streamResponseContent('No valid file found for removal task.', messageId);
				return;
			}

			const originalContent = targetFile.content;
			let newContent = originalContent;

			// Use the exact same pattern as simple mode: delegate to ChatServiceImpl
			console.log('🧠 Using ChatServiceImpl streamEditDirect for proper workflow...');
			
			// Get the chat service (same as simple mode)
			const chatService = sharedChatServiceImpl();
			
			// Use streamEditDirect which handles snapshots and code edits correctly
			await (chatService as any).streamEditDirect(prompt, targetFile.uri, undefined);
			
			console.log('✅ PRE loop delegated to working streamEditDirect');

		} catch (error) {
			console.error('🧠 Error creating removal code edit:', error);
			this.streamResponseContent('Error processing removal request.', messageId);
		}
	}

	// DEPRECATED: Hardwired comment removal - should use dynamic MCP tools
	private removeComments_DEPRECATED(content: string): string {
		if (!content) return '';
		const lines = content.split('\n');
		return lines.filter(line => !line.trim().startsWith('#')).join('\n');
	}

	// DEPRECATED: Hardwired debug statement removal - should use dynamic MCP tools
	private removeDebugStatements_DEPRECATED(content: string): string {
		if (!content) return '';
		const lines = content.split('\n');
		return lines.filter(line => {
			const trimmed = line.trim().toLowerCase();
			return !trimmed.includes('console.log') && 
				   !trimmed.includes('print(') && 
				   !trimmed.includes('debug') &&
				   !trimmed.includes('logger.');
		}).join('\n');
	}

	// DEPRECATED: Hardwired term extraction - should use MCP analyze_request_intent
	private extractRemovalTerms_DEPRECATED(prompt: string): string[] {
		// Extract what should be removed from the prompt
		const words = prompt.toLowerCase().split(' ');
		const removeIndex = words.indexOf('remove');
		if (removeIndex >= 0 && removeIndex < words.length - 1) {
			return words.slice(removeIndex + 1, removeIndex + 3); // Get next 1-2 words
		}
		return [];
	}

	// DEPRECATED: Hardwired term removal - should use dynamic MCP tools
	private removeGenericTerms_DEPRECATED(content: string, terms: string[]): string {
		if (!content || terms.length === 0) return content;
		
		let result = content;
		for (const term of terms) {
			// Remove lines containing the term
			const lines = result.split('\n');
			result = lines.filter(line => !line.toLowerCase().includes(term)).join('\n');
		}
		return result;
	}

	private async removePrintStatementsDynamic(content: string, filePath?: string): Promise<string> {
		if (!content || typeof content !== 'string') {
			return '';
		}
		
		console.log('🧠 Removing print statements dynamically using MCP...');
		
		try {
			const mcpClient = await getInitializedMCPClient();
			const languageHint = this.getLanguageFromPath(filePath || '');
			
			const result = await mcpClient.removePrintStatementsDynamic(
				content,
				filePath || '',
				languageHint
			);
			
			if (result.success && result.changes_made) {
				console.log('🧠 Dynamic removal successful:', {
					language: result.language,
					originalLines: content.split('\n').length,
					modifiedLines: result.modified_content.split('\n').length
				});
				return result.modified_content;
			} else if (result.success) {
				console.log('🧠 No print statements found to remove');
				return content;
			} else {
				console.warn('🧠 Dynamic removal failed, falling back to content:', result.error);
				return content;
			}
		} catch (error) {
			console.error('🧠 Error in dynamic print statement removal:', error);
			// Fallback to original content if MCP fails
			return content;
		}
	}

	// DEPRECATED: Hardwired print statement removal - replaced by removePrintStatementsDynamic
	private removePrintStatements_DEPRECATED(content: string, _filePath?: string): string {
		if (!content || typeof content !== 'string') {
			return '';
		}
		
		console.log('🧠 Using fallback print statement removal...');
		
		// Simple fallback - remove obvious print patterns
		const lines = content.split('\n');
		const filteredLines = lines.filter(line => {
			const trimmed = line.trim();
			
			// Skip empty lines and comments
			if (!trimmed || trimmed.startsWith('#')) {
				return true;
			}
			
			// Basic print pattern detection
			const hasPrint = 
				trimmed.includes('print(') || 
				trimmed.startsWith('print ') ||
				trimmed.includes('console.log(') ||
				(trimmed.startsWith('echo ') && !trimmed.startsWith('echo "#!/'));
			
			if (hasPrint) {
				console.log('🧠 Fallback removing line:', trimmed);
			}
			
			return !hasPrint;
		});
		
		return filteredLines.join('\n');
	}

	// NOTE: This method is still used but should eventually call MCP analyze_language_features
	// Keeping for now as a fast fallback for file extension hints
	private getLanguageFromPath(filePath: string): string {
		if (!filePath || typeof filePath !== 'string') {
			return 'plaintext';
		}
		
		const ext = filePath.split('.').pop()?.toLowerCase();
		const languageMap: { [key: string]: string } = {
			'py': 'python',
			'js': 'javascript',
			'ts': 'typescript',
			'sh': 'bash',
			'bash': 'bash',
			'zsh': 'zsh',
			'java': 'java',
			'cpp': 'cpp',
			'c': 'c',
			'go': 'go',
			'rs': 'rust'
		};
		return languageMap[ext || ''] || 'plaintext';
	}

	// DEPRECATED: Hardwired file content detection - should use MCP analyze_code_structure  
	private looksLikeNewFileContent_DEPRECATED(content: string): boolean {
		// Check if this looks like complete file content (has imports, multiple functions, etc.)
		return content.includes('import ') && (content.match(/def |function /g) || []).length > 1;
	}

	private async createCodeEditFromArtifact(artifact: string, messageId: string, prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			// Determine target file from context or active editor
			let targetFile = '';
			let originalContent = '';

			const activeEditor = vscode.window.activeTextEditor;
			if (activeEditor) {
				targetFile = activeEditor.document.uri.fsPath;
				originalContent = activeEditor.document.getText();
			} else if (contextFiles && contextFiles.length > 0) {
				targetFile = contextFiles[0].uri;
				originalContent = contextFiles[0].content || '';
			} else {
				// No target file - just stream as response
				this.streamResponseContent(artifact, messageId);
				return;
			}

			// Create code edit UI
			const codeEdit = {
				filePath: targetFile,
				originalContent: originalContent,
				newCode: artifact, // Fix: Use newCode instead of newContent
				description: `PRE Loop Generated: ${prompt}`,
				language: vscode.window.activeTextEditor?.document.languageId || 'python'
			};

			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: messageId,
					codeEdit: codeEdit,
					streamingPhase: 'complete'
				}
			});

		} catch (error) {
			console.error('🧠 Error creating code edit from artifact:', error);
			this.streamResponseContent(artifact, messageId);
		}
	}

	private async handlePRELoopEdit(prompt: string, filePath: string, selection?: any, contextFiles?: any[], skipUserMessage: boolean = false): Promise<void> {
		try {
			console.log('🧠 ChatProvider: Starting PRE loop edit with theoretical framework');
			
			const mcpClient = await getInitializedMCPClient();
			
			// Read current file content
			let currentFileContent = '';
			try {
				const fileUri = vscode.Uri.file(filePath);
				const fileBytes = await vscode.workspace.fs.readFile(fileUri);
				currentFileContent = Buffer.from(fileBytes).toString('utf8');
			} catch (error) {
				console.warn('🔷 Could not read current file for PRE loop:', error);
			}

			const languageId = vscode.window.activeTextEditor?.document.languageId || 'python';
			const workspacePath = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || '';

			// Add user message if not skipped
			const chatService = sharedChatServiceImpl();
			if (!skipUserMessage) {
				// CRITICAL FIX: Don't create duplicate user message to avoid override issues
				console.log('🧠 CRITICAL: handlePRELoopEdit - NOT creating user message to avoid duplicates');
				// await chatService.addMessage(`Edit with PRE Loop: ${prompt}`, false); // DISABLED
			}

			// Start assistant response with PRE loop analysis
			const assistantMessageId = await chatService.addMessage('', true);
			this._currentMessageId = assistantMessageId;

			// Show initial analysis
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					analysisContent: '### 🧠 PRE Loop File Edit Analysis\n\nInitializing event-driven planning for file modifications...\n',
					streamingPhase: 'analysis'
				}
			});

			// Execute PRE loop for the edit
			const editInstruction = `Edit file ${filePath}: ${prompt}\n\nCurrent file content:\n${currentFileContent}`;
			const result = await mcpClient.planExecute(
				editInstruction,
				currentFileContent,
				languageId,
				workspacePath,
				contextFiles
			);

			// Display PRE loop analysis
			let analysisContent = '### 🧠 PRE Loop File Edit Analysis\n\n';
			analysisContent += `**Target File:** \`${filePath}\`\n`;
			
			if (result.events_fired) {
				analysisContent += `**Events Fired:** ${result.events_fired}\n`;
			}

			if (result.steps && Array.isArray(result.steps)) {
				analysisContent += '\n**Execution Steps:**\n';
				result.steps.forEach((step: any, idx: number) => {
					const statusIcon = step.status === 'completed' ? '✅' : step.status === 'failed' ? '❌' : '⏳';
					const probability = step.successProbability ? ` (p=${step.successProbability.toFixed(2)})` : '';
					analysisContent += `${idx + 1}. ${statusIcon} **${step.title}**${probability}\n`;
					if (step.description) {
						analysisContent += `   ${step.description}\n`;
					}
					analysisContent += '\n';
				});
			}

			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					analysisContent: analysisContent,
					streamingPhase: 'response'
				}
			});

			// Extract code from approved artifacts and apply to file
			// NOTE: Artifact detection kept minimal - PRE loop tools handle language-specific logic
			let finalCode = '';
			if (result.approved_artifacts && Array.isArray(result.approved_artifacts) && result.approved_artifacts.length > 0) {
				// Simply use all approved artifacts (they're already validated by PRE loop critic)
				finalCode = result.approved_artifacts.join('\n\n');
				console.log('🧠 Using approved artifacts from PRE loop:', result.approved_artifacts.length);
			} else {
				// Fallback to simple edit if no artifacts
				console.log('🧠 No approved artifacts, falling back to simple edit');
				finalCode = await mcpClient.streamEdit(currentFileContent, prompt, filePath, languageId);
			}

			// Apply the edit with code edit UI
			if (finalCode && finalCode.trim() !== currentFileContent.trim()) {
				// Create code edit message
				const codeEdit = {
					filePath: filePath,
					originalContent: currentFileContent,
					newContent: finalCode,
					description: `PRE Loop Edit: ${prompt}`,
					language: languageId
				};

				// Update message with code edit
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: assistantMessageId,
						codeEdit: codeEdit,
						streamingPhase: 'complete'
					}
				});
			} else {
				// No changes or same content
				this.streamResponseContent('No changes were needed or generated.', assistantMessageId);
			}

		} catch (error) {
			console.error('🧠 ChatProvider: Error in PRE loop edit:', error);
			// Fallback to simple edit
			const chatService = sharedChatServiceImpl();
			await chatService.streamEdit(prompt, filePath, selection);
		}
	}

	private async handleInteractivePRELoop(prompt: string, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 ChatProvider: Starting interactive PRE loop execution');
			
			const mcpClient = await getInitializedMCPClient();
			const activeDoc = vscode.window.activeTextEditor?.document;
			const currentFileContent = activeDoc ? activeDoc.getText() : undefined;
			const languageId = activeDoc?.languageId || 'python';
			const workspacePath = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || '';

			// Generate plan first
			const plan = await mcpClient.generatePlanSteps(prompt, currentFileContent, 6);
			
			if (!plan || !plan.steps || !Array.isArray(plan.steps)) {
				throw new Error('Failed to generate valid plan');
			}

			// Add user message
			const chatService = sharedChatServiceImpl();
			// CRITICAL FIX: Don't create duplicate user message to avoid override issues
			console.log('🧠 CRITICAL: handleInteractivePRELoop - NOT creating user message to avoid duplicates');
			// const userMessageId = await chatService.addMessage(`Interactive PRE: ${prompt}`, false); // DISABLED
			const assistantMessageId = await chatService.addMessage('', true);
			this._currentMessageId = assistantMessageId;

			// Show plan overview
			let analysisContent = '### 🧠 Interactive PRE Loop Execution\n\n';
			analysisContent += `**Generated Plan (${plan.steps.length} steps):**\n`;
			plan.steps.forEach((step: any, idx: number) => {
				analysisContent += `${idx + 1}. ⏳ **${step.title}**\n`;
				if (step.description) {
					analysisContent += `   ${step.description}\n`;
				}
				analysisContent += '\n';
			});

			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					analysisContent: analysisContent,
					streamingPhase: 'analysis'
				}
			});

			// Execute steps one by one
			let approvedArtifacts: string[] = [];
			
			for (let stepIndex = 0; stepIndex < plan.steps.length; stepIndex++) {
				const stepResult = await mcpClient.streamPlanExecute(
					prompt,
					currentFileContent,
					languageId,
					workspacePath,
					contextFiles,
					plan.plan_id,
					stepIndex
				);

				// Update analysis content with step progress
				const step = stepResult.current_step;
				if (step) {
					const statusIcon = step.status === 'completed' ? '✅' : step.status === 'failed' ? '❌' : '🔄';
					const probability = step.successProbability ? ` (p=${step.successProbability.toFixed(2)})` : '';
					
					// Update the step status in the analysis
					const stepLine = `${stepIndex + 1}. ${statusIcon} **${step.title}**${probability}`;
					analysisContent = analysisContent.replace(
						new RegExp(`${stepIndex + 1}\\. ⏳ \\*\\*${step.title.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\*\\*`),
						stepLine
					);

					if (step.output && step.status === 'completed') {
						analysisContent += `\n   ✅ **Output:** ${step.output.substring(0, 200)}${step.output.length > 200 ? '...' : ''}\n`;
						approvedArtifacts.push(step.output);
					}
					
					if (step.error && step.status === 'failed') {
						analysisContent += `\n   ❌ **Error:** ${step.error}\n`;
					}

					// Update UI
					this._view?.webview.postMessage({
						type: 'messageChange',
						message: {
							id: assistantMessageId,
							analysisContent: analysisContent,
							streamingPhase: 'analysis'
						}
					});

					// Small delay for visual feedback
					await new Promise(resolve => setTimeout(resolve, 500));
				}

				if (stepResult.execution_complete) {
					break;
				}
			}

			// Generate final response from approved artifacts
			const finalResponse = approvedArtifacts.length > 0 
				? approvedArtifacts.join('\n\n')
				: await mcpClient.streamGenerate(prompt, currentFileContent, languageId, workspacePath);

			// Show completion
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: assistantMessageId,
					streamingPhase: 'response'
				}
			});

			this.streamResponseContent(finalResponse, assistantMessageId);

		} catch (error) {
			console.error('🧠 ChatProvider: Error in interactive PRE loop:', error);
			// Fallback to simple generation
			const chatService = sharedChatServiceImpl();
			await chatService.streamGenerate(prompt, contextFiles);
		}
	}

	private async handleStreamCreate(prompt: string, contextFiles?: any[]): Promise<void> {
		// 🧠 UNIFIED APPROACH: Use handleUniversalPRELoop for consistency
		console.log('🧠 ChatProvider: handleStreamCreate routing to unified handler');
		await this.handleUniversalPRELoop(prompt, contextFiles);
	}

	private async streamChatResponseRealTime(prompt: string, contextFiles: any[], messageId: string): Promise<void> {
		try {
			console.log('🚀 Starting real-time streaming for prompt:', prompt.substring(0, 100));
			
			// Use the same real streaming approach as ChatServiceImpl
			const payload = {
				prompt: prompt,
				contextFiles: contextFiles || [],
				conversationId: 'default',
				messageId: messageId
			};

			// Make direct HTTP request to get real streaming
			const response = await fetch('http://localhost:3000/mcp', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify({
					jsonrpc: "2.0",
					method: "tools/call",
					params: {
						name: "stream_chat",
						arguments: payload
					},
					id: Date.now()
				})
			});

			if (!response.ok) {
				throw new Error(`HTTP error! status: ${response.status}`);
			}

			// Handle real streaming response
			const reader = response.body?.getReader();
			if (!reader) {
				throw new Error('No response body reader available');
			}

			const decoder = new TextDecoder();
			let buffer = '';
			let accumulatedContent = '';

			while (true) {
				const { done, value } = await reader.read();
				if (done) break;

				const chunk = decoder.decode(value, { stream: true });
				buffer += chunk;

				// Process SSE events as they come in
				let sepIndex: number;
				while ((sepIndex = buffer.indexOf('\n\n')) !== -1) {
					const eventBlock = buffer.slice(0, sepIndex);
					buffer = buffer.slice(sepIndex + 2);

					const dataLines = eventBlock
						.split('\n')
						.filter(l => l.trim().startsWith('data:'))
						.map(l => l.replace(/^data:\s?/, ''));
					if (dataLines.length === 0) continue;

					const jsonStr = dataLines.join('');
					try {
						const data = JSON.parse(jsonStr);

						// Real-time content streaming
						if (data.content) {
							accumulatedContent += data.content;
							this._view?.webview.postMessage({
								type: 'messageChange',
								message: {
									id: messageId,
									content: accumulatedContent,
									isFinished: false,
									streamingPhase: 'response'
								}
							});
						}
					} catch (e) {
						// ignore malformed partials
					}
				}
			}

			// Finalize the message
			console.log('🏁 Finalizing streaming for message ID:', messageId);
			this._view?.webview.postMessage({
				type: 'messageChange',
				message: {
					id: messageId,
					content: accumulatedContent,
					isFinished: true,
					streamingPhase: 'complete'
				}
			});

			console.log('✅ Real-time streaming completed - isFinished: true sent');
			
			// CRITICAL: Also send a global streaming completion message to ensure webview clears streaming state
			this._view?.webview.postMessage({
				type: 'streamingComplete',
				messageId: messageId
			});

		} catch (error) {
			console.error('❌ Real-time streaming failed:', error);
			// Fallback to MCP client and simulated streaming
			const mcpClient = await getInitializedMCPClient();
			const fallbackResponse = await mcpClient.streamChat(prompt, contextFiles);
			this.streamResponseContent(fallbackResponse, messageId);
		}
	}

	private streamResponseContent(content: string, messageId: string): void {
		// Simulate streaming by breaking content into chunks (fallback only)
		console.log('⚠️ Using simulated streaming fallback for message ID:', messageId);
		const chunks = content.match(/.{1,50}/g) || [content];
		let currentContent = '';
		
		chunks.forEach((chunk, index) => {
			setTimeout(() => {
				currentContent += chunk;
				const isLastChunk = index === chunks.length - 1;
				
				this._view?.webview.postMessage({
					type: 'messageChange',
					message: {
						id: messageId,
						content: currentContent,
						isFinished: isLastChunk,  // ✅ Ensure isFinished is set on last chunk
						streamingPhase: isLastChunk ? 'complete' : 'response'
					}
				});
				
				if (isLastChunk) {
					console.log('🏁 Simulated streaming completed - isFinished: true sent for message:', messageId);
					
					// CRITICAL: Also send global streaming completion
					setTimeout(() => {
						this._view?.webview.postMessage({
							type: 'streamingComplete',
							messageId: messageId
						});
					}, 100); // Small delay after last chunk
				}
			}, index * 50); // 50ms delay between chunks
		});
	}

	private async handleApplyCodeEdit(messageId: string, codeEdit: any): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Applying code edit:', codeEdit);

			if (!codeEdit || !codeEdit.filePath || !codeEdit.newCode) {
				throw new Error('Invalid code edit data');
			}

			// Always resolve the target path to be inside the first workspace folder
			const workspaceFolders = vscode.workspace.workspaceFolders;
			if (!workspaceFolders || workspaceFolders.length === 0) {
				throw new Error('Cannot resolve file path - no workspace folder found');
			}
			const workspaceRoot = workspaceFolders[0].uri.fsPath;

			// Normalize incoming filePath to an fsPath string
			let requestedPathRaw: string = codeEdit.filePath;
			try {
				if (requestedPathRaw.startsWith('file://')) {
					requestedPathRaw = vscode.Uri.parse(requestedPathRaw).fsPath;
				}
			} catch { }

			// Sanitize and force into workspace: remove any drive/root prefix and leading slashes
			const stripRoot = (p: string) => p.replace(/^([A-Za-z]:)?[\\/]+/, '');
			let finalRelative: string;
			if (requestedPathRaw.startsWith(workspaceRoot)) {
				finalRelative = path.relative(workspaceRoot, requestedPathRaw);
			} else if (path.isAbsolute(requestedPathRaw)) {
				finalRelative = stripRoot(requestedPathRaw);
			} else {
				finalRelative = stripRoot(requestedPathRaw);
			}

			// Normalize relative path tokens like ./ and ../
			finalRelative = path.normalize(finalRelative).replace(/^\.\//, '');
			if (finalRelative.startsWith('..')) {
				// Prevent escaping workspace
				finalRelative = finalRelative.replace(/^\.\.[\\/]+/, '');
			}

			const fileUri = vscode.Uri.file(path.join(workspaceRoot, finalRelative));
			console.log('🔷 ChatProvider: Final file URI (workspace-scoped):', fileUri.toString());
			console.log('🔷 ChatProvider: Final file path (workspace-scoped):', fileUri.fsPath);

			// Try to read file; if not exists, create it (and parent dirs) then write new content
			let fileExists = true;
			try {
				await vscode.workspace.fs.stat(fileUri);
			} catch {
				fileExists = false;
			}

			if (!fileExists) {
				console.log('🔷 ChatProvider: Target file does not exist, creating:', fileUri.fsPath);
				// Ensure parent directory exists
				const parentDir = path.dirname(fileUri.fsPath);
				try {
					await vscode.workspace.fs.createDirectory(vscode.Uri.file(parentDir));
				} catch (dirErr) {
					console.warn('🔷 ChatProvider: createDirectory warning:', dirErr);
				}
				// Write new file content
				await vscode.workspace.fs.writeFile(fileUri, Buffer.from(codeEdit.newCode, 'utf8'));

				// Open the file to show the newly created content
				const document = await vscode.workspace.openTextDocument(fileUri);
				await vscode.window.showTextDocument(document);

				// Refresh context
				await this.refreshContextFileAfterEdit(fileUri);

				this.sendToWebview({
					type: 'codeEditApplied',
					messageId: messageId,
					success: true
				});
				return;
			}

			// Existing file: apply a replace edit across the whole file
			const edit = new vscode.WorkspaceEdit();

			try {
				const fileBytes = await vscode.workspace.fs.readFile(fileUri);
				const currentContent = Buffer.from(fileBytes).toString('utf8');
				console.log('🔷 ChatProvider: Successfully read file content:', currentContent.length, 'characters');

				const lines = currentContent.split('\n');
				const startPosition = new vscode.Position(0, 0);
				const endPosition = new vscode.Position(lines.length - 1, lines[lines.length - 1].length);
				const fullRange = new vscode.Range(startPosition, endPosition);

				edit.replace(fileUri, fullRange, codeEdit.newCode);

				const success = await vscode.workspace.applyEdit(edit);
				if (success) {
					console.log('🔷 ChatProvider: Code edit applied successfully');
					vscode.window.showInformationMessage(`Code changes applied to ${path.basename(fileUri.fsPath)}`);

					const document = await vscode.workspace.openTextDocument(fileUri);
					await vscode.window.showTextDocument(document);

					await this.refreshContextFileAfterEdit(fileUri);

					this.sendToWebview({
						type: 'codeEditApplied',
						messageId: messageId,
						success: true
					});
				} else {
					throw new Error('Failed to apply workspace edit');
				}

			} catch (fileError) {
				console.error('🔷 ChatProvider: Error applying file edit:', fileError);
				throw new Error(`Cannot apply edit: ${fileError instanceof Error ? fileError.message : 'Unknown error'}`);
			}

		} catch (error) {
			console.error('🔷 ChatProvider: Error applying code edit:', error);
			vscode.window.showErrorMessage(`Failed to apply code changes: ${error instanceof Error ? error.message : 'Unknown error'}`);

			this.sendToWebview({
				type: 'codeEditApplied',
				messageId: messageId,
				success: false,
				error: error instanceof Error ? error.message : 'Unknown error'
			});
		}
	}

	private async handleRejectCodeEdit(messageId: string): Promise<void> {
		console.log('🔷 ChatProvider: Rejecting code edit for message:', messageId);

		// Notify webview that the edit was rejected
		this.sendToWebview({
			type: 'codeEditRejected',
			messageId: messageId
		});

		vscode.window.showInformationMessage('Code changes rejected');
	}

	private async handleRestoreFileSnapshots(snapshotId: string): Promise<void> {
		console.log('🔷 ChatProvider: Handling restoreFileSnapshots for snapshot:', snapshotId);
		console.log('🔷 ChatProvider: Starting restoration process');
		console.log('🔷 ChatProvider: Getting chat service instance');

		try {
			const chatService = sharedChatServiceImpl();
			console.log('🔷 ChatProvider: Got chat service instance:', !!chatService);

			console.log('🔷 ChatProvider: Calling chatService.restoreCheckpoint with snapshotId:', snapshotId);
			await chatService.restoreCheckpoint(snapshotId);
			console.log('🔷 ChatProvider: chatService.restoreCheckpoint completed successfully');
		} catch (error) {
			console.error('🔷 ChatProvider: Error in handleRestoreFileSnapshots:', error);
			console.error('🔷 ChatProvider: Error details:', error);
			throw error;
		}
	}

	// Implement the ChatServiceClient interface method
	handleCheckpointRestored(snapshotId: string): void {
		console.log('🔷 ChatProvider: Handling checkpointRestored for snapshot:', snapshotId);

		// Notify the webview about the checkpoint restoration
		if (this._view && this._view.webview) {
			this._view.webview.postMessage({
				type: 'checkpointRestored',
				snapshotId: snapshotId
			});
			console.log('🔷 ChatProvider: Sent checkpointRestored message to webview for:', snapshotId);
		}
	}

	// Handle targeted message clearing after checkpoint
	handleClearMessagesAfterCheckpoint(checkpointIdentifier: string): void {
		console.log('🔷 ChatProvider: *** IMPORTANT *** Handling clearMessagesAfterCheckpoint for identifier:', checkpointIdentifier);
		console.log('🔷 ChatProvider: _view exists:', !!this._view);
		console.log('🔷 ChatProvider: webview exists:', !!this._view?.webview);

		// Determine if this is a messageId or snapshotId
		const isSnapshotId = checkpointIdentifier.includes('Z'); // Snapshot IDs contain timestamp with Z
		console.log('🔷 ChatProvider: Identifier type:', isSnapshotId ? 'snapshotId' : 'messageId');

		// Notify the webview to clear messages after the specified checkpoint
		if (this._view && this._view.webview) {
			console.log('🔷 ChatProvider: *** SENDING *** clearMessagesAfterCheckpoint message to webview');
			this._view.webview.postMessage({
				type: 'clearMessagesAfterCheckpoint',
				checkpointMessageId: isSnapshotId ? undefined : checkpointIdentifier,
				checkpointSnapshotId: isSnapshotId ? checkpointIdentifier : undefined
			});
			console.log('🔷 ChatProvider: *** SENT *** clearMessagesAfterCheckpoint message to webview for:', checkpointIdentifier);
		} else {
			console.error('🔷 ChatProvider: *** ERROR *** Cannot send message - webview not available');
		}
	}

	// Method called by chatService when file snapshots are captured
	handleFileSnapshotsCaptured(data: { messageId: string, checkpointDescription: string, messageIndex: number, fileSnapshots: any[] }): void {
		console.log('🔷 ChatProvider: File snapshots captured, sending to webview:', data.fileSnapshots.length);
		this.sendToWebview({
			type: 'fileSnapshotsCaptured',
			...data
		});
	}

	// Method called by chatService when a snapshot is created for a message
	handleSnapshotCreated(messageId: string, snapshotId: string): void {
		console.log('🔷 ChatProvider: *** IMPORTANT *** Snapshot created for message:', messageId, '→', snapshotId);

		// Notify the ChatService about this checkpoint mapping to keep it in sync
		const chatService = sharedChatServiceImpl();
		if (chatService && 'addCheckpointMapping' in chatService) {
			console.log('🔷 ChatProvider: Adding checkpoint mapping to ChatService:', messageId, '→', snapshotId);
			(chatService as any).addCheckpointMapping(messageId, snapshotId);
		} else {
			console.log('🔷 ChatProvider: ChatService does not have addCheckpointMapping method');
		}

		this.sendToWebview({
			type: 'snapshotCreated',
			messageId,
			snapshotId
		});
	}

	private async handleExecuteCommand(command: string, args?: any[]): Promise<void> {
		console.log('🔷 ChatProvider: Executing VS Code command:', command, args);
		console.log('🔷 ChatProvider: handleExecuteCommand called');
		console.log('🔷 ChatProvider: command type:', typeof command);
		console.log('🔷 ChatProvider: args type:', typeof args);
		console.log('🔷 ChatProvider: args length:', args?.length);

		try {
			if (command === 'superinference.restoreSnapshot' && args && args[0]) {
				console.log('🔷 ChatProvider: Handling superinference.restoreSnapshot command');
				// Handle snapshot restoration directly
				const snapshotId = args[0];
				console.log('🔷 ChatProvider: Extracted snapshotId:', snapshotId);
				console.log('🔷 ChatProvider: Calling handleRestoreFileSnapshots with snapshotId:', snapshotId);

				await this.handleRestoreFileSnapshots(snapshotId);
				console.log('🔷 ChatProvider: handleRestoreFileSnapshots completed successfully');
			} else {
				console.log('🔷 ChatProvider: Executing standard VS Code command:', command);
				console.log('🔷 ChatProvider: With args:', args);

				await vscode.commands.executeCommand(command, ...(args || []));
				console.log('🔷 ChatProvider: Standard command execution completed');
			}
		} catch (error) {
			console.error('🔷 ChatProvider: Failed to execute command:', command, error);
			console.error('🔷 ChatProvider: Error details:', error);
		}
	}

	private async sendWorkspaceContext(): Promise<void> {
		try {
			const serviceManager = this._serviceManager;
			if (serviceManager) {
				const chatViewService = await serviceManager.getService(CHAT_VIEW_SERVICE_NAME) as IChatViewService;
				const context = await chatViewService.getWorkspaceContext();

				this._view?.webview.postMessage({
					type: 'workspaceContext',
					context
				});
			}
		} catch (error) {
			console.error('Error getting workspace context:', error);
		}
	}

	private async handleAddFileToContext(): Promise<void> {
		try {
			const editor = vscode.window.activeTextEditor;
			if (!editor) {
				vscode.window.showInformationMessage('No active editor to add to context');
				return;
			}

			const document = editor.document;
			const selection = editor.selection;
			const hasSelection = !selection.isEmpty;

			// Get file content (either selection or entire file)
			const content = hasSelection ? document.getText(selection) : document.getText();

			// Create context file object
			const contextFile = {
				uri: document.uri.toString(),
				name: document.fileName.split('/').pop() || 'Unknown',
				type: hasSelection ? 'selection' : 'file',
				content: content,
				range: hasSelection ? {
					start: { line: selection.start.line, character: selection.start.character },
					end: { line: selection.end.line, character: selection.end.character }
				} : undefined,
				language: document.languageId
			};

			// Send to webview
			this._view?.webview.postMessage({
				type: 'fileAdded',
				file: contextFile
			});

			const fileType = hasSelection ? 'selection' : 'file';
			const lines = hasSelection ? selection.end.line - selection.start.line + 1 : document.lineCount;
			vscode.window.showInformationMessage(`Added ${fileType} (${lines} lines) to context: ${contextFile.name}`);

		} catch (error) {
			console.error('Error adding file to context:', error);
			vscode.window.showErrorMessage('Failed to add file to context');
		}
	}

	private async handleShowFilePicker(): Promise<void> {
		try {
			const files = await vscode.window.showOpenDialog({
				canSelectFiles: true,
				canSelectFolders: false,
				canSelectMany: true,
				openLabel: 'Add to Context',
				filters: {
					'Code files': ['ts', 'js', 'tsx', 'jsx', 'py', 'java', 'cpp', 'c', 'cs', 'go', 'rs', 'php', 'rb', 'swift', 'kt'],
					'Text files': ['txt', 'md', 'json', 'xml', 'yaml', 'yml', 'toml', 'ini', 'cfg', 'conf'],
					'All files': ['*']
				}
			});

			if (files && files.length > 0) {
				const contextFiles = [];

				for (const fileUri of files) {
					try {
						const document = await vscode.workspace.openTextDocument(fileUri);
						const content = document.getText();

						// Skip very large files (>100KB)
						if (content.length > 100000) {
							vscode.window.showWarningMessage(`Skipping large file: ${fileUri.path.split('/').pop()}`);
							continue;
						}

						const contextFile = {
							uri: fileUri.toString(),
							name: fileUri.path.split('/').pop() || 'Unknown',
							type: 'file',
							content: content,
							language: document.languageId
						};

						contextFiles.push(contextFile);
					} catch (error) {
						console.error(`Error reading file ${fileUri.path}:`, error);
						vscode.window.showErrorMessage(`Failed to read file: ${fileUri.path.split('/').pop()}`);
					}
				}

				if (contextFiles.length > 0) {
					// Send to webview
					this._view?.webview.postMessage({
						type: 'filesAdded',
						files: contextFiles
					});

					vscode.window.showInformationMessage(`Added ${contextFiles.length} file${contextFiles.length > 1 ? 's' : ''} to context`);
				}
			}

		} catch (error) {
			console.error('Error showing file picker:', error);
			vscode.window.showErrorMessage('Failed to show file picker');
		}
	}

	private async handleAddAllOpenFiles(): Promise<void> {
		try {
			const openDocuments = vscode.workspace.textDocuments.filter(doc =>
				doc.uri.scheme === 'file' && // Only file scheme (not git, etc.)
				!doc.isUntitled // Skip untitled documents
			);

			if (openDocuments.length === 0) {
				vscode.window.showInformationMessage('No open files to add to context');
				return;
			}

			// Confirm with user if many files
			if (openDocuments.length > 10) {
				const result = await vscode.window.showWarningMessage(
					`Add all ${openDocuments.length} open files to context? This might be a lot of context.`,
					'Yes', 'No'
				);
				if (result !== 'Yes') {
					return;
				}
			}

			const contextFiles = [];

			for (const document of openDocuments) {
				try {
					const content = document.getText();

					// Skip very large files (>100KB)
					if (content.length > 100000) {
						console.log(`Skipping large file: ${document.fileName}`);
						continue;
					}

					const contextFile = {
						uri: document.uri.toString(),
						name: document.fileName.split('/').pop() || 'Unknown',
						type: 'file',
						content: content,
						language: document.languageId
					};

					contextFiles.push(contextFile);
				} catch (error) {
					console.error(`Error reading document ${document.fileName}:`, error);
				}
			}

			if (contextFiles.length > 0) {
				// Send to webview
				this._view?.webview.postMessage({
					type: 'filesAdded',
					files: contextFiles
				});

				vscode.window.showInformationMessage(`Added ${contextFiles.length} open file${contextFiles.length > 1 ? 's' : ''} to context`);
			} else {
				vscode.window.showInformationMessage('No suitable files found to add to context');
			}

		} catch (error) {
			console.error('Error adding all open files:', error);
			vscode.window.showErrorMessage('Failed to add open files to context');
		}
	}

	private async handleSearchFiles(query: string): Promise<void> {
		try {
			if (!query || query.trim().length === 0) {
				this._view?.webview.postMessage({
					type: 'searchResults',
					results: []
				});
				return;
			}

			const workspaceFolders = vscode.workspace.workspaceFolders;
			if (!workspaceFolders || workspaceFolders.length === 0) {
				this._view?.webview.postMessage({
					type: 'searchResults',
					results: []
				});
				return;
			}

			// Search for files using VS Code's file search API
			const searchPattern = `**/*${query}*`;
			const excludePattern = '{**/node_modules/**,**/out/**,**/dist/**,**/.git/**,**/target/**,**/build/**}';

			const files = await vscode.workspace.findFiles(
				searchPattern,
				excludePattern,
				50 // Limit to 50 results for performance
			);

			const searchResults = files.map(file => ({
				uri: file.toString(),
				name: file.path.split('/').pop() || 'Unknown',
				path: vscode.workspace.asRelativePath(file),
				type: 'file'
			}));

			// Sort by relevance (exact matches first, then by name)
			searchResults.sort((a, b) => {
				const aName = a.name.toLowerCase();
				const bName = b.name.toLowerCase();
				const queryLower = query.toLowerCase();

				// Exact matches first
				if (aName === queryLower && bName !== queryLower) return -1;
				if (bName === queryLower && aName !== queryLower) return 1;

				// Then by how early the match appears in the filename
				const aIndex = aName.indexOf(queryLower);
				const bIndex = bName.indexOf(queryLower);

				if (aIndex !== bIndex) {
					if (aIndex === -1) return 1;
					if (bIndex === -1) return -1;
					return aIndex - bIndex;
				}

				// Finally by filename
				return aName.localeCompare(bName);
			});

			this._view?.webview.postMessage({
				type: 'searchResults',
				results: searchResults
			});

		} catch (error) {
			console.error('Error searching files:', error);
			this._view?.webview.postMessage({
				type: 'searchResults',
				results: []
			});
		}
	}

	private async handleAddFileByUri(uri: string): Promise<void> {
		try {
			const fileUri = vscode.Uri.parse(uri);
			const document = await vscode.workspace.openTextDocument(fileUri);
			const content = document.getText();

			// Skip very large files (>100KB)
			if (content.length > 100000) {
				vscode.window.showWarningMessage(`File too large to add to context: ${fileUri.path.split('/').pop()}`);
				return;
			}

			const contextFile = {
				uri: fileUri.toString(),
				name: fileUri.path.split('/').pop() || 'Unknown',
				type: 'file',
				content: content,
				language: document.languageId
			};

			// Send to webview
			this._view?.webview.postMessage({
				type: 'fileAdded',
				file: contextFile
			});

			vscode.window.showInformationMessage(`Added file to context: ${contextFile.name}`);

		} catch (error) {
			console.error('Error adding file by URI:', error);
			vscode.window.showErrorMessage('Failed to add file to context');
		}
	}

	private async handleRefreshContextFiles(): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Refreshing all context files requested');
			// Just notify the webview that context should be refreshed
			// The actual refresh happens when the context files are sent with the request
			this.sendToWebview({
				type: 'contextRefreshRequested'
			});
		} catch (error) {
			console.error('🔷 ChatProvider: Error handling refresh context files:', error);
		}
	}

	// NEW: Embeddings handler methods
	private async handleGetEmbeddingsStatus(): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Getting embeddings status from backend');

			// Use vscode built-in fetch or fallback
			const https = require('https');
			const http = require('http');

			const fetchData = (url: string): Promise<any> => {
				return new Promise((resolve, reject) => {
					const client = url.startsWith('https:') ? https : http;
					client.get(url, (res: any) => {
						let data = '';
						res.on('data', (chunk: any) => data += chunk);
						res.on('end', () => {
							try {
								resolve({ ok: res.statusCode === 200, json: () => JSON.parse(data), status: res.statusCode });
							} catch (e) {
								reject(e);
							}
						});
					}).on('error', reject);
				});
			};

			// Fetch status from MCP server
			const mcpClient = await getInitializedMCPClient();
			const status = await mcpClient.getEmbeddingsStatus();
			console.log('🔷 ChatProvider: Embeddings status received:', status);

			// Send status to webview
			this.sendToWebview({
				type: 'embeddingsStatusUpdate',
				status: status
			});
		} catch (error) {
			console.error('🔷 ChatProvider: Error getting embeddings status:', error);

			// Send error status to webview
			this.sendToWebview({
				type: 'embeddingsStatusUpdate',
				status: {
					status: 'error',
					error: error instanceof Error ? error.message : 'Unknown error',
					vector_store: { total_entries: 0, entry_types: {} },
					smart_context_enabled: false
				}
			});
		}
	}

	private async handleIndexWorkspace(): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Starting workspace indexing');

			// Send indexing started message to webview
			this.sendToWebview({
				type: 'indexingProgress',
				progress: { current: 0, total: 100, currentFile: 'Starting...' }
			});

			// Execute the VS Code command to index workspace
			await vscode.commands.executeCommand('superinference.indexWorkspace');

			// Refresh status after indexing (with delay to allow completion)
			setTimeout(() => {
				this.handleGetEmbeddingsStatus();
			}, 2000);

		} catch (error) {
			console.error('🔷 ChatProvider: Error indexing workspace:', error);
			vscode.window.showErrorMessage(`Failed to index workspace: ${error}`);

			// Send indexing complete message
			this.sendToWebview({
				type: 'indexingComplete'
			});
		}
	}

	private async handleClearEmbeddings(): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Clearing embeddings from backend');

			const https = require('https');
			const http = require('http');

			const postData = (url: string, data: any): Promise<any> => {
				return new Promise((resolve, reject) => {
					const postDataString = JSON.stringify(data);
					const urlObj = new URL(url);
					const client = urlObj.protocol === 'https:' ? https : http;

					const options = {
						hostname: urlObj.hostname,
						port: urlObj.port,
						path: urlObj.pathname,
						method: 'POST',
						headers: {
							'Content-Type': 'application/json',
							'Content-Length': Buffer.byteLength(postDataString)
						}
					};

					const req = client.request(options, (res: any) => {
						let responseData = '';
						res.on('data', (chunk: any) => responseData += chunk);
						res.on('end', () => {
							try {
								resolve({ ok: res.statusCode === 200, json: () => JSON.parse(responseData), status: res.statusCode });
							} catch (e) {
								reject(e);
							}
						});
					});

					req.on('error', reject);
					req.write(postDataString);
					req.end();
				});
			};

			// Use MCP client to clear embeddings
			const mcpClient = await getInitializedMCPClient();
			const result = await mcpClient.clearEmbeddings();

			console.log('🔷 ChatProvider: Embeddings cleared:', result);
			vscode.window.showInformationMessage('🧠 Cleared all embeddings successfully!');

			// Refresh status to show the change
			setTimeout(() => {
				this.handleGetEmbeddingsStatus();
			}, 500);
		} catch (error) {
			console.error('🔷 ChatProvider: Error clearing embeddings:', error);
			vscode.window.showErrorMessage(`Failed to clear embeddings: ${error}`);
		}
	}

	private async handleReindexCurrentFile(): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Reindexing current file');
			// Execute the VS Code command to reindex current file
			await vscode.commands.executeCommand('superinference.forceReindexFile');

			// Refresh status after reindexing (with delay to allow completion)
			setTimeout(() => {
				this.handleGetEmbeddingsStatus();
			}, 1000);

		} catch (error) {
			console.error('🔷 ChatProvider: Error reindexing current file:', error);
			vscode.window.showErrorMessage(`Failed to reindex current file: ${error}`);
		}
	}

	private async handleUpdateEmbeddingsSettings(settings: any): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Updating embeddings settings:', settings);
			// For now, just log the settings. In a full implementation, 
			// you might want to store these in VS Code settings or send to the backend
			vscode.window.showInformationMessage('Embeddings settings updated');
		} catch (error) {
			console.error('🔷 ChatProvider: Error updating embeddings settings:', error);
			vscode.window.showErrorMessage(`Failed to update embeddings settings: ${error}`);
		}
	}

	private async handleUpdateExtensionSettings(settings: any): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Updating extension settings:', settings);

			// Update VS Code configuration for each setting
			const config = vscode.workspace.getConfiguration('superinference');

			// Backend Configuration
			await config.update('backend.host', settings.backendHost, vscode.ConfigurationTarget.Workspace);
			await config.update('backend.port', settings.backendPort, vscode.ConfigurationTarget.Workspace);
			await config.update('backend.protocol', settings.backendProtocol, vscode.ConfigurationTarget.Workspace);
			await config.update('backend.timeout', settings.backendTimeout, vscode.ConfigurationTarget.Workspace);
			await config.update('backend.retryAttempts', settings.backendRetryAttempts, vscode.ConfigurationTarget.Workspace);
			await config.update('backend.healthCheckInterval', settings.healthCheckInterval, vscode.ConfigurationTarget.Workspace);

			// AI Provider Configuration
			await config.update('ai.provider', settings.aiProvider, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.openai.apiKey', settings.openaiApiKey, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.openai.baseUrl', settings.openaiBaseUrl, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.openai.model', settings.openaiModel, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.openai.maxTokens', settings.openaiMaxTokens, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.gemini.apiKey', settings.geminiApiKey, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.gemini.model', settings.geminiModel, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.gemini.maxTokens', settings.geminiMaxTokens, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.anthropic.apiKey', settings.anthropicApiKey, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.anthropic.model', settings.anthropicModel, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.anthropic.maxTokens', settings.anthropicMaxTokens, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.local.endpoint', settings.localEndpoint, vscode.ConfigurationTarget.Workspace);
			await config.update('ai.local.model', settings.localModel, vscode.ConfigurationTarget.Workspace);

			// Embeddings Configuration
			await config.update('embeddings.provider', settings.embeddingsProvider, vscode.ConfigurationTarget.Workspace);
			await config.update('embeddings.model', settings.embeddingsModel, vscode.ConfigurationTarget.Workspace);
			await config.update('embeddings.dimensions', settings.embeddingsDimensions, vscode.ConfigurationTarget.Workspace);

			// API Endpoints
			await config.update('api.endpoints.chat', settings.chatEndpoint, vscode.ConfigurationTarget.Workspace);
			await config.update('api.endpoints.embeddings', settings.embeddingsEndpoint, vscode.ConfigurationTarget.Workspace);
			await config.update('api.endpoints.context', settings.contextEndpoint, vscode.ConfigurationTarget.Workspace);
			await config.update('api.endpoints.health', settings.healthEndpoint, vscode.ConfigurationTarget.Workspace);

			// Rate Limiting
			await config.update('api.rateLimit.requestsPerMinute', settings.requestsPerMinute, vscode.ConfigurationTarget.Workspace);
			await config.update('api.rateLimit.concurrentRequests', settings.concurrentRequests, vscode.ConfigurationTarget.Workspace);

			// Conversation Settings
			await config.update('conversation.autosave', settings.autosave, vscode.ConfigurationTarget.Workspace);
			await config.update('conversation.autorun', settings.autorun, vscode.ConfigurationTarget.Workspace);

			// Performance & Monitoring settings
			await config.update('enablePerformanceMonitoring', settings.enablePerformanceMonitoring, vscode.ConfigurationTarget.Workspace);
			await config.update('enableCircuitBreaker', settings.enableCircuitBreaker, vscode.ConfigurationTarget.Workspace);
			await config.update('maxCacheSize', settings.maxCacheSize, vscode.ConfigurationTarget.Workspace);
			await config.update('cacheTimeout', settings.cacheTimeout, vscode.ConfigurationTarget.Workspace);

			// AI & Context Management settings
			await config.update('enableAutoIndex', settings.enableAutoIndex, vscode.ConfigurationTarget.Workspace);
			await config.update('autoReindexOnSave', settings.autoReindexOnSave, vscode.ConfigurationTarget.Workspace);
			await config.update('enableSmartContext', settings.enableSmartContext, vscode.ConfigurationTarget.Workspace);
			await config.update('maxFileSize', settings.maxFileSize, vscode.ConfigurationTarget.Workspace);

			// Diagnostics & Debug settings
			await config.update('enableDiagnostics', settings.enableDiagnostics, vscode.ConfigurationTarget.Workspace);
			await config.update('debugMode', settings.debugMode, vscode.ConfigurationTarget.Workspace);

			console.log('✅ Extension settings updated successfully');
			vscode.window.showInformationMessage('🚀 Backend configuration and all extension settings updated successfully!');

			// Send confirmation message back to webview
			this._view?.webview.postMessage({
				type: 'extensionSettingsUpdated',
				success: true,
				settings: settings
			});

		} catch (error) {
			console.error('❌ Failed to update extension settings:', error);
			vscode.window.showErrorMessage(`Failed to update extension settings: ${error}`);

			// Send error message back to webview
			this._view?.webview.postMessage({
				type: 'extensionSettingsUpdated',
				success: false,
				error: error
			});
		}
	}

	// ChatServiceClient implementation
	handleReadyStateChange(isReady: boolean): void {
		this._view?.webview.postMessage({
			type: "readyStateChange",
			isReady: isReady,
		});
	}

	handleNewMessage(msg: MessageItemModel): void {
		this._view?.webview.postMessage({
			type: "newMessage",
			message: msg,
		});
	}

	handleMessageChange(msg: MessageItemModel): void {
		this._view?.webview.postMessage({
			type: "messageChange",
			message: msg,
		});
	}

	handleClearMessage(): void {
		console.log('🔷 ChatProvider: Handling clear message (from ChatService)');
		// This is called by ChatService.clearSession(), so don't call clearSession() again
		// Just handle any webview-specific clearing if needed
		this._view?.webview.postMessage({
			type: 'clearMessage'
		});
	}

	public clearChat(): void {
		console.log('🔷 ChatProvider: Clearing chat and resetting MCP session');
		// Reset MCP client session when chat is cleared
		resetMCPClient();
		// Clear the chat service which will handle clearing messages
		const chatService = sharedChatServiceImpl();
		chatService.clearSession();
	}

	public sendMessage(message: string) {
		if (this._view) {
			const chatService = sharedChatServiceImpl();
			chatService.confirmPrompt(message);
		}
	}

	public updateReasoningMode(_mode: 'pre_loop'): void {
		// PRE loop is now mandatory (paper compliance)
		this._reasoningMode = 'pre_loop';
		console.log('🧠 ChatProvider: PRE loop mode is mandatory (paper compliance)');
		
		// Persist the setting
		this._context.globalState.update('superinference.reasoningMode', 'pre_loop');
		
		// Notify webview
		if (this._view) {
			this._view.webview.postMessage({
				type: 'reasoningModeUpdate',
				mode: 'pre_loop',
				description: 'Event-driven PRE loop with belief states and critic-gated memory (mandatory for paper compliance)'
			});
		}
	}

	public async triggerStreamEdit(prompt: string, filePath: string, selection?: vscode.Selection): Promise<void> {
		console.log('🔷 ChatProvider: Public triggerStreamEdit called:', { prompt, filePath });
		await this.handleStreamEdit(prompt, filePath, selection, this._currentContextFiles);
	}

	public async triggerInteractivePRELoop(instruction: string): Promise<void> {
		console.log('🧠 ChatProvider: Public triggerInteractivePRELoop called:', instruction);
		await this.handleInteractivePRELoop(instruction, this._currentContextFiles);
	}

	private async refreshContextFileAfterEdit(fileUri: vscode.Uri): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Refreshing context file after edit:', fileUri.toString());

			// Read the actual file content from disk to ensure it's up to date
			const fileBytes = await vscode.workspace.fs.readFile(fileUri);
			const actualContent = Buffer.from(fileBytes).toString('utf8');

			// Create updated context file
			const updatedFile = {
				uri: fileUri.toString(),
				name: path.basename(fileUri.fsPath),
				type: 'file',
				content: actualContent,
				language: this.getLanguageFromFileName(fileUri.fsPath)
			};

			// Send message to webview to update the specific context file
			this.sendToWebview({
				type: 'contextFileUpdated',
				file: updatedFile
			});

			console.log('🔷 ChatProvider: Context file refreshed with', actualContent.length, 'characters');

		} catch (error) {
			console.error('🔷 ChatProvider: Error refreshing context file after edit:', error);
		}
	}

	// NOTE: Simple extension mapping - kept for fast lookups, but MCP tools provide deeper analysis
	private getLanguageFromFileName(fileName: string): string {
		const ext = path.extname(fileName).toLowerCase();
		const languageMap: { [key: string]: string } = {
			'.py': 'python',
			'.js': 'javascript',
			'.ts': 'typescript',
			'.java': 'java',
			'.cpp': 'cpp',
			'.c': 'c',
			'.cs': 'csharp',
			'.go': 'go',
			'.rs': 'rust',
			'.php': 'php',
			'.rb': 'ruby',
			'.swift': 'swift',
			'.kt': 'kotlin'
		};
		return languageMap[ext] || 'text';
	}

	private async getWorkspaceContextFiles_DEPRECATED(): Promise<any[]> {
		// This method is no longer needed
		return [];
	}

	private _getHtmlForWebview(webview: vscode.Webview, baseUri: vscode.Uri): string {
		const nonce = getNonce();

		// Get proper webview URIs for resources
		const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, 'webview.js'));
		const stylesUri = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, 'superinference-icons.css'));

		// Get codicon CSS for VS Code icons
		const codiconsUri = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, 'codicon.css'));

		// Get font URIs with cache-busting (fonts are heavily cached by browsers)
		// Version hash ensures new fonts are loaded after rebuild
		const fontVersion = '20251202-new';
		const fontEot = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, `superinference.eot?v=${fontVersion}`));
		const fontWoff = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, `superinference.woff?v=${fontVersion}`));
		const fontTtf = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, `superinference.ttf?v=${fontVersion}`));
		const fontSvg = webview.asWebviewUri(vscode.Uri.joinPath(baseUri, `superinference.svg?v=${fontVersion}`));

		return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; font-src ${webview.cspSource}; connect-src http://localhost:3000;">
    <title>SuperInference Assistant</title>
    
    <!-- SuperInference Custom Fonts -->
    <style nonce="${nonce}">
        @font-face {
            font-family: 'superinference';
            src: url('${fontEot}#iefix') format('embedded-opentype'),
                 url('${fontWoff}') format('woff'),
                 url('${fontTtf}') format('truetype'),
                 url('${fontSvg}#superinference') format('svg');
            font-weight: normal;
            font-style: normal;
            font-display: block;
        }

        .icon-superinference {
            font-family: 'superinference' !important;
            speak: none;
            font-style: normal;
            font-weight: normal;
            font-variant: normal;
            text-transform: none;
            line-height: 1;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }

        .icon-superinference:before {
            content: "R"; /* SuperInference custom icon */
        }

        /* VS Code Integration Styles */
        body {
            font-family: var(--vscode-font-family);
            font-size: var(--vscode-font-size);
            font-weight: var(--vscode-font-weight);
            color: var(--vscode-foreground);
            background-color: var(--vscode-editor-background);
            margin: 0;
            padding: 0;
            height: 100vh;
            overflow: hidden;
        }

        .superinference-root {
            height: 100vh;
            display: flex;
            flex-direction: column;
        }

        .loading-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 100vh;
            gap: 16px;
        }

        .loading-icon {
            font-size: 32px;
            animation: spin 2s linear infinite;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .loading-text {
            color: var(--vscode-descriptionForeground);
            font-size: 14px;
        }
    </style>
    
    <link rel="stylesheet" href="${stylesUri}">
    <link rel="stylesheet" href="${codiconsUri}">
</head>
<body>
    <div class="superinference-root">
        <div class="loading-container" id="loading">
            <div class="loading-icon icon-superinference"></div>
            <div class="loading-text">Loading SuperInference Assistant...</div>
        </div>
        <div id="root" style="height: 100vh; display: none;"></div>
    </div>

    <script nonce="${nonce}" src="${scriptUri}"></script>
    <script nonce="${nonce}">
        // Backend connection configuration
        window.SUPERINFERENCE_CONFIG = {
            backendUrl: 'http://localhost:3000'
            // Note: API key is handled by the backend server
        };
        
        // Initialize the React app
        window.addEventListener('DOMContentLoaded', () => {
            const loading = document.getElementById('loading');
            const root = document.getElementById('root');
            
            // Show loading state briefly
            setTimeout(() => {
                loading.style.display = 'none';
                root.style.display = 'block';
                
                // Initialize chat page if available
                if (window.initializeChatPage) {
                    window.initializeChatPage();
                }
            }, 1000);
        });
    </script>
</body>
</html>`;
	}

	private sendToWebview(message: any): void {
		if (this._view) {
			this._view.webview.postMessage(message);
		}
	}

	async makeFinalResponseCall(prompt: string, context?: any[]): Promise<string> {
		try {
			console.log('🔌 Making backend final response call...');
			console.log('🔌 Prompt:', prompt.substring(0, 100) + '...');
			console.log('🔌 Context files being sent:', context?.length || 0, 'files');
			console.log('🔌 Context details:', context?.map(f => ({
				name: f.name || 'unnamed',
				uri: f.uri || 'no-uri',
				contentLength: f.content?.length || 0,
				contentPreview: f.content?.substring(0, 50) + '...' || 'no content'
			})) || 'none');

			// Use MCP client instead of direct Flask call
			const mcpClient = await getInitializedMCPClient();
			const finalResponse = await mcpClient.streamChat(
				prompt,
				context || [],
				[], // chat history
				'final', // conversation ID
				`final_${Date.now()}`
			);

			return finalResponse;
		} catch (error) {
			console.error('❌ Backend final response call failed:', error);
			return `Error generating response: ${error instanceof Error ? error.message : 'Unknown error'}`;
		}
	}

	async makeAnalysisCall(prompt: string, context?: any[], onToken?: (delta: string, meta?: any) => void): Promise<string> {
		try {
			console.log('🔌 Making MCP analysis call...');
			console.log('🔌 Prompt:', prompt.substring(0, 100) + '...');
			console.log('🔌 Context files being sent:', context?.length || 0, 'files');

			// Use MCP client for analysis call
			const mcpClient = await getInitializedMCPClient();
			const analysisResponse = await mcpClient.streamChat(
				prompt,
				context || [],
				[], // chat history
				'analysis', // conversation ID
				`analysis_${Date.now()}`
			);

			console.log(`✅ Got MCP analysis response: ${analysisResponse.length} characters`);

			// Call the onToken callback with the complete response
			if (onToken) {
				onToken(analysisResponse, { type: 'analysis_complete' });
			}

			return analysisResponse;

		} catch (error) {
			console.error('❌ Backend analysis call failed:', error);
			return `Error during analysis: ${error instanceof Error ? error.message : 'Unknown error'}`;
		}
	}

	private buildContextSummary(files: any[]): string {
		try {
			if (!files || files.length === 0) return '';
			const top = files.slice(0, 3).map((f: any) => {
				const name = f.name || 'file';
				const snippet = (f.content || '').toString().substring(0, 300).replace(/\n/g, ' ');
				return `- ${name}: ${snippet}...`;
			}).join('\n');
			return `Context summary:\n${top}`;
		} catch {
			return '';
		}
	}

	private buildStructuredReasoningPrompt(userPrompt: string, enhancedContextSummary: string): string {
		return `You are an expert AI assistant. For the following request, produce ONLY a structured chain of thoughts analysis. Do NOT provide the final answer in this message.\n\nUse this EXACT format for your reasoning:\n\n<thinking>\nYour initial thoughts and understanding of the request\n</thinking>\n\n<analysis>\nDetailed analysis of the problem/request including:\n- Current state assessment\n- Technical considerations\n- Potential challenges\n- Required approach\n</analysis>\n\n<planning>\nStep-by-step plan including:\n- Specific actions to take\n- Implementation sequence\n- Resource requirements\n- Success criteria\n</planning>\n\n<execution>\nDetailed execution guidance:\n- Specific implementation details\n- Code examples if applicable\n- Best practices to follow\n- Expected outcomes\n</execution>\n\n<validation>\nValidation approach:\n- What to verify\n- Testing strategies\n- Quality checks\n- Success metrics\n</validation>\n\nIMPORTANT: Do NOT include the final answer. End after </validation>.\n\nRequest: ${userPrompt}\n\n${enhancedContextSummary}`;
	}

	// Read-only analysis search helpers
	private extractSearchTermsFromPrompt(prompt: string, maxTerms: number = 6): string[] {
		try {
			const terms = new Set<string>();
			// Quoted strings
			for (const m of prompt.matchAll(/"([^"]{2,80})"|'([^']{2,80})'/g)) {
				const t = (m[1] || m[2] || '').trim();
				if (t.length >= 2) terms.add(t);
			}
			// Code-like identifiers (min 3 chars)
			for (const m of prompt.matchAll(/\b[A-Za-z_][A-Za-z0-9_\.]{2,}\b/g)) {
				const t = (m[0] || '').trim();
				if (t.length >= 3 && !/^(the|and|for|with|from|this|that|into|onto|code|file|files|class|function)$/i.test(t)) {
					terms.add(t);
				}
			}
			// Path-like fragments
			for (const m of prompt.matchAll(/[A-Za-z0-9_\-\.\/]+\.[A-Za-z0-9_]{1,6}/g)) {
				const t = (m[0] || '').trim();
				if (t.length >= 3) terms.add(t);
			}
			return Array.from(terms).slice(0, maxTerms);
		} catch {
			return [];
		}
	}

	private async findTextInWorkspace(queries: string[], perQueryLimit: number = 10): Promise<Array<{ query: string; file: string; uri: vscode.Uri; line: number; preview: string }>> {
		const results: Array<{ query: string; file: string; uri: vscode.Uri; line: number; preview: string }> = [];
		if (!queries || queries.length === 0) return results;

		const rootFolder = vscode.workspace.workspaceFolders?.[0];
		const exclude: vscode.GlobPattern | undefined = rootFolder
			? new vscode.RelativePattern(rootFolder, '{**/node_modules/**,**/.git/**,**/dist/**,**/build/**,**/out/**,**/target/**,**/.next/**,**/.nuxt/**}')
			: undefined;

		for (const query of queries) {
			let count = 0;
			try {
				await (vscode.workspace as any).findTextInFiles(
					{ pattern: query, isCaseSensitive: false, isRegExp: false },
					{ include: undefined, exclude, maxResults: perQueryLimit },
					(res: any) => {
						if (count >= perQueryLimit) return;
						const uri: vscode.Uri = res.uri;
						const ranges: any[] = (res.ranges || (res.range ? [res.range] : []));
						const previewText: string = (res.preview && res.preview.text) ? res.preview.text : '';
						const line = ranges[0] && ranges[0].start && typeof ranges[0].start.line === 'number' ? (ranges[0].start.line as number) : 0;
						results.push({ query, file: vscode.workspace.asRelativePath(uri), uri, line: line + 1, preview: previewText.trim() });
						count++;
					}
				);
			} catch (e) {
				console.warn('Search failed for query:', query, e);
			}
		}

		return results;
	}

	private formatSearchResultsMarkdown(hits: Array<{ query: string; file: string; line: number; preview: string }>): string {
		if (!hits || hits.length === 0) return '';
		const grouped = hits.reduce((acc: Record<string, Array<{ file: string; line: number; preview: string }>>, h) => {
			acc[h.query] = acc[h.query] || [];
			acc[h.query].push({ file: h.file, line: h.line, preview: h.preview });
			return acc;
		}, {});

		const sections = Object.entries(grouped).map(([q, items]) => {
			const lines = items.slice(0, 10).map(it => `- ${it.file}:${it.line} — ${it.preview.replace(/\n/g, ' ')}`);
			return `• Query: "${q}"
${lines.join('\n')}`;
		});

		return `\n\n### Workspace search findings\n${sections.join('\n\n')}\n`;
	}

	private async embeddingsSearch(query: string, topK: number = 8, threshold: number = 0.1): Promise<Array<{ id: string; content: string; similarity: number; metadata?: any }>> {
		try {
			const mcpClient = await getInitializedMCPClient();
			const results = await mcpClient.searchEmbeddings(query, topK, threshold);
			return results as Array<{ id: string; content: string; similarity: number; metadata?: any }>;
		} catch (e) {
			console.warn('Embeddings search failed:', e);
			return [];
		}
	}

	private formatEmbeddingsResultsMarkdown(results: Array<{ content: string; similarity: number; metadata?: any }>): string {
		if (!results || results.length === 0) return '';
		const lines = results.slice(0, 10).map(r => {
			const name = r.metadata?.name || r.metadata?.file_path || r.metadata?.relativePath || r.metadata?.type || 'entry';
			const sim = typeof r.similarity === 'number' ? r.similarity.toFixed(3) : '—';
			const snippet = (r.content || '').toString().replace(/\n/g, ' ').slice(0, 160);
			return `- ${name} (sim=${sim}) — ${snippet}${snippet.length >= 160 ? '...' : ''}`;
		});
		return `\n\n### Semantic search findings\n${lines.join('\n')}\n`;
	}

	private async handleCompareCheckpoint(checkpointId: string, conversationId: string): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Comparing checkpoint:', checkpointId);

			// Use MCP client to compare checkpoint
			const mcpClient = await getInitializedMCPClient();
			const comparisonResult = await mcpClient.compareCheckpoint(checkpointId, []);

			console.log(`🔷 ChatProvider: Comparing checkpoint ${checkpointId} for conversation ${conversationId}`);

			if (comparisonResult.success) {
				// Send comparison data to webview
				this.sendToWebview({
					type: 'checkpointComparison',
					checkpointId: checkpointId,
					comparison: comparisonResult,
					success: true
				});

				// Show diff information
				const changedFiles = comparisonResult.changed_files || 0;
				vscode.window.showInformationMessage(
					`Checkpoint comparison: ${changedFiles} file${changedFiles !== 1 ? 's' : ''} changed`
				);
			} else {
				throw new Error(comparisonResult.error || 'Comparison failed');
			}

		} catch (error) {
			console.error('🔷 ChatProvider: Error comparing checkpoint:', error);
			vscode.window.showErrorMessage(
				`Failed to compare checkpoint: ${error instanceof Error ? error.message : 'Unknown error'}`
			);

			this.sendToWebview({
				type: 'checkpointComparison',
				checkpointId: checkpointId,
				success: false,
				error: error instanceof Error ? error.message : 'Unknown error'
			});
		}
	}

	private async handleMultiFileEdit(prompt: string, contextFiles: any[]): Promise<void> {
		try {
			console.log('🔷 ChatProvider: Processing multi-file edit for', contextFiles.length, 'files');

			// Add user message for the multi-file request
			const chatService = sharedChatServiceImpl();
			const userMessageId = await chatService.addMessage(
				`Multi-file edit request: ${prompt}`,
				false, // not a reply
				undefined, // no codeEdit yet
				'text'
			);

			// Process each file separately
			console.log('🔷 ChatProvider: Starting to process', contextFiles.length, 'files in sequence');
			for (let i = 0; i < contextFiles.length; i++) {
				const file = contextFiles[i];
				console.log(`🔷 ChatProvider: *** PROCESSING FILE ${i + 1}/${contextFiles.length} ***`);
				console.log(`🔷 ChatProvider: File details:`, { name: file.name, uri: file.uri, hasContent: !!file.content });

				try {
					// Use the existing streamEdit method for each file
					console.log(`🔷 ChatProvider: Calling streamEdit for file ${i + 1}:`, file.uri);
					await chatService.streamEdit(prompt, file.uri, undefined);
					console.log(`🔷 ChatProvider: ✅ Completed streamEdit for file ${i + 1}`);

					// Small delay between files to avoid overwhelming the system
					if (i < contextFiles.length - 1) {
						console.log(`🔷 ChatProvider: Waiting 500ms before processing next file...`);
						await new Promise(resolve => setTimeout(resolve, 500));
					}
				} catch (error) {
					console.error(`🔷 ChatProvider: ❌ Error editing file ${file.uri}:`, error);

					// Add error message for this file
					await chatService.addMessage(
						`Error editing ${file.name || file.uri}: ${error instanceof Error ? error.message : 'Unknown error'}`,
						true, // is reply
						undefined,
						'text'
					);
				}
			}
			console.log('🔷 ChatProvider: *** MULTI-FILE PROCESSING COMPLETE ***');

			console.log('🔷 ChatProvider: Multi-file edit completed');

		} catch (error) {
			console.error('🔷 ChatProvider: Error in handleMultiFileEdit:', error);
			vscode.window.showErrorMessage(`Failed to edit multiple files: ${error instanceof Error ? error.message : 'Unknown error'}`);
		}
	}

	private async handleIntelligentEditDirect(prompt: string, analysis: any, contextFiles?: any[]): Promise<void> {
		// This version doesn't create user messages since they're already created in streamChat
		try {
			console.log('🧠 ChatProvider: Starting intelligent edit (direct) with AI guidance');
			console.log('🧠 ChatProvider: Analysis:', analysis);

			// Extract target files from AI analysis
			const targetFiles = analysis.target_files || [];
			const fileActions = analysis.target_file_actions || {};

			if (targetFiles.length === 0) {
				console.log('🧠 ChatProvider: No specific target files detected, using current file');
				// Fallback to current file or first context file
				const currentFile = vscode.window.activeTextEditor?.document.uri.fsPath;
				const fallbackFile = currentFile || (contextFiles?.[0]?.uri);

				if (fallbackFile) {
					// Call streamEdit directly without user message creation
					const chatService = sharedChatServiceImpl();
					await chatService.streamEdit(prompt, fallbackFile, undefined);
				}
				return;
			}

			// Process each target file identified by AI
			const chatService = sharedChatServiceImpl();
			for (const targetFile of targetFiles) {
				console.log(`🧠 ChatProvider: Processing AI-identified target file: ${targetFile}`);

				// Check if this is a special case (all files)
				if (targetFile === 'all_context_files' || targetFile === '*') {
					console.log('🧠 ChatProvider: AI detected request for all context files');
					if (contextFiles && contextFiles.length > 1) {
						// Process each context file
						for (const file of contextFiles) {
							await chatService.streamEdit(prompt, file.uri, undefined);
						}
					} else if (contextFiles && contextFiles.length === 1) {
						await chatService.streamEdit(prompt, contextFiles[0].uri, undefined);
					}
					continue;
				}

				// Find the actual file path for the target file
				let actualFilePath = targetFile;

				// If it's just a filename, try to find it in context files
				if (!targetFile.includes('/') && !targetFile.includes('\\')) {
					const matchingContextFile = contextFiles?.find(f =>
						f.name === targetFile ||
						f.uri.endsWith(targetFile) ||
						f.uri.includes(targetFile)
					);

					if (matchingContextFile) {
						actualFilePath = matchingContextFile.uri;
						console.log(`🧠 ChatProvider: Matched ${targetFile} to context file: ${actualFilePath}`);
					}
				}

				// Determine action for this file
				const action = fileActions[targetFile] || 'modify';

				if (action === 'modify') {
					console.log(`🧠 ChatProvider: Modifying existing file: ${actualFilePath}`);
					// Use streamEdit to modify existing file (without creating user messages)
					await chatService.streamEdit(prompt, actualFilePath, undefined);
				}
				// Note: Skip 'create' action for now to avoid complexity
			}

			console.log('🧠 ChatProvider: Intelligent edit (direct) processing completed');

		} catch (error) {
			console.error('🧠 ChatProvider: Error in intelligent edit (direct):', error);
		}
	}

	private async handleIntelligentEdit(prompt: string, analysis: any, contextFiles?: any[]): Promise<void> {
		try {
			console.log('🧠 ChatProvider: Starting intelligent edit with AI guidance');
			console.log('🧠 ChatProvider: Analysis:', analysis);

			// Extract target files from AI analysis
			const targetFiles = analysis.target_files || [];
			const fileActions = analysis.target_file_actions || {};

			if (targetFiles.length === 0) {
				console.log('🧠 ChatProvider: No specific target files detected, using current file');
				// Fallback to current file or first context file
				const currentFile = vscode.window.activeTextEditor?.document.uri.fsPath;
				const fallbackFile = currentFile || (contextFiles?.[0]?.uri);

				if (fallbackFile) {
					await this.handleStreamEdit(prompt, fallbackFile, undefined, contextFiles);
				} else {
					console.log('🧠 ChatProvider: No files available, falling back to streamChat');
					await this.handleStreamChat(prompt, contextFiles);
				}
				return;
			}

			// Process each target file identified by AI
			for (const targetFile of targetFiles) {
				console.log(`🧠 ChatProvider: Processing AI-identified target file: ${targetFile}`);

				// Check if this is a special case (all files)
				if (targetFile === 'all_context_files' || targetFile === '*') {
					console.log('🧠 ChatProvider: AI detected request for all context files');
					if (contextFiles && contextFiles.length > 1) {
						await this.handleMultiFileEdit(prompt, contextFiles);
					} else if (contextFiles && contextFiles.length === 1) {
						await this.handleStreamEdit(prompt, contextFiles[0].uri, undefined, contextFiles);
					}
					continue;
				}

				// Find the actual file path for the target file
				let actualFilePath = targetFile;

				// If it's just a filename, try to find it in context files
				if (!targetFile.includes('/') && !targetFile.includes('\\')) {
					const matchingContextFile = contextFiles?.find(f =>
						f.name === targetFile ||
						f.uri.endsWith(targetFile) ||
						f.uri.includes(targetFile)
					);

					if (matchingContextFile) {
						actualFilePath = matchingContextFile.uri;
						console.log(`🧠 ChatProvider: Matched ${targetFile} to context file: ${actualFilePath}`);
					} else {
						// Check if it should be created as a new file
						const action = fileActions[targetFile];
						if (action === 'create') {
							console.log(`🧠 ChatProvider: AI suggests creating new file: ${targetFile}`);
							// Use streamCreate for new files
							await this.handleStreamCreate(`Create ${targetFile}: ${prompt}`, contextFiles);
							continue;
						} else {
							// Try to construct path relative to workspace
							const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
							if (workspaceRoot) {
								actualFilePath = `${workspaceRoot}/${targetFile}`;
								console.log(`🧠 ChatProvider: Constructed workspace path: ${actualFilePath}`);
							}
						}
					}
				}

				// Determine action for this file
				const action = fileActions[targetFile] || 'modify';

				if (action === 'create') {
					console.log(`🧠 ChatProvider: Creating new file: ${actualFilePath}`);
					// Use streamCreate to create new file
					await this.handleStreamCreate(`Create ${targetFile}: ${prompt}`, contextFiles);
				} else {
					console.log(`🧠 ChatProvider: Modifying existing file: ${actualFilePath}`);
					// Use streamEdit to modify existing file
					await this.handleStreamEdit(prompt, actualFilePath, undefined, contextFiles);
				}
			}

			console.log('🧠 ChatProvider: Intelligent edit processing completed');

		} catch (error) {
			console.error('🧠 ChatProvider: Error in intelligent edit:', error);
			// Fallback to regular edit
			const currentFile = vscode.window.activeTextEditor?.document.uri.fsPath;
			await this.handleStreamEdit(prompt, currentFile || '', undefined, contextFiles);
		}
	}

} 