/**
 * MCP Client for SuperInference VS Code Extension
 * Implements proper MCP protocol communication over HTTP
 */

import * as vscode from 'vscode';
import * as http from 'http';
import * as https from 'https';
import { URL } from 'url';

export interface MCPToolCall {
    name: string;
    arguments: Record<string, any>;
}

export interface MCPToolResult {
    content: Array<{
        type: string;
        text?: string;
        data?: string;
        mimeType?: string;
    }>;
    isError?: boolean;
}

interface MCPRequest {
    jsonrpc: string;
    method: string;
    params: any;
    id: number | string;
}

interface MCPResponse {
    jsonrpc: string;
    result?: any;
    error?: {
        code: number;
        message: string;
        data?: any;
    };
    id: number | string;
}

export class MCPClient {
    private baseUrl: string;
    private requestId = 0;
    private sessionId: string | null = null;
    private initialized = false;

    constructor(baseUrl: string = 'http://localhost:3000/mcp') {
        this.baseUrl = baseUrl;
    }

    /**
     * Alternative HTTP request method using Node.js built-in modules
     * This should work better in VS Code extension host environment
     */
    private async makeHttpRequest(data: any): Promise<{ status: number; statusText: string; headers: Record<string, string>; text: string }> {
        return new Promise((resolve, reject) => {
            const url = new URL(this.baseUrl);
            const isHttps = url.protocol === 'https:';
            const httpModule = isHttps ? https : http;

            const postData = JSON.stringify(data);

            const options = {
                hostname: url.hostname,
                port: url.port || (isHttps ? 443 : 80),
                path: url.pathname + url.search,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json, text/event-stream',
                    'Content-Length': Buffer.byteLength(postData),
                    ...(this.sessionId ? { 'mcp-session-id': this.sessionId } : {})
                }
            };

            const req = httpModule.request(options, (res) => {
                let responseData = '';

                res.on('data', (chunk) => {
                    responseData += chunk;
                });

                res.on('end', () => {
                    // Extract session ID from response headers if present
                    const responseSessionId = res.headers['mcp-session-id'];
                    if (responseSessionId && !this.sessionId) {
                        this.sessionId = responseSessionId as string;
                        console.log(`📝 Received session ID: ${this.sessionId}`);
                    }

                    resolve({
                        status: res.statusCode || 500,
                        statusText: res.statusMessage || 'Unknown',
                        headers: res.headers as Record<string, string>,
                        text: responseData
                    });
                });
            });

            req.on('error', (error) => {
                console.error('❌ HTTP request error:', error);
                reject(error);
            });

            req.write(postData);
            req.end();
        });
    }

    private getNextRequestId(): number {
        return ++this.requestId;
    }

    private async makeRequest(method: string, params: any = {}): Promise<MCPResponse> {
        const request: MCPRequest = {
            jsonrpc: "2.0",
            method,
            params,
            id: this.getNextRequestId()
        };

        try {
            const headers: Record<string, string> = {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/event-stream'
            };

            // Add session ID if we have one (optional for some servers)
            if (this.sessionId) {
                headers['mcp-session-id'] = this.sessionId;
            }

            console.log(`🔧 Making MCP request: ${method}`, params);

            // Try fetch first, fallback to Node.js HTTP if it fails
            let response: { status: number; statusText: string; headers: Record<string, string>; text: string };

            try {
                // Try using fetch (might fail in VS Code extension host)
                const fetchResponse = await fetch(this.baseUrl, {
                    method: 'POST',
                    headers,
                    body: JSON.stringify(request)
                });

                // Extract session ID from response headers if present
                const responseSessionId = fetchResponse.headers.get('mcp-session-id');
                if (responseSessionId && !this.sessionId) {
                    this.sessionId = responseSessionId;
                    console.log(`📝 Received session ID: ${this.sessionId}`);
                }

                // Convert Headers to plain object
                const headersObj: Record<string, string> = {};
                fetchResponse.headers.forEach((value, key) => {
                    headersObj[key] = value;
                });

                response = {
                    status: fetchResponse.status,
                    statusText: fetchResponse.statusText,
                    headers: headersObj,
                    text: await fetchResponse.text()
                };
            } catch (fetchError) {
                console.warn('⚠️ Fetch failed, trying Node.js HTTP:', fetchError);
                // Fallback to Node.js HTTP implementation
                response = await this.makeHttpRequest(request);
            }

            if (response.status < 200 || response.status >= 300) {
                console.error(`❌ MCP request failed: ${response.status} ${response.statusText}`, response.text);

                // Handle session invalidation (400/401 errors often indicate bad session)
                if (response.status === 400 || response.status === 401) {
                    console.log('🔄 Session may be invalid, clearing session and retrying...');
                    this.clearSession();

                    // If this wasn't already an initialize request, try to re-initialize and retry
                    if (method !== 'initialize') {
                        try {
                            await this.initialize();
                            console.log('🔄 Re-initialized after session error, retrying request...');
                            return await this.makeRequest(method, params);
                        } catch (retryError) {
                            console.error('❌ Retry after re-initialization failed:', retryError);
                        }
                    }
                }

                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            // Handle SSE response format
            const responseText = response.text;
            let result: MCPResponse;

            // Check if response is SSE format
            if (responseText.includes('event: message') && responseText.includes('data:')) {
                // Parse SSE format
                const lines = responseText.split('\n');
                let jsonData = '';

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        jsonData = line.substring(6); // Remove 'data: ' prefix
                        break;
                    }
                }

                if (jsonData) {
                    result = JSON.parse(jsonData);
                } else {
                    throw new Error('No data found in SSE response');
                }
            } else {
                // Try to parse as regular JSON
                try {
                    result = JSON.parse(responseText);
                } catch (parseError) {
                    console.error('❌ Failed to parse response:', responseText.substring(0, 200));
                    throw new Error(`Failed to parse MCP response: ${parseError}`);
                }
            }

            if (result.error) {
                console.error(`❌ MCP protocol error:`, result.error);
                throw new Error(`MCP Error ${result.error.code}: ${result.error.message}`);
            }

            console.log(`✅ MCP request successful: ${method}`);
            return result;
        } catch (error) {
            console.error(`❌ MCP request failed for ${method}:`, error);
            throw error;
        }
    }

    clearSession(): void {
        console.log('🗑️ Clearing MCP session');
        this.sessionId = null;
        this.initialized = false;
    }

    async initialize(): Promise<void> {
        if (this.initialized) {
            return;
        }

        try {
            console.log('🔌 Initializing MCP connection...');
            const response = await this.makeRequest('initialize', {
                protocolVersion: "2024-11-05",
                capabilities: {
                    tools: {},
                    resources: {}
                },
                clientInfo: {
                    name: "SuperInference VS Code Extension",
                    version: "0.5.4"
                }
            });

            console.log('✅ MCP Server initialized:', response.result);

            // Send the initialized notification to complete the handshake
            try {
                // Send as notification (no response expected)
                const notificationRequest = {
                    jsonrpc: "2.0",
                    method: "notifications/initialized",
                    params: {}
                };

                const headers: Record<string, string> = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json, text/event-stream'
                };

                if (this.sessionId) {
                    headers['mcp-session-id'] = this.sessionId;
                }

                try {
                    await fetch(this.baseUrl, {
                        method: 'POST',
                        headers,
                        body: JSON.stringify(notificationRequest)
                    });
                } catch (fetchError) {
                    console.warn('⚠️ Fetch failed for notification, trying Node.js HTTP:', fetchError);
                    // Fallback to Node.js HTTP implementation
                    await this.makeHttpRequest(notificationRequest);
                }

                console.log('✅ MCP initialization handshake completed');

                // Small delay to ensure server processes the initialization
                await new Promise(resolve => setTimeout(resolve, 100));
            } catch (notificationError) {
                console.warn('⚠️ Failed to send initialized notification (non-critical):', notificationError);
            }

            this.initialized = true;
        } catch (error) {
            console.error('❌ Failed to initialize MCP server:', error);
            throw error;
        }
    }

    async callTool(toolCall: MCPToolCall): Promise<MCPToolResult> {
        // Always ensure we're properly initialized and have a session
        if (!this.initialized || !this.sessionId) {
            console.log('🔄 Reinitializing MCP client...');
            this.initialized = false;
            this.sessionId = null;
            await this.initialize();
        }

        try {
            const response = await this.makeRequest('tools/call', {
                name: toolCall.name,
                arguments: toolCall.arguments
            });

            return response.result as MCPToolResult;
        } catch (error) {
            console.error(`❌ Failed to call tool ${toolCall.name}:`, error);
            // If we get a session error, try reinitializing once
            if (error instanceof Error && error.message.includes('session')) {
                console.log('🔄 Session error, reinitializing...');
                this.initialized = false;
                this.sessionId = null;
                await this.initialize();

                // Retry the call once
                const response = await this.makeRequest('tools/call', {
                    name: toolCall.name,
                    arguments: toolCall.arguments
                });
                return response.result as MCPToolResult;
            }
            throw error;
        }
    }

    async listTools(): Promise<any[]> {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const response = await this.makeRequest('tools/list');
            return response.result?.tools || [];
        } catch (error) {
            console.error('❌ Failed to list tools:', error);
            throw error;
        }
    }

    async readResource(uri: string): Promise<any> {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const response = await this.makeRequest('resources/read', { uri });
            return response.result;
        } catch (error) {
            console.error(`❌ Failed to read resource ${uri}:`, error);
            throw error;
        }
    }

    // High-level methods that match the original Flask API
    async streamChat(
        prompt: string,
        contextFiles?: Array<{ name: string, content: string }>,
        chatHistory?: Array<{ role: string, content: string }>,
        conversationId: string = 'default',
        messageId?: string,
        apiKey?: string,
        model: string = 'gemini-pro'
    ): Promise<string> {
        const result = await this.callTool({
            name: 'stream_chat',
            arguments: {
                prompt,
                context_files: contextFiles,
                chat_history: chatHistory,
                conversation_id: conversationId,
                message_id: messageId,
                api_key: apiKey,
                model
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Chat failed');
        }

        return result.content[0]?.text || '';
    }

    async streamGenerate(
        query: string,
        currentFileContent?: string,
        languageId: string = 'python',
        workspacePath: string = ''
    ): Promise<string> {
        const result = await this.callTool({
            name: 'stream_generate',
            arguments: {
                query,
                current_file_content: currentFileContent,
                language_id: languageId,
                workspace_path: workspacePath
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Generation failed');
        }

        return result.content[0]?.text || '';
    }

    async streamEdit(
        fileContent: string,
        editPrompt: string,
        fileName: string = 'file.py',
        languageId: string = 'python'
    ): Promise<string> {
        const result = await this.callTool({
            name: 'stream_edit',
            arguments: {
                file_content: fileContent,
                edit_prompt: editPrompt,
                file_name: fileName,
                language_id: languageId
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Edit failed');
        }

        return result.content[0]?.text || '';
    }

    async createEmbeddings(
        content: string,
        metadata: Record<string, any>,
        chunkType: string = 'general',
        filePath?: string,
        functionName?: string,
        className?: string,
        startLine?: number,
        endLine?: number
    ): Promise<string> {
        // Bundle all parameters into metadata as expected by MCP server
        const enhancedMetadata = {
            ...metadata,
            chunk_type: chunkType,
            file_path: filePath,
            function_name: functionName,
            class_name: className,
            start_line: startLine,
            end_line: endLine
        };

        const result = await this.callTool({
            name: 'create_embeddings',
            arguments: {
                content,
                metadata: enhancedMetadata
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Embedding creation failed');
        }

        return result.content[0]?.text || '';
    }

    async searchEmbeddings(
        query: string,
        topK: number = 10,
        minSimilarity: number = 0.3
    ): Promise<Array<any>> {
        const result = await this.callTool({
            name: 'search_embeddings',
            arguments: {
                query,
                top_k: topK,
                min_similarity: minSimilarity
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Search failed');
        }

        // Parse JSON result
        try {
            return JSON.parse(result.content[0]?.text || '[]');
        } catch {
            return [];
        }
    }

    async clearEmbeddings(): Promise<string> {
        const result = await this.callTool({
            name: 'clear_embeddings',
            arguments: {}
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Clear failed');
        }

        return result.content[0]?.text || '';
    }

    async callResource(uri: string): Promise<string> {
        const result = await this.makeRequest('resources/read', {
            uri: uri
        });

        if (result.error) {
            throw new Error(`Resource error: ${result.error.message}`);
        }

        // Return the resource content
        return result.result?.contents?.[0]?.text || '';
    }

    async getEmbeddingsStatus(): Promise<any> {
        try {
            // Call as resource instead of tool
            const statusText = await this.callResource('status://embeddings');
            return JSON.parse(statusText || '{}');
        } catch (error) {
            console.warn('Failed to get embeddings status:', error);
            return { error: 'Failed to get status' };
        }
    }

    // Intelligent request analysis
    async analyzeRequestIntent(
        userRequest: string,
        contextFiles?: any[],
        currentFile?: string
    ): Promise<any> {
        const result = await this.callTool({
            name: 'analyze_request_intent',
            arguments: {
                user_request: userRequest,
                context_files: contextFiles || [],
                current_file: currentFile
            }
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Intent analysis failed');
        }

        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            // Fallback to simple analysis
            return {
                action_type: 'chat',
                confidence: 0.0,
                reasoning: 'Failed to parse intent analysis',
                target_files: [],
                requires_file_context: false
            };
        }
    }

    async getPerformanceMetrics(): Promise<any> {
        const result = await this.callTool({
            name: 'get_performance_metrics',
            arguments: {}
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Metrics retrieval failed');
        }

        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async healthCheck(): Promise<any> {
        const result = await this.callTool({
            name: 'health_check',
            arguments: {}
        });

        if (result.isError) {
            throw new Error(result.content[0]?.text || 'Health check failed');
        }

        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async streamCreate(prompt: string, projectName: string = "new_project", description: string = ""): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'stream_create',
            arguments: {
                prompt,
                project_name: projectName,
                description
            }
        });
    }

    // Planning tools
    async updatePlanningConfig(options: {
        tau_event_threshold?: number;
        kappa_confidence_stop?: number;
        epsilon_min_eig?: number;
        max_events?: number;
        max_steps?: number;
        critic_accept_threshold?: number;
        critic_provider?: string;
        critic_model?: string;
    }): Promise<any> {
        const result = await this.callTool({
            name: 'update_planning_config',
            arguments: {
                ...options
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'update_planning_config failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return { success: true };
        }
    }

    async generatePlanSteps(instruction: string, currentFileContent?: string, maxSteps?: number): Promise<any> {
        const result = await this.callTool({
            name: 'generate_plan_steps',
            arguments: {
                instruction,
                current_file_content: currentFileContent,
                max_steps: maxSteps
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'generate_plan_steps failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async streamPlanExecute(
        instruction: string,
        currentFileContent?: string,
        languageId: string = 'python',
        workspacePath: string = '',
        contextFiles?: any[],
        planId?: string,
        stepIndex: number = 0
    ): Promise<any> {
        const result = await this.callTool({
            name: 'stream_plan_execute',
            arguments: {
                instruction,
                current_file_content: currentFileContent,
                language_id: languageId,
                workspace_path: workspacePath,
                context_files: contextFiles,
                plan_id: planId,
                step_index: stepIndex
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'stream_plan_execute failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async planExecute(instruction: string, currentFileContent?: string, languageId: string = 'python', workspacePath: string = '', contextFiles?: any[]): Promise<any> {
        const result = await this.callTool({
            name: 'plan_execute',
            arguments: {
                instruction,
                current_file_content: currentFileContent,
                language_id: languageId,
                workspace_path: workspacePath,
                context_files: contextFiles
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'plan_execute failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    // Additional theoretical framework methods
    async executeDataAnalysis(instruction: string, dataDirectory: string = '', maxSteps: number = 3): Promise<any> {
        const result = await this.callTool({
            name: 'execute_data_analysis',
            arguments: {
                instruction,
                data_directory: dataDirectory,
                max_steps: maxSteps
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'execute_data_analysis failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async analyzeLanguageFeatures(
        content: string,
        filePath: string = '',
        languageHint: string = ''
    ): Promise<any> {
        const result = await this.callTool({
            name: 'analyze_language_features',
            arguments: {
                content,
                file_path: filePath,
                language_hint: languageHint
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'analyze_language_features failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async removePrintStatementsDynamic(
        content: string,
        filePath: string = '',
        languageHint: string = ''
    ): Promise<any> {
        const result = await this.callTool({
            name: 'remove_print_statements_dynamic',
            arguments: {
                content,
                file_path: filePath,
                language_hint: languageHint
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'remove_print_statements_dynamic failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async analyzeCodeStructure(
        content: string,
        filePath: string = '',
        languageHint: string = '',
        analysisType: string = 'comprehensive'
    ): Promise<any> {
        const result = await this.callTool({
            name: 'analyze_code_structure',
            arguments: {
                content,
                file_path: filePath,
                language_hint: languageHint,
                analysis_type: analysisType
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'analyze_code_structure failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async getAvailableTools(): Promise<any> {
        try {
            const toolsText = await this.callResource('tools://available');
            return JSON.parse(toolsText || '{}');
        } catch (error) {
            console.warn('Failed to get available tools:', error);
            return { 
                tools: [], 
                tool_categories: {}, 
                tool_dependencies: {} 
            };
        }
    }

    async switchProvider_DEPRECATED(providerName: string, apiKey?: string, model?: string): Promise<any> {
        const result = await this.callTool({
            name: 'switch_provider',
            arguments: {
                provider_name: providerName,
                api_key: apiKey,
                model: model
            }
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'switch_provider failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    async getProviderStatus_DEPRECATED(): Promise<any> {
        const result = await this.callTool({
            name: 'get_provider_status',
            arguments: {}
        });
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'get_provider_status failed');
        }
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    // File operations and diff management
    async applyCodeEdit_DEPRECATED(
        filePath: string,
        newContent: string,
        originalContent?: string,
        startLine?: number,
        endLine?: number
    ): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'apply_code_edit',
            arguments: {
                file_path: filePath,
                new_content: newContent,
                original_content: originalContent,
                start_line: startLine,
                end_line: endLine
            }
        });
    }

    async generateFileDiff(
        filePath: string,
        originalContent: string,
        newContent: string,
        contextLines: number = 3
    ): Promise<any> {
        const result = await this.callTool({
            name: 'generate_file_diff',
            arguments: {
                file_path: filePath,
                original_content: originalContent,
                new_content: newContent,
                context_lines: contextLines
            }
        });
        
        if (result.isError) {
            throw new Error(result.content[0]?.text || 'generate_file_diff failed');
        }
        
        try {
            return JSON.parse(result.content[0]?.text || '{}');
        } catch {
            return {};
        }
    }

    // Checkpoint management
    async createCheckpoint_DEPRECATED(
        conversationId: string,
        description: string,
        messageIndex: number,
        codeChanges?: any[],
        conversationHistory?: any[]
    ): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'create_checkpoint',
            arguments: {
                conversation_id: conversationId,
                description,
                message_index: messageIndex,
                code_changes: codeChanges || [],
                conversation_history: conversationHistory || []
            }
        });
    }

    async restoreCheckpoint_DEPRECATED(checkpointId: string, restoreType: string = "both"): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'restore_checkpoint',
            arguments: {
                checkpoint_id: checkpointId,
                restore_type: restoreType
            }
        });
    }

    async listCheckpoints_DEPRECATED(conversationId: string): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'list_checkpoints',
            arguments: {
                conversation_id: conversationId
            }
        });
    }

    async compareCheckpoint(checkpointId: string, currentCodeChanges?: any[]): Promise<any> {
        return this.makeRequest('tools/call', {
            name: 'compare_checkpoint',
            arguments: {
                checkpoint_id: checkpointId,
                current_code_changes: currentCodeChanges || []
            }
        });
    }


}

// Singleton instance for the extension with proper session management
let mcpClient: MCPClient | null = null;
let initializationPromise: Promise<void> | null = null;

export function getMCPClient(baseUrl?: string): MCPClient {
    if (!mcpClient || (baseUrl && mcpClient['baseUrl'] !== baseUrl)) {
        mcpClient = new MCPClient(baseUrl);
        initializationPromise = null; // Reset initialization promise
    }
    return mcpClient;
}

export async function getInitializedMCPClient(baseUrl?: string): Promise<MCPClient> {
    const client = getMCPClient(baseUrl);

    // Ensure initialization happens only once across all calls
    if (!initializationPromise) {
        initializationPromise = client.initialize();
    }

    await initializationPromise;
    return client;
}

export function resetMCPClient(): void {
    if (mcpClient) {
        mcpClient.clearSession();
    }
    mcpClient = null;
    initializationPromise = null;
} 