import { IService } from "../ipc";
import { MessageItemModel } from "./model";

export const CHAT_SERVICE_NAME = "chat";

export interface IChatService extends IService {
    confirmPrompt(prompt: string): Promise<void>;
    syncState(): Promise<void>;
    insertCodeSnippet(contents: string): Promise<void>;
    clearSession(): void;
    // New streaming operations
    streamChat(prompt: string, contextFiles?: any[]): Promise<void>;
    streamEdit(prompt: string, filePath: string, selection?: any): Promise<void>;
    streamGenerate(prompt: string, contextFiles?: any[]): Promise<void>;
    // New: create files via streaming
    streamCreate(prompt: string, contextFiles?: any[]): Promise<void>;
    // Forwards structured messages from provider/webview
    addMessage(contents: string, isReply?: boolean, codeEdit?: MessageItemModel['codeEdit'], messageType?: 'text' | 'code' | 'diff' | 'file-edit'): Promise<string>;
}

export const CHAT_VIEW_SERVICE_NAME = "chat_view";

export interface IChatViewService extends IService {
    setIsBusy(isBusy: boolean): Promise<void>;
    setHasSelection(hasSelection: boolean): Promise<void>;
    addMessage(msg: MessageItemModel): Promise<void>;
    updateMessage(msg: MessageItemModel): Promise<void>;
    clearMessage(): Promise<void>;
    focus(): Promise<void>;
    insertCodeSnippet(contents: string): Promise<void>;
    openFile(uri: string): Promise<void>;
    getWorkspaceContext(): Promise<any>;
    // New file editing operations
    applyFileEdit(filePath: string, newContent: string, startLine?: number, endLine?: number): Promise<boolean>;
    showFileDiff(filePath: string, originalContent: string, newContent: string): Promise<void>;
}

// Backend API configuration
export const BACKEND_CONFIG = {
    baseUrl: 'http://localhost:3000',
    // Note: API key is handled by the backend server - no key needed in extension
    endpoints: {
        streamChat: '/aiserver.v1.AiService/StreamChat',
        streamEdit: '/aiserver.v1.AiService/StreamEdit', 
        streamGenerate: '/aiserver.v1.AiService/StreamGenerate',
        // New create endpoint
        streamCreate: '/aiserver.v1.AiService/StreamCreate'
    }
};
