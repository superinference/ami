import {
    IChatViewService,
    CHAT_VIEW_SERVICE_NAME,
} from "../../common/chatService";
import { MessageItemModel } from "../../common/chatService/model";

export class ChatViewServiceImpl implements IChatViewService {
    setIsReadyAction: ((isReady: boolean) => void) | null = null;
    setHasSelectionAction: ((hasSelection: boolean) => void) | null = null;
    addMessageAction: ((msg: MessageItemModel) => void) | null = null;
    updateMessageAction: ((msg: MessageItemModel) => void) | null = null;
    clearMessageAction: (() => void) | null = null;

    get name(): string {
        return CHAT_VIEW_SERVICE_NAME;
    }

    async setIsBusy(isBusy: boolean): Promise<void> {
        this.setIsReadyAction?.call(null, isBusy);
    }

    async setHasSelection(hasSelection: boolean): Promise<void> {
        this.setHasSelectionAction?.call(null, hasSelection);
    }

    async addMessage(msg: MessageItemModel): Promise<void> {
        this.addMessageAction?.call(null, msg);
    }

    async updateMessage(msg: MessageItemModel): Promise<void> {
        this.updateMessageAction?.call(null, msg);
    }

    async clearMessage(): Promise<void> {
        this.clearMessageAction?.call(null);
    }

    async focus(): Promise<void> {
        // Send message to webview to focus
        this.postMessage('focus', {});
    }

    async insertCodeSnippet(contents: string): Promise<void> {
        // Send message to webview to insert code
        this.postMessage('insertCode', { contents });
    }

    async openFile(uri: string): Promise<void> {
        // Send message to webview to open file
        this.postMessage('openFile', { uri });
    }

    async getWorkspaceContext(): Promise<any> {
        // Request workspace context from webview
        return new Promise((resolve) => {
            this.postMessage('getWorkspaceContext', {});
            // For now, return mock data
            setTimeout(() => {
                resolve({
                    activeFile: 'src/example.ts',
                    language: 'typescript',
                    selection: {
                        start: { line: 0, character: 0 },
                        end: { line: 10, character: 0 }
                    },
                    selectedText: 'function example() {\n    console.log("Hello");\n}',
                    workspace: '/workspace'
                });
            }, 100);
        });
    }

    async applyFileEdit(filePath: string, newContent: string, startLine?: number, endLine?: number): Promise<boolean> {
        try {
            // Send message to webview to apply file edit
            this.postMessage('applyFileEdit', {
                filePath,
                newContent,
                startLine,
                endLine
            });
            
            console.log(`Applied file edit to ${filePath}:`, { startLine, endLine, newContent });
            return true;
        } catch (error) {
            console.error('Failed to apply file edit:', error);
            return false;
        }
    }

    async showFileDiff(filePath: string, originalContent: string, newContent: string): Promise<void> {
        // Send message to webview to show diff
        this.postMessage('showFileDiff', {
            filePath,
            originalContent,
            newContent
        });
    }

    private postMessage(type: string, data: any): void {
        // This would normally post to the webview
        // For now, we'll simulate the message posting
        console.log(`ChatViewService: ${type}`, data);
        
        // In a real implementation, this would send messages to VS Code
        // window.postMessage({ type, ...data }, '*');
    }
}
