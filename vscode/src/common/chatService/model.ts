export interface MessageItemModel {
    id: string;
    contents: string;
    isReply?: boolean;
    isFinished?: boolean;
    messageType?: 'text' | 'code' | 'diff' | 'file-edit';
    timestamp?: number; // Add timestamp for checkpoint ordering
    // New properties for enhanced chat features
    codeEdit?: {
        filePath: string;
        originalCode: string;
        newCode: string;
        startLine: number;
        endLine: number;
        explanation: string;
    };
    references?: Array<{
        uri: string;
        range?: {
            start: { line: number; character: number };
            end: { line: number; character: number };
        };
    }>;
}
