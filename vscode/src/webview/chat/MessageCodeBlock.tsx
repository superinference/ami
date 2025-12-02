import * as React from "react";
import { useCallback, useState } from "react";
import { VSCodeButton } from "@vscode/webview-ui-toolkit/react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/cjs/styles/prism";
import { Copy, Check, FileText, Plus } from "lucide-react";

import { getServiceManager } from "../../common/ipc/webview";
import { IChatService, CHAT_SERVICE_NAME } from "../../common/chatService";

export interface MessageCodeBlockProps {
    contents: string;
    language: string;
}

export function MessageCodeBlock(props: MessageCodeBlockProps) {
    const { contents, language } = props;
    const [copied, setCopied] = useState(false);
    
    const handleCopyAction = useCallback(async () => {
        try {
            await navigator.clipboard.writeText(contents);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch (err) {
            // Fallback for older browsers
            const textArea = document.createElement('textarea');
            textArea.value = contents;
            document.body.appendChild(textArea);
            textArea.select();
            document.execCommand('copy');
            document.body.removeChild(textArea);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        }
    }, [contents]);
    
    const handleInsertCodeSnippetAction = useCallback(async () => {
        const chatService = await getServiceManager().getService<IChatService>(
            CHAT_SERVICE_NAME
        );
        await chatService.insertCodeSnippet(contents);
    }, [contents]);

    const customStyle = {
        margin: 0,
        padding: '16px',
        background: 'var(--vscode-editor-background)',
        fontSize: 'var(--vscode-editor-font-size, 12px)',
        fontFamily: 'var(--vscode-editor-font-family, "Fira Code", "Cascadia Code", "JetBrains Mono", Consolas, "Courier New", monospace)',
        lineHeight: '1.4',
        borderRadius: '0px'
    };

    return (
        <div className="code-block-container my-3 rounded border border-vscode-border overflow-hidden group">
            {/* Enhanced header */}
            <div className="code-block-header bg-vscode-sidebar-bg border-b border-vscode-border px-3 py-2 flex justify-between items-center">
                <div className="flex items-center gap-2">
                    <div className="flex items-center gap-1">
                        <span className="w-2 h-2 bg-red-500 rounded-full"></span>
                        <span className="w-2 h-2 bg-yellow-500 rounded-full"></span>
                        <span className="w-2 h-2 bg-green-500 rounded-full"></span>
                    </div>
                    <span className="text-xs text-vscode-description ml-2">
                        {language || 'text'}
                    </span>
                </div>
                
                {/* Enhanced toolbar */}
                <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button
                        onClick={handleCopyAction}
                        className="w-7 h-7 flex items-center justify-center rounded transition-colors hover:bg-vscode-button-bg focus:outline-none focus:ring-2 focus:ring-vscode-focus-border"
                        title={copied ? "Copied!" : "Copy to clipboard"}
                    >
                        {copied ? (
                            <Check size={14} className="text-vscode-success" />
                        ) : (
                            <Copy size={14} className="text-vscode-description hover:text-vscode-fg" />
                        )}
                    </button>
                    <button
                        onClick={handleInsertCodeSnippetAction}
                        className="w-7 h-7 flex items-center justify-center rounded transition-colors hover:bg-vscode-button-bg focus:outline-none focus:ring-2 focus:ring-vscode-focus-border"
                        title="Insert into editor"
                    >
                        <Plus size={14} className="text-vscode-description hover:text-vscode-fg" />
                    </button>
                </div>
            </div>
            
            {/* Enhanced syntax highlighter */}
            <div className="code-block-content">
                <SyntaxHighlighter
                    language={language}
                    style={vscDarkPlus}
                    customStyle={customStyle}
                    showLineNumbers={(contents || '').split('\n').length > 1}
                    lineNumberStyle={{
                        color: 'var(--vscode-editorLineNumber-foreground)',
                        backgroundColor: 'transparent',
                        paddingRight: '16px',
                        minWidth: '2em',
                        textAlign: 'right'
                    }}
                    wrapLines={true}
                    wrapLongLines={true}
                >
                    {contents}
                </SyntaxHighlighter>
            </div>
        </div>
    );
}
