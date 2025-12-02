import * as React from "react";
import ReactMarkdown from "react-markdown";
import { motion } from "framer-motion";
import { User, Bot, Loader2, Terminal, FileCode, GitBranch } from "lucide-react";

import { MessageItemModel } from "../../common/chatService/model";
import { MessageCodeBlock } from "./MessageCodeBlock";
import { IndeterminateProgressBar } from "./IndeterminateProgressBar";
import { CommandExecution } from "../components/CommandExecution";
import { CheckpointControls } from "../components/CheckpointControls";
import { EnhancedDiffView } from "../components/EnhancedDiffView";

export interface MessageItemProps {
    model: MessageItemModel;
    // Enhanced props for special message types
    messageType?: 'text' | 'command' | 'file_edit' | 'checkpoint' | 'error' | 'api_request';
    commandData?: {
        command: string;
        output?: string[];
        isRunning?: boolean;
        exitCode?: number;
        requiresApproval?: boolean;
    };
    checkpointData?: {
        messageId: string;
        isCheckedOut?: boolean;
    };
    diffData?: {
        filePath: string;
        changes: any[];
    };
    onCommandApprove?: () => void;
    onCommandReject?: () => void;
    onCheckpointCompare?: (messageId: string) => void;
    onCheckpointRestore?: (messageId: string, type: 'task' | 'workspace' | 'both') => void;
}

export function MessageItem(props: MessageItemProps) {
    const { 
        model, 
        messageType = 'text',
        commandData,
        checkpointData,
        diffData,
        onCommandApprove,
        onCommandReject,
        onCheckpointCompare,
        onCheckpointRestore
    } = props;
    const { contents, isReply, isFinished } = model;
    
    const messageContent = contents + (isReply && !isFinished ? "\u{258A}" : "");

    const getMessageStyle = () => {
        // Enhanced styling based on message type
        if (isReply) {
            const iconMap = {
                text: Bot,
                command: Terminal,
                file_edit: FileCode,
                checkpoint: GitBranch,
                error: Bot,
                api_request: Bot
            };
            
            const colorMap = {
                text: 'border-blue-500',
                command: 'border-green-500',
                file_edit: 'border-orange-500',
                checkpoint: 'border-purple-500',
                error: 'border-red-500',
                api_request: 'border-gray-500'
            };
            
            return {
                container: `bg-transparent border-l-2 ${colorMap[messageType]} pl-4 group`,
                avatar: messageType === 'error' ? 'bg-red-500' : 
                        messageType === 'command' ? 'bg-green-500' :
                        messageType === 'file_edit' ? 'bg-orange-500' :
                        messageType === 'checkpoint' ? 'bg-purple-500' : 'bg-blue-500',
                icon: iconMap[messageType]
            };
        }
        return {
            container: 'bg-vscode-input-bg border border-vscode-input-border rounded-lg p-3 ml-8 group',
            avatar: 'bg-green-500', 
            icon: User
        };
    };

    const style = getMessageStyle();
    const IconComponent = style.icon;

    return (
        <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3 }}
            className={`message ${isReply ? 'assistant' : 'user'} ${style.container} mb-4`}
        >
            <div className="flex items-start gap-3">
                {/* Avatar */}
                <div className={`
                    w-6 h-6 rounded-full flex items-center justify-center flex-shrink-0 mt-1
                    ${style.avatar}
                `}>
                    <IconComponent size={14} className="text-white" />
                </div>

                {/* Message Content */}
                <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                            <span className="text-sm font-medium text-vscode-fg">
                                {isReply ? 'Assistant' : 'You'}
                            </span>
                            <span className="text-xs text-vscode-description">
                                {new Date().toLocaleTimeString()}
                            </span>
                            {/* Message type indicator */}
                            {messageType !== 'text' && (
                                <span className="px-2 py-1 text-xs bg-vscode-badge-background text-vscode-badge-foreground rounded capitalize">
                                    {messageType.replace('_', ' ')}
                                </span>
                            )}
                        </div>
                    </div>

                    {/* Enhanced message content based on type */}
                    {messageType === 'command' && commandData ? (
                        <CommandExecution
                            command={commandData.command}
                            output={commandData.output}
                            isRunning={commandData.isRunning}
                            exitCode={commandData.exitCode}
                            requiresApproval={commandData.requiresApproval}
                            onApprove={onCommandApprove}
                            onReject={onCommandReject}
                        />
                    ) : messageType === 'checkpoint' && checkpointData ? (
                        <div className="space-y-3">
                            <div className="prose prose-sm max-w-none">
                                <MessageTextView contents={messageContent} />
                            </div>
                            <CheckpointControls
                                messageId={checkpointData.messageId}
                                isCheckedOut={checkpointData.isCheckedOut}
                                onCompare={onCheckpointCompare}
                                onRestore={onCheckpointRestore}
                            />
                        </div>
                    ) : messageType === 'file_edit' && diffData ? (
                        <div className="space-y-3">
                            <div className="prose prose-sm max-w-none">
                                <MessageTextView contents={messageContent} />
                            </div>
                            <EnhancedDiffView
                                fileDiffs={[{
                                    filePath: diffData.filePath,
                                    changes: diffData.changes
                                }]}
                            />
                        </div>
                    ) : (
                        /* Default text message */
                        <div className="prose prose-sm max-w-none">
                            <MessageTextView contents={messageContent} />
                        </div>
                    )}

                    {/* Enhanced loading indicator */}
                    {isReply && !isFinished && (
                        <div className="flex items-center gap-2 mt-3 text-vscode-description animate-pulse">
                            <Loader2 size={14} className="animate-spin" />
                            <span className="text-xs">Assistant is typing...</span>
                        </div>
                    )}
                </div>
            </div>
        </motion.div>
    );
}

interface MessageTextViewProps {
    contents: string;
}

function MessageTextView(props: MessageTextViewProps) {
    const { contents } = props;
    return (
        <ReactMarkdown
            components={{
                pre({ children, ...props }) {
                    if (children.length !== 1) {
                        // Not code block.
                        return <pre {...props}>{children}</pre>;
                    }
                    const child = children[0] as React.ReactElement;
                    const codeContents = child.props.children[0];
                    const codeClassName = child.props.className;
                    const languageMatch =
                        /language-(\w+)/.exec(codeClassName || "") || [];
                    return (
                        <MessageCodeBlock
                            contents={codeContents}
                            language={languageMatch[1] || ""}
                        />
                    );
                },
            }}
        >
            {contents}
        </ReactMarkdown>
    );
}
