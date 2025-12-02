import * as React from "react";
import { createRoot } from "react-dom/client";

import "./style.css";
import "./styles/base.css";
import { EnhancedChatPage } from "./chat/EnhancedChatPage";

// Error Boundary Component
class ErrorBoundary extends React.Component<
    { children: React.ReactNode },
    { hasError: boolean; error?: Error }
> {
    constructor(props: { children: React.ReactNode }) {
        super(props);
        this.state = { hasError: false };
    }

    static getDerivedStateFromError(error: Error) {
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
        console.error('SuperInference Chat Error:', error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    padding: '20px',
                    textAlign: 'center',
                    color: 'var(--vscode-foreground)',
                    backgroundColor: 'var(--vscode-editor-background)',
                    height: '100vh',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'center',
                    alignItems: 'center'
                }}>
                    <h2>AMI from SuperInference</h2>
                    <p>Loading chat interface...</p>
                    <div style={{ marginTop: '20px', fontSize: '12px', opacity: 0.7 }}>
                        {this.state.error?.message || 'Initializing components...'}
                    </div>
                    <button
                        onClick={() => this.setState({ hasError: false })}
                        style={{
                            marginTop: '20px',
                            padding: '8px 16px',
                            backgroundColor: 'var(--vscode-button-background)',
                            color: 'var(--vscode-button-foreground)',
                            border: 'none',
                            borderRadius: '0px',
                            cursor: 'pointer'
                        }}
                    >
                        Retry
                    </button>
                </div>
            );
        }

        return this.props.children;
    }
}

// SuperInference Chat App - Always render ChatPage with error boundary
function SuperInferenceApp() {
    const [isLoaded, setIsLoaded] = React.useState(false);

    React.useEffect(() => {
        // Ensure DOM is ready and give a moment for initialization
        const timer = setTimeout(() => {
            setIsLoaded(true);
        }, 500);

        return () => clearTimeout(timer);
    }, []);

    if (!isLoaded) {
        return (
            <div style={{
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                alignItems: 'center',
                height: '100vh',
                color: 'var(--vscode-foreground)',
                backgroundColor: 'var(--vscode-editor-background)'
            }}>
                <div style={{
                    fontSize: '32px',
                    marginBottom: '16px',
                    animation: 'spin 2s linear infinite',
                    fontFamily: 'superinference',
                    lineHeight: '1'
                }} className="icon-superinference">
                    R
                </div>
                <div>Loading AMI from SuperInference...</div>
                <style>{`
                    @keyframes spin {
                        0% { transform: rotate(0deg); }
                        100% { transform: rotate(360deg); }
                    }
                `}</style>
            </div>
        );
    }

    return (
        <ErrorBoundary>
                            <EnhancedChatPage />
        </ErrorBoundary>
    );
}

// Initialize the chat page
let isInitialized = false;
let reactRoot: any = null;

function initializeChatPage() {
    if (isInitialized) {
        console.log('SuperInference: Already initialized, skipping...');
        return;
    }
    
    console.log('SuperInference: Initializing chat page...');
    
    const rootElement = document.getElementById("root");
    if (rootElement) {
        try {
            if (!reactRoot) {
                reactRoot = createRoot(rootElement);
            }
            reactRoot.render(<SuperInferenceApp />);
            isInitialized = true;
            console.log('SuperInference: Chat page initialized successfully');
        } catch (error) {
            console.error('SuperInference: Failed to initialize React app:', error);
            // Fallback to simple HTML
            rootElement.innerHTML = `
                <div style="padding: 20px; text-align: center; color: var(--vscode-foreground);">
                    <h2>AMI from SuperInference</h2>
                    <p>Chat interface is loading...</p>
                    <p style="font-size: 12px; opacity: 0.7;">Preparing AI assistant...</p>
                </div>
            `;
        }
    } else {
        console.error('SuperInference: Root element not found');
    }
}

// Expose the initialization function globally
(window as any).initializeChatPage = initializeChatPage;

// Auto-initialize if root exists
if (typeof document !== 'undefined' && document.getElementById("root")) {
    console.log('SuperInference: Auto-initializing...');
    initializeChatPage();
}
