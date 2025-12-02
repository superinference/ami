interface TimeoutInfo {
    timeoutId: NodeJS.Timeout;
    startTime: number;
    timeout: number;
    maxTotalTimeout?: number;
    resetTimeoutOnProgress: boolean;
    onTimeout: () => void;
}

export class TimeoutManager {
    private _timeoutInfo = new Map<string, TimeoutInfo>();

    setupTimeout(
        requestId: string,
        timeout: number,
        onTimeout: () => void,
        options: {
            maxTotalTimeout?: number;
            resetOnProgress?: boolean;
        } = {}
    ): void {
        const info: TimeoutInfo = {
            timeoutId: setTimeout(onTimeout, timeout),
            startTime: Date.now(),
            timeout,
            maxTotalTimeout: options.maxTotalTimeout,
            resetTimeoutOnProgress: options.resetOnProgress || false,
            onTimeout
        };

        this._timeoutInfo.set(requestId, info);
    }

    resetTimeout(requestId: string): void {
        const info = this._timeoutInfo.get(requestId);
        if (!info) return;

        const elapsed = Date.now() - info.startTime;

        // Check if we've exceeded maximum total timeout
        if (info.maxTotalTimeout && elapsed >= info.maxTotalTimeout) {
            this._timeoutInfo.delete(requestId);
            throw new Error(`Maximum total timeout exceeded: ${info.maxTotalTimeout}ms`);
        }

        // Reset the timeout
        clearTimeout(info.timeoutId);
        info.timeoutId = setTimeout(info.onTimeout, info.timeout);
    }

    clearTimeout(requestId: string): void {
        const info = this._timeoutInfo.get(requestId);
        if (info) {
            clearTimeout(info.timeoutId);
            this._timeoutInfo.delete(requestId);
        }
    }

    clearAllTimeouts(): void {
        const requestIds = Array.from(this._timeoutInfo.keys());
        requestIds.forEach(requestId => this.clearTimeout(requestId));
    }
}

// Circuit Breaker Pattern Implementation
enum CircuitState {
    CLOSED = 'CLOSED',
    OPEN = 'OPEN',
    HALF_OPEN = 'HALF_OPEN'
}

interface CircuitBreakerOptions {
    failureThreshold: number;
    recoveryTimeout: number;
    monitoringPeriod: number;
}

export class CircuitBreaker {
    private failures = 0;
    private lastFailureTime = 0;
    private state = CircuitState.CLOSED;
    private nextAttemptTime = 0;

    constructor(private options: CircuitBreakerOptions = {
        failureThreshold: 5,
        recoveryTimeout: 60000, // 1 minute
        monitoringPeriod: 10000  // 10 seconds
    }) { }

    async execute<T>(operation: () => Promise<T>): Promise<T> {
        if (this.state === CircuitState.OPEN) {
            if (Date.now() < this.nextAttemptTime) {
                throw new Error('Circuit breaker is OPEN');
            }
            this.state = CircuitState.HALF_OPEN;
        }

        try {
            const result = await operation();
            this.onSuccess();
            return result;
        } catch (error) {
            this.onFailure();
            throw error;
        }
    }

    private onSuccess(): void {
        this.failures = 0;
        this.state = CircuitState.CLOSED;
    }

    private onFailure(): void {
        this.failures++;
        this.lastFailureTime = Date.now();

        if (this.failures >= this.options.failureThreshold) {
            this.state = CircuitState.OPEN;
            this.nextAttemptTime = Date.now() + this.options.recoveryTimeout;
        }
    }

    getState(): CircuitState {
        return this.state;
    }

    getStats() {
        return {
            state: this.state,
            failures: this.failures,
            lastFailureTime: this.lastFailureTime,
            nextAttemptTime: this.nextAttemptTime
        };
    }
}

// Exponential Backoff Implementation
export class ExponentialBackoff {
    private attempts = 0;
    private baseDelay: number;
    private maxDelay: number;
    private maxAttempts: number;

    constructor(options: {
        baseDelay?: number;
        maxDelay?: number;
        maxAttempts?: number;
    } = {}) {
        this.baseDelay = options.baseDelay || 1000;
        this.maxDelay = options.maxDelay || 30000;
        this.maxAttempts = options.maxAttempts || 5;
    }

    async execute<T>(operation: () => Promise<T>): Promise<T> {
        while (this.attempts < this.maxAttempts) {
            try {
                const result = await operation();
                this.reset();
                return result;
            } catch (error) {
                this.attempts++;

                if (this.attempts >= this.maxAttempts) {
                    throw error;
                }

                const delay = Math.min(
                    this.baseDelay * Math.pow(2, this.attempts - 1),
                    this.maxDelay
                );

                await this.delay(delay);
            }
        }

        throw new Error('Max attempts exceeded');
    }

    private delay(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    reset(): void {
        this.attempts = 0;
    }

    getAttempts(): number {
        return this.attempts;
    }
} 