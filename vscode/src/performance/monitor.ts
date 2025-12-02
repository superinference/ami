interface OperationTracker {
    name: string;
    startTime: number;
    endTime?: number;
    duration?: number;
    metadata?: any;
}

interface PerformanceMetrics {
    totalOperations: number;
    averageDuration: number;
    maxDuration: number;
    minDuration: number;
    operationCounts: Map<string, number>;
    memoryUsage: NodeJS.MemoryUsage;
    timestamp: number;
}

export class PerformanceMonitor {
    private operations: OperationTracker[] = [];
    private activeOperations = new Map<string, OperationTracker>();
    private isMonitoring = false;
    private metricsInterval?: NodeJS.Timeout;
    private maxOperationHistory = 1000;

    constructor() { }

    startMonitoring(): void {
        if (this.isMonitoring) return;

        this.isMonitoring = true;

        // Collect metrics every 30 seconds
        this.metricsInterval = setInterval(() => {
            this.collectMetrics();
        }, 30000);
    }

    stopMonitoring(): void {
        if (!this.isMonitoring) return;

        this.isMonitoring = false;

        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
            this.metricsInterval = undefined;
        }
    }

    startOperation(name: string, metadata?: any): OperationTracker {
        const tracker: OperationTracker = {
            name,
            startTime: Date.now(),
            metadata
        };

        const operationId = `${name}_${tracker.startTime}_${Math.random().toString(36).substr(2, 9)}`;
        this.activeOperations.set(operationId, tracker);

        return {
            ...tracker,
            complete: () => this.completeOperation(operationId)
        } as OperationTracker & { complete: () => void };
    }

    private completeOperation(operationId: string): void {
        const tracker = this.activeOperations.get(operationId);
        if (!tracker) return;

        tracker.endTime = Date.now();
        tracker.duration = tracker.endTime - tracker.startTime;

        this.operations.push(tracker);
        this.activeOperations.delete(operationId);

        // Keep only recent operations to prevent memory leaks
        if (this.operations.length > this.maxOperationHistory) {
            this.operations.shift();
        }
    }

    private collectMetrics(): PerformanceMetrics {
        const now = Date.now();
        const recentOperations = this.operations.filter(op =>
            op.endTime && (now - op.endTime) < 300000 // Last 5 minutes
        );

        const durations = recentOperations
            .map(op => op.duration!)
            .filter(d => d !== undefined);

        const operationCounts = new Map<string, number>();
        recentOperations.forEach(op => {
            operationCounts.set(op.name, (operationCounts.get(op.name) || 0) + 1);
        });

        const metrics: PerformanceMetrics = {
            totalOperations: recentOperations.length,
            averageDuration: durations.length > 0 ? durations.reduce((a, b) => a + b, 0) / durations.length : 0,
            maxDuration: durations.length > 0 ? Math.max(...durations) : 0,
            minDuration: durations.length > 0 ? Math.min(...durations) : 0,
            operationCounts,
            memoryUsage: process.memoryUsage(),
            timestamp: now
        };

        return metrics;
    }

    getMetrics(): PerformanceMetrics {
        return this.collectMetrics();
    }

    getSlowOperations(threshold = 5000): OperationTracker[] {
        return this.operations.filter(op =>
            op.duration && op.duration > threshold
        );
    }

    getOperationStats(operationName: string): {
        count: number;
        averageDuration: number;
        maxDuration: number;
        minDuration: number;
    } {
        const operations = this.operations.filter(op => op.name === operationName);
        const durations = operations
            .map(op => op.duration!)
            .filter(d => d !== undefined);

        return {
            count: operations.length,
            averageDuration: durations.length > 0 ? durations.reduce((a, b) => a + b, 0) / durations.length : 0,
            maxDuration: durations.length > 0 ? Math.max(...durations) : 0,
            minDuration: durations.length > 0 ? Math.min(...durations) : 0
        };
    }

    clear(): void {
        this.operations = [];
        this.activeOperations.clear();
    }
} 