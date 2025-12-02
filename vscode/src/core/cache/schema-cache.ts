interface CacheEntry<T> {
    value: T;
    timestamp: number;
    accessCount: number;
    lastAccessed: number;
}

export class SchemaCache {
    private _cache = new Map<string, CacheEntry<any>>();
    private _enumCache = new Set<any>();
    private _maxSize = 1000;
    private _ttl = 30 * 60 * 1000; // 30 minutes

    constructor(maxSize = 1000, ttl = 30 * 60 * 1000) {
        this._maxSize = maxSize;
        this._ttl = ttl;

        // Periodic cleanup every 5 minutes
        setInterval(() => this.cleanup(), 5 * 60 * 1000);
    }

    get<T>(key: string): T | undefined {
        const entry = this._cache.get(key);
        if (!entry) return undefined;

        // Check TTL
        if (Date.now() - entry.timestamp > this._ttl) {
            this._cache.delete(key);
            return undefined;
        }

        // Update access statistics
        entry.accessCount++;
        entry.lastAccessed = Date.now();

        return entry.value;
    }

    set<T>(key: string, value: T): void {
        // Evict least recently used if at capacity
        if (this._cache.size >= this._maxSize) {
            this.evictLRU();
        }

        this._cache.set(key, {
            value,
            timestamp: Date.now(),
            accessCount: 1,
            lastAccessed: Date.now()
        });
    }

    private evictLRU(): void {
        let oldestTime = Date.now();
        let oldestKey: string | null = null;

        this._cache.forEach((entry, key) => {
            if (entry.lastAccessed < oldestTime) {
                oldestTime = entry.lastAccessed;
                oldestKey = key;
            }
        });

        if (oldestKey) {
            this._cache.delete(oldestKey);
        }
    }

    private cleanup(): void {
        const now = Date.now();
        const keysToDelete: string[] = [];
        this._cache.forEach((entry, key) => {
            if (now - entry.timestamp > this._ttl) {
                keysToDelete.push(key);
            }
        });
        keysToDelete.forEach(key => this._cache.delete(key));
    }

    // Cache enum values for faster lookup
    cacheEnum(value: any): boolean {
        if (this._enumCache.has(value)) {
            return true;
        }
        this._enumCache.add(value);
        return false;
    }

    clear(): void {
        this._cache.clear();
        this._enumCache.clear();
    }

    getStats() {
        const entries = Array.from(this._cache.values());
        return {
            size: this._cache.size,
            averageAccessCount: entries.length > 0 ? entries.reduce((sum, e) => sum + e.accessCount, 0) / entries.length : 0,
            memoryUsage: this._cache.size
        };
    }
} 