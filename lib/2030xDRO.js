const fs = require('fs');
const path = require('path');

// Path to the JSON database
const dbFilePath = path.join(__dirname, '../database.json');
const batchInterval = 1000; // 1 second for batch processing
const cache = new Map();
let batchQueue = [];

/**
 * Load the database from JSON file.
 * @returns {Object} The database object.
 */
const loadDatabase = () => {
    if (fs.existsSync(dbFilePath)) {
        return JSON.parse(fs.readFileSync(dbFilePath, 'utf-8'));
    }
    return {};
};

/**
 * Save the database to JSON file.
 * @param {Object} data - The database object to save.
 */
const saveDatabase = (data) => {
    fs.writeFileSync(dbFilePath, JSON.stringify(data, null, 2));
};

/**
 * Resolve conflicts between existing and incoming records using advanced strategies.
 * @param {Object} existing - The existing record in the database.
 * @param {Object} incoming - The incoming record to merge.
 * @returns {Object} The merged record.
 */
const resolveConflicts = (existing, incoming) => {
    for (const key in incoming) {
        if (!existing.hasOwnProperty(key) || (existing[key] !== incoming[key])) {
            existing[key] = incoming[key];
        }
    }
    existing.version += 1;
    return existing;
};

/**
 * Initialize in-memory cache from the database.
 */
const initializeCache = () => {
    const data = loadDatabase();
    for (const id in data) {
        cache.set(id, data[id]);
    }
};

/**
 * Process the batch queue periodically to update the JSON file.
 */
const processBatchQueue = () => {
    if (batchQueue.length > 0) {
        const batchedData = {};
        batchQueue.forEach(({ id, record }) => {
            if (!batchedData[id]) {
                batchedData[id] = cache.get(id) || {};
            }
            batchedData[id] = resolveConflicts(batchedData[id], record);
            cache.set(id, batchedData[id]);
        });
        const database = Object.fromEntries(cache);
        saveDatabase(database);
        batchQueue = [];
    }
    setTimeout(processBatchQueue, batchInterval);
};

// Initialize cache and start batch processing
initializeCache();
processBatchQueue();

/**
 * Save a record with advanced conflict resolution and batching.
 * @param {string} id - The unique identifier for the record.
 * @param {Object} newRecord - The new record to save.
 */
const saveWith2030DataResolution = (id, newRecord) => {
    if (!cache.has(id)) {
        newRecord.version = 1;
        cache.set(id, newRecord);
    } else {
        const existingRecord = cache.get(id);
        newRecord = resolveConflicts(existingRecord, newRecord);
        cache.set(id, newRecord);
    }
    batchQueue.push({ id, record: newRecord });
};

// Export the functions for public use
module.exports = {
    loadDatabase,
    saveWith2030DataResolution
};
