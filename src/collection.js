const fs = require('fs').promises;
const path = require('path');
const { generateId, validateData, STATUS_CODES } = require('./utils');
const SyncManager = require('./sync');

class Collection {
  constructor(name, dbPath, config = {}) {
    this.name = name;
    this.filePath = path.join(dbPath, `${name}.json`);
    this.cache = new Map();
    this.cacheLimit = config.cacheLimit || 100;
    this.index = new Map();
    this.indexFields = new Set();
    this.writeBuffer = [];
    this.writeBufferTimeout = null;
    this.maxSizeBytes = config.maxSizeBytes || 10 * 1024 * 1024; // 10MB default
    this.maxConcurrent = config.maxConcurrent || 10; // Max concurrent operations
    this.activeOperations = 0;
    this.logger = config.logger || console;
    this.syncManager = new SyncManager(this);
  }

  async init() {
    try {
      await fs.access(this.filePath);
      const data = await fs.readFile(this.filePath, 'utf8');
      const records = JSON.parse(data || '[]');
      this.buildIndex(records);
    } catch (err) {
      if (err.code === 'ENOENT') {
        await fs.writeFile(this.filePath, '[]');
      } else {
        throw err;
      }
    }
  }

  async checkConcurrency() {
    if (this.activeOperations >= this.maxConcurrent) {
      this.logger.warn('Too many concurrent operations');
      return { status: STATUS_CODES.TOO_MANY_CONNECTIONS, data: null };
    }
    this.activeOperations++;
    return null;
  }

  async checkSize() {
    try {
      const stats = await fs.stat(this.filePath);
      if (stats.size > this.maxSizeBytes) {
        this.logger.warn('Database size limit exceeded');
        return { status: STATUS_CODES.SIZE_LIMIT_EXCEEDED, data: null };
      }
      return null;
    } catch (err) {
      this.logger.error(`Error checking file size: ${err.message}`);
      return { status: STATUS_CODES.SIZE_LIMIT_EXCEEDED, data: null };
    }
  }

  buildIndex(records) {
    if (this.indexFields.size === 0) return;
    records.forEach(record => {
      this.indexFields.forEach(field => {
        if (record[field]) {
          this.index.set(`${field}:${record[field]}`, record.id);
        }
      });
    });
  }

  async addIndexField(field) {
    this.indexFields.add(field);
    const records = await this.readAll();
    this.buildIndex(records);
  }

  async readAll() {
    try {
      const data = await fs.readFile(this.filePath, 'utf8');
      return JSON.parse(data || '[]');
    } catch (err) {
      this.logger.error(`Error reading ${this.filePath}: ${err.message}`);
      return [];
    }
  }

  async flushWrites() {
    if (this.writeBufferTimeout) {
      clearTimeout(this.writeBufferTimeout);
      const latestRecords = this.writeBuffer[this.writeBuffer.length - 1];
      if (latestRecords) {
        await fs.writeFile(this.filePath, JSON.stringify(latestRecords, null, 2));
      }
      this.writeBuffer = [];
      this.writeBufferTimeout = null;
    }
  }

  async writeAll(records) {
    const sizeCheck = await this.checkSize();
    if (sizeCheck) return sizeCheck;

    if (process.env.NODE_ENV === 'test') {
      await fs.writeFile(this.filePath, JSON.stringify(records, null, 2));
      return { status: STATUS_CODES.SUCCESS, data: null };
    }

    this.writeBuffer.push(records);
    if (!this.writeBufferTimeout) {
      this.writeBufferTimeout = setTimeout(async () => {
        const latestRecords = this.writeBuffer[this.writeBuffer.length - 1];
        try {
          await fs.writeFile(this.filePath, JSON.stringify(latestRecords, null, 2));
          this.writeBuffer = [];
          this.writeBufferTimeout = null;
          this.logger.info(`Wrote ${latestRecords.length} records to ${this.filePath}`);
        } catch (err) {
          this.logger.error(`Failed to write to ${this.filePath}: ${err.message}`);
          throw err;
        }
      }, 100);
    }
    return { status: STATUS_CODES.SUCCESS, data: null };
  }

  async insert(data) {
    const concurrencyCheck = await this.checkConcurrency();
    if (concurrencyCheck) return concurrencyCheck;

    try {
      if (!validateData(data)) {
        this.logger.warn('Invalid data provided for insert');
        return { status: STATUS_CODES.INVALID_DATA, data: null };
      }
      const record = { ...data, id: generateId() };
      const records = await this.readAll();
      records.push(record);
      const writeResult = await this.writeAll(records);
      if (writeResult.status.code !== STATUS_CODES.SUCCESS.code) return writeResult;

      if (this.cache.size >= this.cacheLimit) {
        this.cache.delete(this.cache.keys().next().value);
      }
      this.cache.set(record.id, record);

      this.indexFields.forEach(field => {
        if (record[field]) {
          this.index.set(`${field}:${record[field]}`, record.id);
        }
      });

      this.logger.info(`Inserted record with id ${record.id}`);
      return { status: STATUS_CODES.SUCCESS, data: record };
    } finally {
      this.activeOperations--;
    }
  }

  async findById(id) {
    const concurrencyCheck = await this.checkConcurrency();
    if (concurrencyCheck) return concurrencyCheck;

    try {
      if (this.cache.has(id)) {
        this.logger.info(`Cache hit for id ${id}`);
        return { status: STATUS_CODES.SUCCESS, data: this.cache.get(id) };
      }
      const records = await this.readAll();
      const record = records.find(r => r.id === id);
      if (record) {
        this.cache.set(id, record);
        this.logger.info(`Found record with id ${id}`);
        return { status: STATUS_CODES.SUCCESS, data: record };
      }
      this.logger.warn(`Record not found with id ${id}`);
      return { status: STATUS_CODES.NOT_FOUND, data: null };
    } finally {
      this.activeOperations--;
    }
  }

  async findByField(field, value) {
    const concurrencyCheck = await this.checkConcurrency();
    if (concurrencyCheck) return concurrencyCheck;

    try {
      if (this.indexFields.has(field)) {
        const id = this.index.get(`${field}:${value}`);
        if (id) {
          this.logger.info(`Index hit for ${field}:${value}`);
          return this.findById(id);
        }
      }
      const records = await this.readAll();
      const record = records.find(r => r[field] === value);
      if (record) {
        this.logger.info(`Found record with ${field}:${value}`);
        return { status: STATUS_CODES.SUCCESS, data: record };
      }
      this.logger.warn(`Record not found with ${field}:${value}`);
      return { status: STATUS_CODES.NOT_FOUND, data: null };
    } finally {
      this.activeOperations--;
    }
  }

  async update(id, updates) {
    const concurrencyCheck = await this.checkConcurrency();
    if (concurrencyCheck) return concurrencyCheck;

    try {
      if (!validateData(updates)) {
        this.logger.warn('Invalid updates provided');
        return { status: STATUS_CODES.INVALID_DATA, data: null };
      }
      this.cache.delete(id); // Clear cache to avoid stale data
      const records = await this.readAll();
      const index = records.findIndex(r => r.id === id);
      if (index === -1) {
        this.logger.warn(`Update failed: No record found with id ${id}`);
        return { status: STATUS_CODES.NOT_FOUND, data: null };
      }
      const updatedRecord = { ...records[index], ...updates, id };
      records[index] = updatedRecord;
      const writeResult = await this.writeAll(records);
      if (writeResult.status.code !== STATUS_CODES.SUCCESS.code) return writeResult;

      this.cache.set(id, updatedRecord);
      this.indexFields.forEach(field => {
        if (updatedRecord[field]) {
          this.index.set(`${field}:${updatedRecord[field]}`, id);
        }
      });

      this.logger.info(`Updated record with id ${id}`);
      return { status: STATUS_CODES.SUCCESS, data: updatedRecord };
    } finally {
      this.activeOperations--;
    }
  }

  async delete(id) {
    const concurrencyCheck = await this.checkConcurrency();
    if (concurrencyCheck) return concurrencyCheck;

    try {
      const records = await this.readAll();
      const index = records.findIndex(r => r.id === id);
      if (index === -1) {
        this.logger.warn(`Delete failed: No record found with id ${id}`);
        return { status: STATUS_CODES.NOT_FOUND, data: false };
      }
      const [deleted] = records.splice(index, 1);
      const writeResult = await this.writeAll(records);
      if (writeResult.status.code !== STATUS_CODES.SUCCESS.code) return writeResult;

      this.cache.delete(id);
      this.indexFields.forEach(field => {
        if (deleted[field]) {
          this.index.delete(`${field}:${deleted[field]}`);
        }
      });

      this.logger.info(`Deleted record with id ${id}`);
      return { status: STATUS_CODES.SUCCESS, data: true };
    } finally {
      this.activeOperations--;
    }
  }

  configureSync({ apiUrl, logger }) {
    this.syncManager.configure({ apiUrl, logger });
  }

  async syncPull() {
    return this.syncManager.pull();
  }

  async syncPush() {
    return this.syncManager.push();
  }
}

module.exports = Collection;
