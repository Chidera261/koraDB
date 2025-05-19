const fs = require('fs').promises;
const path = require('path');
const Collection = require('./collection');

class Database {
  constructor(dbPath, config = {}) {
    this.dbPath = dbPath;
    this.collections = new Map();
    this.config = config;
  }

  async init() {
    try {
      await fs.mkdir(this.dbPath, { recursive: true });
    } catch (err) {
      throw new Error(`Failed to create database directory: ${err.message}`);
    }
  }

  async getCollection(name) {
    if (this.collections.has(name)) {
      return this.collections.get(name);
    }
    const collection = new Collection(name, this.dbPath, this.config);
    await collection.init();
    this.collections.set(name, collection);
    return collection;
  }
}

module.exports = Database;
