const axios = require('axios');
const { STATUS_CODES } = require('./utils');

class SyncManager {
  constructor(collection) {
    this.collection = collection;
    this.apiUrl = null;
    this.logger = console;
  }

  configure({ apiUrl, logger }) {
    this.apiUrl = apiUrl;
    this.logger = logger || console;
  }

  async pull() {
    if (!this.apiUrl) {
      this.logger.error('Sync failed: No API URL configured');
      return { status: STATUS_CODES.SYNC_FAILED, data: null };
    }
    try {
      const response = await axios.get(this.apiUrl);
      const records = Array.isArray(response.data) ? response.data : [];
      for (const record of records) {
        await this.collection.insert(record);
      }
      this.logger.info(`Synced ${records.length} records from ${this.apiUrl}`);
      return { status: STATUS_CODES.SUCCESS, data: records };
    } catch (err) {
      this.logger.error(`Sync failed: ${err.message}`);
      return { status: STATUS_CODES.SYNC_FAILED, data: null };
    }
  }

  async push() {
    if (!this.apiUrl) {
      this.logger.error('Sync failed: No API URL configured');
      return { status: STATUS_CODES.SYNC_FAILED, data: null };
    }
    try {
      const records = await this.collection.readAll();
      await axios.post(this.apiUrl, records);
      this.logger.info(`Pushed ${records.length} records to ${this.apiUrl}`);
      return { status: STATUS_CODES.SUCCESS, data: records };
    } catch (err) {
      this.logger.error(`Sync failed: ${err.message}`);
      return { status: STATUS_CODES.SYNC_FAILED, data: null };
    }
  }
}

module.exports = SyncManager;
