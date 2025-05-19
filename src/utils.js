const crypto = require('crypto');

const STATUS_CODES = {
  SUCCESS: { code: 200, message: 'Operation successful' },
  NOT_FOUND: { code: 404, message: 'Record not found' },
  INVALID_DATA: { code: 400, message: 'Invalid data' },
  TOO_MANY_CONNECTIONS: { code: 429, message: 'Too many concurrent connections' },
  SIZE_LIMIT_EXCEEDED: { code: 413, message: 'Database size limit exceeded' },
  SYNC_FAILED: { code: 502, message: 'Failed to sync with external API' }
};

module.exports = {
  generateId() {
    return crypto.randomBytes(16).toString('hex');
  },
  validateData(data) {
    return data && typeof data === 'object' && !Array.isArray(data);
  },
  STATUS_CODES
};
