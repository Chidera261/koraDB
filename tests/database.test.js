const Database = require('../src/database');
const path = require('path');
const fs = require('fs').promises;
const os = require('os');
const nock = require('nock');

describe('koraDB', () => {
  let db;
  const dbPath = path.join(os.homedir(), 'test-db');
  const logger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  };

  beforeEach(async () => {
    db = new Database(dbPath, {
      maxSizeBytes: 1024 * 1024, // 1MB for tests
      maxConcurrent: 2,
      logger
    });
    await db.init();
  });

  afterEach(async () => {
    for (const collection of db.collections.values()) {
      await collection.flushWrites();
    }
    await fs.rm(dbPath, { recursive: true, force: true });
  });

  test('should create and insert into a collection', async () => {
    const users = await db.getCollection('users');
    const result = await users.insert({ name: 'Alice', age: 30 });
    expect(result.status.code).toBe(200);
    expect(result.data.id).toBeDefined();
    expect(result.data.name).toBe('Alice');
    expect(result.data.age).toBe(30);
    expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('Inserted record'));
  });

  test('should find a record by ID', async () => {
    const users = await db.getCollection('users');
    const insertResult = await users.insert({ name: 'Bob', age: 25 });
    const found = await users.findById(insertResult.data.id);
    expect(found.status.code).toBe(200);
    expect(found.data.name).toBe('Bob');
  });

  test('should update a record', async () => {
    const users = await db.getCollection('users');
    const insertResult = await users.insert({ name: 'Charlie', age: 40 });
    const updated = await users.update(insertResult.data.id, { age: 41 });
    expect(updated.status.code).toBe(200);
    expect(updated.data.age).toBe(41);
  });

  test('should delete a record', async () => {
    const users = await db.getCollection('users');
    const insertResult = await users.insert({ name: 'Dave', age: 50 });
    const deleted = await users.delete(insertResult.data.id);
    expect(deleted.status.code).toBe(200);
    expect(deleted.data).toBe(true);
    const found = await users.findById(insertResult.data.id);
    expect(found.status.code).toBe(404);
  });

  test('should support indexing for faster queries', async () => {
    const users = await db.getCollection('users');
    await users.addIndexField('name');
    const insertResult = await users.insert({ name: 'Eve', age: 28 });
    const found = await users.findByField('name', 'Eve');
    expect(found.status.code).toBe(200);
    expect(found.data.name).toBe('Eve');
  });

  test('should generate unique IDs', async () => {
    const { generateId } = require('../src/utils');
    const id1 = generateId();
    const id2 = generateId();
    expect(id1).not.toBe(id2);
    expect(typeof id1).toBe('string');
    expect(id1.length).toBe(32);
  });

  test('should enforce max concurrent operations', async () => {
    const users = await db.getCollection('users');
    const promises = [
      users.insert({ name: 'Test1', age: 20 }),
      users.insert({ name: 'Test2', age: 21 }),
      users.insert({ name: 'Test3', age: 22 })
    ];
    const results = await Promise.all(promises);
    expect(results[2].status.code).toBe(429);
    expect(logger.warn).toHaveBeenCalledWith('Too many concurrent operations');
  });

  test('should sync with external API', async () => {
    const users = await db.getCollection('users');
    const mockData = [{ name: 'Synced', age: 35 }];
    nock('https://api.example.com')
      .get('/data')
      .reply(200, mockData);

    users.configureSync({ apiUrl: 'https://api.example.com/data', logger });
    const result = await users.syncPull();
    expect(result.status.code).toBe(200);
    expect(result.data).toEqual(mockData);
    const found = await users.findByField('name', 'Synced');
    expect(found.status.code).toBe(200);
    expect(found.data.name).toBe('Synced');
  });
});
