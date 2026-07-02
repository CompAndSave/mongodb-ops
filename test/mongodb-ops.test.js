'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const mongodb = require('mongodb');

const originalConnect = mongodb.MongoClient.connect;

function freshMongoDBOps(connectImpl) {
  delete require.cache[require.resolve('../lib/mongodb-ops')];
  mongodb.MongoClient.connect = connectImpl;

  const MongoDBOps = require('../lib/mongodb-ops');
  MongoDBOps.dbClients = undefined;
  MongoDBOps.dbClientList = undefined;
  MongoDBOps.clientOptions = undefined;

  return MongoDBOps;
}

function createClient(name) {
  const closeCalls = [];
  return {
    name,
    closeCalls,
    db() { return {}; },
    async close() { closeCalls.push(name); }
  };
}

test.afterEach(() => {
  mongodb.MongoClient.connect = originalConnect;
  delete require.cache[require.resolve('../lib/mongodb-ops')];
});

test('reuses one client for repeated calls with the same connection string', async () => {
  const calls = [];
  const client = createClient('same');
  const MongoDBOps = freshMongoDBOps(async (connString, options) => {
    calls.push({ connString, options });
    return client;
  });

  const first = await MongoDBOps.getDbClient('mongodb://example-a');
  const second = await MongoDBOps.getDbClient('mongodb://example-a');

  assert.equal(first, client);
  assert.equal(second, client);
  assert.equal(first, second);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.serverSelectionTimeoutMS, 8000);
  assert.equal(calls[0].options.retryWrites, true);
  assert.equal(calls[0].options.retryReads, true);
});

test('uses separate clients for different connection strings', async () => {
  const calls = [];
  const MongoDBOps = freshMongoDBOps(async (connString, options) => {
    calls.push({ connString, options });
    return createClient(connString);
  });

  const first = await MongoDBOps.getDbClient('mongodb://example-a');
  const second = await MongoDBOps.getDbClient('mongodb://example-b');

  assert.notEqual(first, second);
  assert.deepEqual(calls.map((call) => call.connString), ['mongodb://example-a', 'mongodb://example-b']);
});

test('dedupes concurrent cold-start connection attempts', async () => {
  const calls = [];
  const client = createClient('concurrent');
  let releaseConnect;
  const connectStarted = new Promise((resolve) => {
    releaseConnect = resolve;
  });
  const MongoDBOps = freshMongoDBOps(async (connString, options) => {
    calls.push({ connString, options });
    await connectStarted;
    return client;
  });

  const pending = Array.from({ length: 5 }, () => MongoDBOps.getDbClient('mongodb://example-a'));
  assert.equal(calls.length, 1);

  releaseConnect();
  const clients = await Promise.all(pending);

  assert.equal(calls.length, 1);
  assert.equal(new Set(clients).size, 1);
  assert.equal(clients[0], client);
});

test('evicts failed connection promises so a later call reconnects', async () => {
  let attempts = 0;
  const client = createClient('retry');
  const MongoDBOps = freshMongoDBOps(async () => {
    attempts += 1;
    if (attempts === 1) { throw new Error('connect failed'); }
    return client;
  });

  await assert.rejects(MongoDBOps.getDbClient('mongodb://example-a'), /connect failed/);
  const recovered = await MongoDBOps.getDbClient('mongodb://example-a');

  assert.equal(recovered, client);
  assert.equal(attempts, 2);
});

test('closeDBConn closes cached clients, clears the map, and reconnects next time', async () => {
  const calls = [];
  const clients = [];
  const MongoDBOps = freshMongoDBOps(async (connString, options) => {
    calls.push({ connString, options });
    const client = createClient(connString);
    clients.push(client);
    return client;
  });

  await MongoDBOps.getDbClient('mongodb://example-a');
  await MongoDBOps.getDbClient('mongodb://example-b');
  await MongoDBOps.closeDBConn();

  assert.equal(clients[0].closeCalls.length, 1);
  assert.equal(clients[1].closeCalls.length, 1);
  assert.equal(MongoDBOps.dbClients.size, 0);

  await MongoDBOps.getDbClient('mongodb://example-a');
  assert.equal(calls.length, 3);
});

test('closeDBConn also drains legacy dbClientList clients', async () => {
  const MongoDBOps = freshMongoDBOps(async () => createClient('unused'));
  const legacyClient = createClient('legacy');
  MongoDBOps.dbClientList = [legacyClient];

  await MongoDBOps.closeDBConn();

  assert.equal(legacyClient.closeCalls.length, 1);
  assert.deepEqual(MongoDBOps.dbClientList, []);
});

test('passes safe MongoDB driver options and permits consumer overrides', async () => {
  const calls = [];
  const client = createClient('options');
  const MongoDBOps = freshMongoDBOps(async (connString, options) => {
    calls.push({ connString, options });
    return client;
  });
  MongoDBOps.clientOptions = { serverSelectionTimeoutMS: 5000, maxPoolSize: 10 };

  await MongoDBOps.getDbClient('mongodb://example-a');

  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.serverSelectionTimeoutMS, 5000);
  assert.equal(calls[0].options.retryWrites, true);
  assert.equal(calls[0].options.retryReads, true);
  assert.equal(calls[0].options.maxPoolSize, 10);
});

