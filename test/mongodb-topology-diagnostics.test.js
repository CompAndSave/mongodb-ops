'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { inspect } = require('node:util');
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

test.afterEach(() => {
  mongodb.MongoClient.connect = originalConnect;
  delete require.cache[require.resolve('../lib/mongodb-ops')];
});

function privateConnection(hosts, scheme = 'mongodb') {
  const user = 'diaguser';
  const pass = ['not', 'a', 'real', 'pw'].join('-');
  const database = 'diagnostic_private_database';
  const appName = 'diagnostic_private_app';
  const uri = `${scheme}://${user}:${pass}@${hosts}/${database}?appName=${appName}`;
  return { uri, secrets: [user, pass, database, appName, uri] };
}

function captureConsoleError(t, secrets = []) {
  const original = console.error;
  const calls = [];
  console.error = (...args) => calls.push(args);
  t.after(() => {
    console.error = original;
    // Check every argument, including unprefixed lines and object arguments.
    for (const args of calls) {
      for (const arg of args) {
        const text = typeof arg === 'string' ? arg : inspect(arg, { depth: null, customInspect: false });
        for (const secret of secrets) {
          assert.equal(text.includes(secret), false, 'console.error leaked private connection content');
        }
        assert.doesNotMatch(text, /(?:mongodb(?:\+srv)?|https?):\/\//i);
      }
    }
  });
  return calls;
}

async function silentServer() {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return {
    address: `127.0.0.1:${server.address().port}`,
    async close() {
      for (const socket of sockets) { socket.destroy(); }
      await new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()));
    }
  };
}

async function rejection(promise) {
  let rejection;
  await assert.rejects(promise, (err) => {
    rejection = err;
    return true;
  });
  return rejection;
}

async function realSelectionError() {
  const server = await silentServer();
  try {
    return await rejection(originalConnect.call(mongodb.MongoClient,
      `mongodb://${server.address}/?directConnection=true`, { serverSelectionTimeoutMS: 300 }));
  } finally {
    await server.close();
  }
}

function topologyError(message = 'selection failed') {
  return new mongodb.MongoServerSelectionError(message, {
    type: 'ReplicaSetNoPrimary',
    setName: 'rs_allowed',
    servers: new Map([['127.0.0.1:27017', { type: 'Unknown', roundTripTime: -1, error: null }]])
  });
}

test('T1: pending host logs once across concurrent callers and the write catch', async (t) => {
  const server = await silentServer();
  const connection = privateConnection(server.address);
  const uri = `${connection.uri}&directConnection=true`;
  const calls = captureConsoleError(t, connection.secrets);
  const MongoDBOps = freshMongoDBOps(originalConnect);
  MongoDBOps.clientOptions = { serverSelectionTimeoutMS: 1000 };
  try {
    const errors = await Promise.all(Array.from({ length: 2 }, () =>
      rejection(MongoDBOps.writeData('insertOne', 'log', { a: 1 }, undefined, uri))));
    assert.ok(errors[0] instanceof Error);
    assert.equal(errors[0].message, 'Server selection timed out after 1000 ms');
    assert.equal(errors[1], errors[0]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].length, 1);
    assert.match(calls[0][0], /^\[mongodb-ops\] connect MongoServerSelectionError: Server selection timed out after 1000 ms \| topology=/);
    assert.ok(calls[0][0].includes(`${server.address} type=Unknown rtt=-1 error=none`));
  } finally {
    try { await MongoDBOps.closeDBConn(); } finally { await server.close(); }
  }
});

test('T2: refused hosts retain per-host errors and the allowed replica-set name', async (t) => {
  const connection = privateConnection('127.0.0.1:1,127.0.0.1:2');
  const calls = captureConsoleError(t, connection.secrets);
  const MongoDBOps = freshMongoDBOps(originalConnect);
  MongoDBOps.clientOptions = { serverSelectionTimeoutMS: 1000 };
  try {
    const err = await rejection(MongoDBOps.writeData('insertOne', 'log', {}, undefined,
      `${connection.uri}&replicaSet=rs0`));
    assert.ok(err instanceof Error);
    assert.match(err.message, /ECONNREFUSED/);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].length, 1);
    assert.match(calls[0][0], /topology=ReplicaSetNoPrimary setName=rs0 servers=2 \|/);
    for (const port of [1, 2]) {
      assert.ok(calls[0][0].includes(`127.0.0.1:${port} type=Unknown rtt=-1 error=MongoNetworkError: connect ECONNREFUSED 127.0.0.1:${port}`));
    }
  } finally {
    await MongoDBOps.closeDBConn();
  }
});

test('T3: post-connect single and bulk failures log without replacing either Error', async (t) => {
  const connection = privateConnection('post-connect.example.test');
  const calls = captureConsoleError(t, connection.secrets);
  const singleError = await realSelectionError();
  const bulkError = await realSelectionError();
  const MongoDBOps = freshMongoDBOps(async () => ({
    db: () => ({ collection: () => ({
      insertOne: async () => { throw singleError; },
      bulkWrite: async () => { throw bulkError; }
    }) }),
    close: async () => {}
  }));
  try {
    const single = await rejection(MongoDBOps.writeData('insertOne', 'log', {}, undefined, connection.uri));
    const bulk = await rejection(MongoDBOps.writeBulkData('insertBulk', 'log', [{}], false, connection.uri));
    assert.equal(single, singleError);
    assert.equal(bulk, bulkError);
    assert.equal(single.message, 'Server selection timed out after 300 ms');
    assert.equal(bulk.message, 'Server selection timed out after 300 ms');
    assert.equal(calls.length, 2);
    assert.match(calls[0][0], /^\[mongodb-ops\] writeData /);
    assert.match(calls[1][0], /^\[mongodb-ops\] writeBulkData /);
  } finally {
    await MongoDBOps.closeDBConn();
  }
});

test('T4: errors without topology are unchanged and produce no console output', async (t) => {
  const connection = privateConnection('no-topology.example.test');
  const calls = captureConsoleError(t, connection.secrets);
  const suppliedError = new Error('boom');
  const MongoDBOps = freshMongoDBOps(async () => { throw suppliedError; });
  try {
    const err = await rejection(MongoDBOps.writeData('insertOne', 'log', {}, undefined, connection.uri));
    assert.equal(err, suppliedError);
    assert.equal(err.message, 'boom');
    assert.equal(calls.length, 0);
  } finally {
    await MongoDBOps.closeDBConn();
  }
});

test('P1-3: redact URI and userinfo text before truncating or logging any error message', async (t) => {
  const connection = privateConnection('private.example.test', 'mongodb+srv');
  const plainUri = 'mongodb://private.example.test/private_database?appName=private_query';
  const httpUri = 'https://private.example.test/private_path';
  const userinfo = `${connection.secrets[0]}:${connection.secrets[1]}@private.example.test`;
  const calls = captureConsoleError(t, [...connection.secrets, 'private_database', 'private_query', 'private_path']);
  const suppliedError = topologyError(`failed ${connection.uri}\nretry ${plainUri} via ${httpUri} userinfo ${userinfo}`);
  suppliedError.reason.servers.get('127.0.0.1:27017').error = new mongodb.MongoNetworkError(
    `host failure ${connection.uri}\nretry ${plainUri} userinfo ${userinfo}`);
  suppliedError.reason.servers.set('127.0.0.1:27018', {
    type: 'Unknown', roundTripTime: -1,
    error: new mongodb.MongoNetworkError(`host failure ${'x'.repeat(160)} ${connection.uri}`)
  });
  const originalMessage = suppliedError.message;
  const originalHostMessage = suppliedError.reason.servers.get('127.0.0.1:27017').error.message;
  const MongoDBOps = freshMongoDBOps(async () => { throw suppliedError; });
  try {
    assert.equal(await rejection(MongoDBOps.getDbClient(connection.uri)), suppliedError);
    assert.equal(calls.length, 1);
    assert.match(calls[0][0], /\[REDACTED_URI\]/);
    assert.match(calls[0][0], /\[REDACTED_USERINFO\]/);
    assert.match(calls[0][0], /setName=rs_allowed/);
    assert.match(calls[0][0], /127\.0\.0\.1:27017 type=Unknown rtt=-1/);
    assert.doesNotMatch(calls[0][0], /[\r\n]/);
    assert.equal(suppliedError.message, originalMessage);
    assert.equal(suppliedError.reason.servers.get('127.0.0.1:27017').error.message, originalHostMessage);
  } finally {
    await MongoDBOps.closeDBConn();
  }
});

for (const failure of ['console.error', 'message getter', 'host message getter', 'servers getter']) {
  test(`P1-2: throwing ${failure} preserves the connect Error and permits reconnection`, async (t) => {
    const connection = privateConnection('guard.example.test');
    const calls = captureConsoleError(t, connection.secrets);
    const suppliedError = topologyError();
    const loggerError = new Error('diagnostics failed');
    if (failure === 'message getter') {
      Object.defineProperty(suppliedError, 'message', { get() { throw loggerError; } });
    } else if (failure === 'host message getter') {
      const hostError = new Error('host failed');
      Object.defineProperty(hostError, 'message', { get() { throw loggerError; } });
      suppliedError.reason.servers.get('127.0.0.1:27017').error = hostError;
    } else if (failure === 'servers getter') {
      Object.defineProperty(suppliedError.reason, 'servers', { get() { throw loggerError; } });
    }
    const client = { close: async () => {} };
    let attempts = 0;
    const MongoDBOps = freshMongoDBOps(async () => {
      if (++attempts === 1) { throw suppliedError; }
      return client;
    });
    const cachedAtLog = [];
    if (failure === 'console.error') {
      console.error = (...args) => {
        calls.push(args);
        cachedAtLog.push(MongoDBOps.dbClients.size);
        throw loggerError;
      };
    }
    try {
      const err = await rejection(MongoDBOps.getDbClient(connection.uri));
      assert.ok(err === suppliedError, 'diagnostics must preserve the original Error by identity');
      assert.equal(MongoDBOps.dbClients.size, 0);
      assert.ok(cachedAtLog.every((size) => size === 0), 'evict the failed connection before logging');
      assert.equal(await MongoDBOps.getDbClient(connection.uri), client);
      assert.equal(attempts, 2);
    } finally {
      await MongoDBOps.closeDBConn();
    }
  });
}

test('P1-2: a throwing log sink cannot replace post-connect single or bulk Errors', async (t) => {
  const connection = privateConnection('write-guard.example.test');
  const calls = captureConsoleError(t, connection.secrets);
  console.error = (...args) => { calls.push(args); throw new Error('log sink failed'); };
  const singleError = topologyError('single failed');
  const bulkError = topologyError('bulk failed');
  const MongoDBOps = freshMongoDBOps(async () => ({
    db: () => ({ collection: () => ({
      insertOne: async () => { throw singleError; },
      bulkWrite: async () => { throw bulkError; }
    }) }),
    close: async () => {}
  }));
  try {
    assert.equal(await rejection(MongoDBOps.writeData('insertOne', 'log', {}, undefined, connection.uri)), singleError);
    assert.equal(await rejection(MongoDBOps.writeBulkData('insertBulk', 'log', [{}], false, connection.uri)), bulkError);
  } finally {
    await MongoDBOps.closeDBConn();
  }
});
