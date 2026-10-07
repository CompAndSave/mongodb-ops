'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { MongoMemoryServer } = require('mongodb-memory-server');
const { MongoDBToolSet } = require('..');
const validator = require('./fixtures/vendor-orders-validator.json');

const order = (id) => ({
  _id: id,
  ts: new Date('2026-01-01T00:00:00.000Z'),
  order_id: id.replace(/^EDI-/, ''),
  vendor: 'acm',
  submitted_by: 'test',
  status: 'new',
  address: {
    company_name: '',
    name: 'Test Recipient',
    address_line1: '1 Main St',
    address_line2: '',
    city: 'Los Angeles',
    state_province: 'CA',
    postal_code: '90001',
    country_code: 'US'
  }
});

async function rejection(promise) {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  assert.fail('Expected the real MongoDB write to reject');
}

test('real MongoDB write errors retain their diagnostic detail', async (t) => {
  const mongod = await MongoMemoryServer.create();
  try {
    const connString = mongod.getUri('write_error_test');
    const client = await MongoDBToolSet.getDbClient(connString);
    await client.db().createCollection('orders', {
      validator,
      validationLevel: 'strict',
      validationAction: 'error'
    });
    await MongoDBToolSet.insertOne('orders', order('EDI-1001'), connString);

    const single = await rejection(MongoDBToolSet.updateOne(
      'orders', { $set: { invoice_date: null } }, { _id: 'EDI-1001' }, connString
    ));
    const bulk = await rejection(MongoDBToolSet.updateBulkUnOrdered('orders', [{
      filter: { _id: 'EDI-1001' },
      update: { $set: { invoice_date: null } }
    }], connString));
    const duplicate = await rejection(MongoDBToolSet.insertOne(
      'orders', order('EDI-1001'), connString
    ));

    await t.test('G0: single-write rejection is an Error, not a string or copied object', () => {
      assert.ok(single instanceof Error, 'single-write rejection must be an Error');
    });
    await t.test('G0: bulk-write rejection is an Error, not a BulkWriteResult or copied object', () => {
      assert.ok(bulk instanceof Error, 'bulk-write rejection must be an Error');
    });
    await t.test('G1: updateOne retains the real validator field diagnostic', () => {
      const rules = single?.errInfo?.details?.schemaRulesNotSatisfied;
      assert.ok(Array.isArray(rules), 'updateOne must retain errInfo.details.schemaRulesNotSatisfied');
      assert.equal(single.code, 121);
      assert.match(JSON.stringify(rules), /"propertyName":"invoice_date"/);
    });
    await t.test('G2: bulk rejection exposes writeErrors directly with the validator detail', () => {
      assert.ok(Array.isArray(bulk?.writeErrors), 'bulk rejection must expose writeErrors directly');
      assert.equal(bulk.writeErrors.length, 1);
      assert.equal(bulk.writeErrors[0].code, 121);
      const rules = bulk.writeErrors[0].err.errInfo.details.schemaRulesNotSatisfied;
      assert.match(JSON.stringify(rules), /"propertyName":"invoice_date"/);
    });
    await t.test('G3: duplicate-key rejection retains MongoDB code 11000', () => {
      assert.equal(duplicate?.code, 11000, 'duplicate-key rejection must retain code 11000');
    });
  } finally {
    try {
      await MongoDBToolSet.closeDBConn();
    } finally {
      await mongod.stop();
    }
  }
});
