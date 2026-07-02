# mongodb-ops

Lightweight read/write helpers for MongoDB with a **shared, self-managing client**. The MongoDB client is cached at the class (process) level per connection string and reused across calls — you connect once and let the driver handle pooling, topology monitoring, and failover recovery.

> `writeConcern` is not configurable through this library.

## Install

```bash
npm install mongodb-ops
```

Requires Node.js 16+ and bundles the official `mongodb` driver (v6).

## Exports

```js
const { MongoDBOps, MongoDBToolSet } = require('mongodb-ops');
```

- **`MongoDBOps`** — low-level static/instance operations (`getData`, `search`, `writeData`, `writeBulkData`, `getCollectionCount`, `getDbClient`, `closeDBConn`, `getObjectId`).
- **`MongoDBToolSet`** — a higher-level, collection-scoped convenience class that extends `MongoDBOps` (get / insert / update / replace / delete + bulk variants).

## Quick start

### Collection-scoped (`MongoDBToolSet`)

```js
const { MongoDBToolSet } = require('mongodb-ops');

const connString = "mongodb+srv://USER:PASS@your-cluster.mongodb.net/your-db?retryWrites=true&w=majority";

// Instance style — bind a collection + connection once
const orders = new MongoDBToolSet('orders', connString);

await orders.insertOne({ number: 'PO-1001', status: 'open' });
const open = await orders.getDataByFilter({ status: 'open' }, { number: 1 }, { number: -1 });
await orders.updateOne({ $set: { status: 'closed' } }, { number: 'PO-1001' });
```

Every method also has a **static** form that takes the collection name and connection string explicitly — handy when wrapping it in your own model class:

```js
class Order extends MongoDBToolSet {
  static collectionName = 'orders';
  static connString = connString;

  static getById(id) {
    return MongoDBToolSet.getDataByID(this.collectionName, id, undefined, this.connString);
  }
}
```

### Low-level (`MongoDBOps`)

```js
const { MongoDBOps } = require('mongodb-ops');

const rows = await MongoDBOps.getData(
  'orders',
  { status: 'open' },              // query
  false,                           // isAggregate
  { number: 1 },                   // projection
  { number: -1 },                  // sort
  { startIndex: 1, endIndex: 20 }, // pagination (1-based, inclusive)
  false,                           // isGetCount
  connString
);
```

## Connection management

`getDbClient(connString)` maintains **one cached `MongoClient` per connection string** for the life of the process (kept in an internal `Map`; concurrent first-connects are de-duplicated). All operations reuse it, so there is no per-call connect overhead — and the driver's topology monitoring transparently rediscovers a new primary after a replica-set failover.

Clients are created with safe, failover-friendly defaults:

```js
{ serverSelectionTimeoutMS: 8000, retryWrites: true, retryReads: true }
```

**Override or extend** the driver options process-wide, before the first connection is made — your values are merged over the defaults:

```js
const { MongoDBOps } = require('mongodb-ops');
MongoDBOps.clientOptions = { maxPoolSize: 20 };
```

**Close** every cached connection (e.g. in a CLI or test teardown; long-running servers/Lambdas normally leave them open for reuse):

```js
await MongoDBOps.closeDBConn();
```

## API

### `MongoDBToolSet`

Constructor: `new MongoDBToolSet(collectionName, connString)`. Instance methods use the bound collection/connection; each has a matching static method that takes them as arguments.

| Read | Write | Bulk |
|---|---|---|
| `getDataByID` | `insertOne` | `insertBulkOrdered` / `insertBulkUnOrdered` |
| `getDataByFilter` | `replaceOne` | `replaceBulkOrdered` / `replaceBulkUnOrdered` |
| `getDataByAggregate` | `updateOne` / `updateMany` | `updateBulkOrdered` / `updateBulkUnOrdered` |
| `getDataCount` | `deleteOne` / `deleteMany` | `deleteBulkOrdered` / `deleteBulkUnOrdered` |
| `list` (rows + optional total) | | `allBulkOrdered` / `allBulkUnOrdered` |
| `getAllData` | | |

### `MongoDBOps`

`getData`, `search`, `writeData(type, …)`, `writeBulkData(type, …, ordered)`, `getCollectionCount`, `getObjectId`, `getDbClient`, `closeDBConn`.

## Changelog

### 0.11.1
- **Hardened connection caching.** The client cache is now keyed by connection string in a `Map` (previously matched against MongoDB driver internals), making client reuse reliable across driver versions.
- **De-duplicated concurrent cold-start connects** by caching the connect promise; a failed connect is evicted so the next call retries instead of caching a rejected promise.
- **Safer driver defaults:** `serverSelectionTimeoutMS: 8000`, `retryWrites: true`, `retryReads: true` — reads now recover automatically across a replica-set failover/election.
- **New `MongoDBOps.clientOptions`** hook to override/extend the driver options process-wide.
- `closeDBConn()` now drains the client `Map` (and any legacy cache).

### 0.11.0
- Added estimated collection count (`getCollectionCount`).

### Earlier
- `search`, collation, and aggregate options. See the git history for details.

## License

ISC
