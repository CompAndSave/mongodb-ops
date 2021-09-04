'use strict';

const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const DEFAULT_POOLSIZE = 5;

class MongoDBOps {
  /**
   * @class
   * @classdesc MongoDB operations - Note: writeConcern is not supported in this class
   * 
   * @param {string} connString Database connection string
   */
  constructor(connString) {
    if (!connString) { throw new Error("missing-connection-string"); }
    this.connString = connString;
  }

  /**
   * Static method - Get ObjectId instance from string
   * 
   * @param {string} id Object ID string
   * @returns {object} ObjectId instance
   */
  static getObjectId(id) { return new ObjectID(id); }

  /**
   * Instance method - Get ObjectId instance from string
   * 
   * @param {string} id Object ID string
   * @returns {object} ObjectId instance
   */
  getObjectId(id) { return new ObjectID(id); }

  /**
   * Static method - Open database connection
   * 
   * @param {string} connString Database connection string
   * @param {number} [poolSize] Database client pool size
   * @returns {promise}
   */
  static async openDBConn(connString, poolSize = DEFAULT_POOLSIZE) {
    // dbClient will be at static level for reusability
    //
    if (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state === "connected") { return Promise.resolve(); }
    if (!connString) { throw new Error("missing-connection-string"); }

    MongoDBOps.dbClient = await MongoClient.connect(connString, { 
      poolSize: poolSize !== DEFAULT_POOLSIZE ? poolSize : MongoDBOps.poolSize || DEFAULT_POOLSIZE,
      useNewUrlParser: true, 
      useUnifiedTopology: true,
    });

    return Promise.resolve();
  }

  /**
   * Instance method - Open database connection
   * 
   * @param {number} [poolSize] Database client pool size
   * @returns {promise}
   */
  async openDBConn(poolSize = DEFAULT_POOLSIZE) {
    return Promise.resolve(await MongoDBOps.openDBConn(this.connString, poolSize));
  }

  /**
   * Static method - Close database connection
   * @returns {promise}
   */
  static async closeDBConn() {
    if (MongoDBOps.dbClient) { await MongoDBOps.dbClient.close(); }
    return Promise.resolve();
  }

  /**
   * Instance method - Close database connection
   * @returns {promise}
   */
  async closeDBConn() {
    if (MongoDBOps.dbClient) { await MongoDBOps.dbClient.close(); }
    return Promise.resolve();
  }

  /**
   * Static method - Get documents from MongoDB
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/}
   * 
   * @param {string} collectionName Collection name
   * @param {object} [queryExp] Query Specifies selection filter using query operators - {@link https://docs.mongodb.com/manual/reference/operator/}
   * @param {boolean} [isAggregate=false] Set true to use the aggregation
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort}
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {boolean} [isGetCount=false] Set true to get the number of doc count based on the queryExp
   * @param {string} connString Database connection string
   * @returns {promise} Promise with object array
   */
  static async getData(collectionName, queryExp, isAggregate = false, projection, sort, pagination, isGetCount = false, connString) {
    if (!MongoDBOps.dbClient || (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state !== "connected")) { await MongoDBOps.openDBConn(connString); }   // reopen the db connection if it is not connected
    let docs, db = MongoDBOps.dbClient.db();  // Create a new Db instance sharing the current socket connections

    if (isAggregate) { docs = await db.collection(collectionName).aggregate(queryExp).toArray(); }
    else {
      queryExp = queryExp || {};
      projection = { projection: projection || {} };

      let skip, limit;
      if (pagination && Number.isInteger(pagination.startIndex) && Number.isInteger(pagination.endIndex)) {
        if (pagination.endIndex < pagination.startIndex) { return Promise.resolve([]); }

        let startIndex = pagination.startIndex < 1 ? 1 : pagination.startIndex;
        skip = startIndex - 1;
        limit = (pagination.endIndex - startIndex + 1) < 0 ? 0 : pagination.endIndex - startIndex + 1;
      }

      if (isGetCount) { docs = await db.collection(collectionName).countDocuments(queryExp); }
      else if (sort && pagination) { docs = await db.collection(collectionName).find(queryExp, projection).sort(sort).skip(skip).limit(limit).toArray(); }
      else if (!sort && pagination) { docs = await db.collection(collectionName).find(queryExp, projection).skip(skip).limit(limit).toArray(); }
      else if (sort && !pagination) { docs = await db.collection(collectionName).find(queryExp, projection).sort(sort).toArray(); }
      else { docs = await db.collection(collectionName).find(queryExp, projection).toArray(); }
    }

    return Promise.resolve(docs);
  }

  /**
   * Instance method - Get documents from MongoDB
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/}
   * 
   * @param {string} collectionName Collection name
   * @param {object} [queryExp] Query Specifies selection filter using query operators - {@link https://docs.mongodb.com/manual/reference/operator/}
   * @param {boolean} [isAggregate=false] Set true to use the aggregation
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort}
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {boolean} [isGetCount=false] Set true to get the number of doc count based on the queryExp
   * @returns {promise} Promise with object array
   */
  async getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount, this.connString));
  }

  /**
   * Static method - Write document to MongoDB
   * 
   * References:
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.insertOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.replaceOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.deleteOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.deleteMany/}
   * 
   * @param {string} type Write type - insertOne, replaceOne, updateOne, updateMany, deleteOne, deleteMany
   * @param {string} collectionName Collection name
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async writeData(type, collectionName, doc, filter, connString) {
    if (!MongoDBOps.dbClient || (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state !== "connected")) { await MongoDBOps.openDBConn(connString); }   // reopen the db connection if it is not connected

    try {
      let result, db = MongoDBOps.dbClient.db();  // Create a new Db instance sharing the current socket connections

      switch(type) {
        case "insertOne": result = await db.collection(collectionName).insertOne(doc); break;
        case "replaceOne": result = await db.collection(collectionName).replaceOne(filter, doc); break;
        case "updateOne": result = await db.collection(collectionName).updateOne(filter, doc); break;
        case "updateMany": result = await db.collection(collectionName).updateMany(filter, doc); break;
        case "deleteOne": result = await db.collection(collectionName).deleteOne(filter); break;
        case "deleteMany": result = await db.collection(collectionName).deleteMany(filter); break;
        default: throw new Error("invalid-writeData-type");
      }
      return Promise.resolve(result);
    }
    catch (err) { return Promise.reject(err.errmsg || err.message || err); }
  }

  /**
   * Instance method - Write document to MongoDB
   * 
   * References:
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.insertOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.replaceOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.deleteOne/}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.deleteMany/}
   * 
   * @param {string} type Write type - insertOne, replaceOne, updateOne, updateMany, deleteOne, deleteMany
   * @param {string} collectionName Collection name
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @returns {promise}
   */
  async writeData(type, collectionName, doc, filter) {
    return Promise.resolve(await MongoDBOps.writeData(type, collectionName, doc, filter, this.connString));
  }

  /**
   * Static method - Write document to MongoDB via BulkOps / BulkWrite
   * 
   * References:
   * {@link https://mongodb.github.io/node-mongodb-native/3.6/api/BulkOperationBase.html}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.bulkWrite/}
   * 
   * @param {string} type Write type - insertBulk, replaceBulk, updateBulk, allBulk
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data documents array
   * @param {boolean} [ordered=false] Set true to use ordered bulkWrite
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async writeBulkData(type, collectionName, docs, ordered = false, connString) {
    if (!MongoDBOps.dbClient || (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state !== "connected")) { await MongoDBOps.openDBConn(connString); }   // reopen the db connection if it is not connected

    try {
      let result, db = MongoDBOps.dbClient.db();  // Create a new Db instance sharing the current socket connections

      switch(type) {
        case "insertBulk":
          for (let i = 0; i < docs.length; ++i) { docs[i] = { insertOne: { "document": docs[i] }}; }
          break;
        case "replaceBulk":
          // doc = {
          //   "filter": <document>,
          //   "replacement": <document>,
          //   "upsert": <boolean>,
          //   "collation": <document>,
          //   "hint": <document|string>
          // }
          for (let i = 0; i < docs.length; ++i) { docs[i] = { replaceOne: docs[i] }; }
          break;
        case "updateBulk":
          // doc = {
          //   "filter": <document>,
          //   "update": <document or pipeline>,
          //   "upsert": <boolean>,
          //   "collation": <document>,
          //   "arrayFilters": [ <filterdocument1>, ... ],
          //   "hint": <document|string>
          // }
          for (let i = 0; i < docs.length; ++i) { docs[i] = { updateOne: docs[i] }; }
          break;
        case "deleteBulk":
          // doc = {
          //   "filter": <document>,
          //   "collation": <document>
          // }
          for (let i = 0; i < docs.length; ++i) { docs[i] = { deleteOne: docs[i] }; }
          break;
        case "allBulk":
          // allowed bulkWrite operations include insertOne, replaceOne, updateOne, updateMany, deleteOne, deleteMany
          break;
        default: throw new Error("invalid-writeBulkData-type");
      }
      result = await db.collection(collectionName).bulkWrite(docs, { ordered: ordered });
      return Promise.resolve(result);
    }
    catch (err) { return Promise.reject(err.result || err.errmsg || err.message); }
  }

  /**
   * Instance method - Write document to MongoDB via BulkOps / BulkWrite
   * 
   * References:
   * {@link https://mongodb.github.io/node-mongodb-native/3.6/api/BulkOperationBase.html}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.bulkWrite/}
   * 
   * @param {string} type Write type - insertBulk, replaceBulk, updateBulk, allBulk
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data documents array
   * @param {boolean} [ordered=false] Set true to use ordered bulkWrite
   * @returns {promise}
   */
  async writeBulkData(type, collectionName, docs, ordered = false) {
    return Promise.resolve(await MongoDBOps.writeBulkData(type, collectionName, docs, ordered, this.connString));
  }
}

module.exports = MongoDBOps;