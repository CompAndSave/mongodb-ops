'use strict';

const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectId;

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
   * Static method - Get DB client - It supports multiple db client with different connection string. Active db client will be reused for better performance
   * 
   * @param {string} connString Database connection string
   * @returns {promise} Promise with db client
   */
  static async getDbClient(connString) {
    if (!connString) { throw new Error("missing-connection-string"); }

    if (!Array.isArray(MongoDBOps.dbClientList)) { MongoDBOps.dbClientList = []; }

    for (let item of MongoDBOps.dbClientList) {
      if (item.s.url === connString) {
        if (item.topology.s.state !== "connected") {
          item = await MongoClient.connect(connString);
        }

        return Promise.resolve(item);
      }
    }

    const newDbClient = await MongoClient.connect(connString);
    MongoDBOps.dbClientList.push(newDbClient);

    return Promise.resolve(newDbClient);
  }

  /**
   * Static method - Close database connection
   * @returns {promise}
   */
  static async closeDBConn() {
    if (Array.isArray(MongoDBOps.dbClientList)) {
      for (const item of MongoDBOps.dbClientList) {
        await item.close();
      }
    }
    return Promise.resolve();
  }

  /**
   * Instance method - Close database connection
   * @returns {promise}
   */
  async closeDBConn() {
    return Promise.resolve(await MongoDBOps.closeDBConn());
  }

  /**
   * Static method - Get estimated document count of a collection
   * 
   * @param {string} collectionName Collection Name
   * @param {string} connString Database connection string
   */
  static async getCollectionCount(collectionName, connString) {
    const db = (await MongoDBOps.getDbClient(connString)).db();
    return Promise.resolve(await db.collection(collectionName).estimatedDocumentCount());
  }

  /**
   * Instance method - Get estimated document count of a collection
   * 
   * @param {string} collectionName Collection Name
   * @param {string} connString Database connection string
   */
  async getCollectionCount(collectionName) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, this.connString));
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
   * @param {object} [collation] Collation {@link https://www.mongodb.com/docs/manual/reference/collation/#std-label-collation-document-fields}
   * @param {object} [options] Aggregate options {@link https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/}
   * @returns {promise} Promise with object array
   */
  static async getData(collectionName, queryExp, isAggregate = false, projection, sort, pagination, isGetCount = false, connString, collation, options) {   
    const db = (await MongoDBOps.getDbClient(connString)).db();
    
    if (isAggregate) { return Promise.resolve(await db.collection(collectionName).aggregate(queryExp, options).toArray()); }

    queryExp = queryExp || {};
    if (isGetCount) { return Promise.resolve(await db.collection(collectionName).countDocuments(queryExp)); }

    const { skip, limit } = parsePagination(pagination);
    if (limit < 1) { return Promise.resolve([]); }

    projection = { projection: projection || {} };
    
    let data = db.collection(collectionName).find(queryExp, projection);
    if (sort) { data = data.sort(sort); }
    if (pagination) { data = data.skip(skip).limit(limit); }
    if (collation) { data = data.collation(collation); }

    return Promise.resolve(await data.toArray());
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
   * @param {object} [collation] Collation {@link https://www.mongodb.com/docs/manual/reference/collation/#std-label-collation-document-fields}
   * @param {object} [options] Aggregate options {@link https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/}
   * @returns {promise} Promise with object array
   */
  async getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount, collation, options) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount, this.connString, collation, options));
  }

  /**
   * Static method - Use altas search at MongoDB
   * {@link https://www.mongodb.com/docs/atlas/atlas-search/}
   * 
   * @param {string} connString Database connection string
   * @param {string} collectionName Collection name
   * @param {object} search $search object - {@link https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/}
   * @param {object} [obj]
   * @param {object} [obj.projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [obj.sort] Sort {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort}
   * @param {object} [obj.pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {boolean} [isGetCount=true] Set true to get the number of total matching docs and current page number
   * @returns {promise} Promise with object or object array
   */
  static async search(connString, collectionName, search, { projection, sort, pagination }={}, isGetCount = true) {
    if ([connString, collectionName, search].includes(undefined)) { return Promise.reject("Connection string, collection name and search cannot be undefined"); }

    const { skip, limit } = parsePagination(pagination);
    if (limit < 1) { return Promise.resolve([]); }

    const payload = [
      { $search: search },
      { $addFields: { score: { $meta: "searchScore" }}},
    ];

    if (projection) { payload.push({ $project: projection }); }
    if (sort) { payload.push({ $sort: sort }); }

    const skipLimit = [];
    if (skip) { skipLimit.push({ $skip: skip }); }
    if (limit) { skipLimit.push({ $limit: limit }); }

    if (isGetCount) {
      payload.push({ 
        $facet: {
          metadata: [{ $count: "total" }, { $addFields: { page: Math.ceil((skip + 1) / limit) }}],
          data: skipLimit,
        }
      });
    }
    else { payload.push(...skipLimit); }

    const result = await MongoDBOps.getData(collectionName, payload, true, undefined, undefined, undefined, undefined, connString);

    return Promise.resolve(isGetCount ? result[0] : result);
  }

  /**
   * Instance method - Use altas search at MongoDB
   * {@link https://www.mongodb.com/docs/atlas/atlas-search/}
   * 
   * @param {string} collectionName Collection name
   * @param {object} search $search object - {@link https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/}
   * @param {object} [obj]
   * @param {object} [obj.projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [obj.sort] Sort {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort}
   * @param {object} [obj.pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {boolean} [isGetCount=true] Set true to get the number of total matching docs and current page number
   * @returns {promise} Promise with object or object array
   */
  async search(collectionName, search, { projection, sort, pagination }={}, isGetCount = true) {
    return Promise.resolve(await MongoDBOps.search(this.connString, collectionName, search, { projection, sort, pagination }, isGetCount));
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
    try {
      const db = (await MongoDBOps.getDbClient(connString)).db();

      let result;
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
    try {
      const db = (await MongoDBOps.getDbClient(connString)).db();

      let result;
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

/**
 * Parse the pagaintion and return skip and limit values
 * 
 * @param {object} obj
 * @param {number} [obj.startIndex] Start index
 * @param {number} [obj.endIndex] Start index
 * @returns {object} Object with skip and limit values
 */
const parsePagination = ({ startIndex, endIndex }={})=> {
  startIndex = +startIndex; endIndex = +endIndex;
  startIndex = startIndex < 1 ? 1 : startIndex;

  return {
    skip: Number.isInteger(startIndex) ? startIndex - 1 : undefined,
    limit: Number.isInteger(startIndex) && Number.isInteger(endIndex) ? ((endIndex - startIndex + 1) < 0 ? 0 : endIndex - startIndex + 1) : undefined
  };
}