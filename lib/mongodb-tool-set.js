'use strict';

const MongoDBOps = require('./mongodb-ops');

class MongoDBToolSet extends MongoDBOps {
  /**
   * @class
   * @classdesc MongoDBToolSet Operations
   * 
   * @param {string} collectionName Collection name
   * @param {string} connString Database connection string
   */
  constructor(collectionName, connString) {
    super(connString);
    this.collectionName = collectionName;
    this.connString = connString;
  }

  /**
   * Static method - Get data by ID
   * 
   * @param {string} collectionName Collection name
   * @param {string|number} id _id
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {string} connString Database connection string
   * @returns {promise} Promise with object array
   */
  static async getDataByID(collectionName, id, projection, connString) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, { _id: id }, false, projection, undefined, undefined, undefined, connString));
  }

  /**
   * Instance method - Get data by ID
   * 
   * @param {string|number} id _id
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @returns {promise} Promise with object array
   */
  async getDataByID(id, projection) { return Promise.resolve(await MongoDBToolSet.getDataByID(this.collectionName, id, projection, this.connString)); }

  /**
   * Static method - Get data by filter
   * 
   * @param {string} collectionName Collection name 
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {string} connString Database connection string
   * @param {object} [collation] Collation {@link https://www.mongodb.com/docs/manual/reference/collation/#std-label-collation-document-fields}
   * @returns {promise} Promise with object array
   */
  static async getDataByFilter(collectionName, filter, projection, sort, pagination, connString, collation) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, filter, false, projection, sort, pagination, undefined, connString, collation));
  }

  /**
   * Instance method - Get data by filter
   * 
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {object} [collation] Collation {@link https://www.mongodb.com/docs/manual/reference/collation/#std-label-collation-document-fields}
   * @returns {promise} Promise with object array
   */
  async getDataByFilter(filter, projection, sort, pagination, collation) { return Promise.resolve(await MongoDBToolSet.getDataByFilter(this.collectionName, filter, projection, sort, pagination, this.connString, collation)); }

  /**
   * Static method - Get data by aggregate
   * 
   * @param {string} collectionName Collection name 
   * @param {array} pipeline Aggregate pipeline {@link https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/}
   * @param {string} connString Database connection string
   * @param {object} [options] Aggregate options {@link https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/}
   * @returns {promise} Promise with object array
   */
  static async getDataByAggregate(collectionName, pipeline, connString, options) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, pipeline, true, undefined, undefined, undefined, undefined, connString, undefined, options));
  }

  /**
   * Instance method - Get data by aggregate
   * 
   * @param {array} pipeline Aggregate pipeline {@link https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/}
   * @param {object} [options] Aggregate options {@link https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/}
   * @returns {promise} Promise with object array
   */
  async getDataByAggregate(pipeline, options) { return Promise.resolve(await MongoDBToolSet.getDataByAggregate(this.collectionName, pipeline, this.connString, options)); }

  /**
   * Static method - Get data count by query
   * 
   * @param {string} collectionName Collection name
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @param {string} connString Database connection string
   * @returns {promise} Promise with data count
   */
  static async getDataCount(collectionName, filter, connString) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, filter, false, undefined, undefined, undefined, true, connString));
  }

  /**
   * Instance method - Get data count by query
   * 
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @returns {promise} Promise with data count
   */
  async getDataCount(filter) {
    return Promise.resolve(await MongoDBToolSet.getDataCount(this.collectionName, filter, this.connString));
  }

  /**
   * Static method - List record with optional no. of data counts
   * 
   * @param {string} collectionName Collection name
   * @param {object} [query] Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`  
   * @param {boolean|string} [showCount] Set true to return the data with total_count which is the record count on the data by query
   * @param {string} connString Database connection string
   * @returns {promise} Promise with data object or array
   */
  static async list(collectionName, query, projection, sort, pagination, showCount, connString) {
    if (["true", true].includes(showCount)) {
      const [total_count, data] = await Promise.all([
        MongoDBToolSet.getDataCount(collectionName, query, connString),
        MongoDBToolSet.getDataByFilter(collectionName, query, projection, sort, pagination, connString)
      ]);
      return Promise.resolve({ total_count, data });
    }
    else { return Promise.resolve(await MongoDBToolSet.getDataByFilter(collectionName, query, projection, sort, pagination, connString)); }
  }

  /**
   * Instance method - List record with optional no. of data counts
   * 
   * @param {object} [query] Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`  
   * @param {boolean|string} [showCount=false] Set true to return the data with total_count which is the record count on the data by query
   * @returns {promise} Promise with data count
   */
  async list(query, projection, sort, pagination, showCount) {
    return Promise.resolve(await MongoDBToolSet.list(this.collectionName, query, projection, sort, pagination, showCount, this.connString));
  }

  /**
   * Static method - Get all data
   * 
   * @param {string} collectionName Collection name 
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection} 
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {string} connString Database connection string 
   * @returns {promise} Promise with object array
   */
  static async getAllData(collectionName, projection, sort, pagination, connString) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, {}, false, projection, sort, pagination, false, connString));
  }

  /**
   * Instance method - Get all data
   * 
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection} 
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @returns {promise} Promise with object array
   */
  async getAllData(projection, sort, pagination) { return Promise.resolve(await MongoDBToolSet.getAllData(this.collectionName, projection, sort, pagination, this.connString)); }

  /**
   * Static method - Insert one document to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc Data document
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async insertOne(collectionName, doc, connString) { return Promise.resolve(await MongoDBOps.writeData("insertOne", collectionName, doc, undefined, connString)); }

  /**
   * Instance method - Insert one document to database
   * 
   * @param {object} doc Data document
   * @returns {promise}
   */
  async insertOne(doc) { return Promise.resolve(await MongoDBToolSet.insertOne(this.collectionName, doc, this.connString)); }

  /**
   * Static method - Insert multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data document array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async insertBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("insertBulk", collectionName, docs, true, connString)); }

  /**
   * Instance method - Insert multiple documents to database in ordered way
   * 
   * @param {Array} docs Data document array
   * @returns {promise}
   */
  async insertBulkOrdered(docs) { return Promise.resolve(await MongoDBToolSet.insertBulkOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - Insert multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data document array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async insertBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("insertBulk", collectionName, docs, false, connString)); }

  /**
   * Instance method - Insert multiple documents to database in unordered way
   * 
   * @param {Array} docs Data document array
   * @returns {promise}
   */
  async insertBulkUnOrdered(docs) { return Promise.resolve(await MongoDBToolSet.insertBulkUnOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - Replace one document to database
   * 
   * @param {string} collectionName Collection name 
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceOne(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("replaceOne", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Replace one document to database
   * 
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @returns {promise}
   */
  async replaceOne(doc, filter) { return Promise.resolve(await MongoDBToolSet.replaceOne(this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Replace multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs ReplaceOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("replaceBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - Replace multiple documents to database in ordered way
   * 
   * @param {Array} docs ReplaceOne object array
   * @returns {promise}
   */
  async replaceBulkOrdered(docs) { return Promise.resolve(await MongoDBToolSet.replaceBulkOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - Replace multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs ReplaceOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("replaceBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - Replace multiple documents to database in unordered way
   * 
   * @param {Array} docs ReplaceOne object array
   * @returns {promise}
   */
  async replaceBulkUnOrdered(docs) { return Promise.resolve(await MongoDBToolSet.replaceBulkUnOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - Update one document to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async updateOne(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("updateOne", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Update one document to database
   * 
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @returns {promise}
   */
  async updateOne(doc, filter) { return Promise.resolve(await MongoDBToolSet.updateOne(this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Update many documents to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async updateMany(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("updateMany", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Update many documents to database
   * 
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @returns {promise}
   */
  async updateMany(doc, filter) { return Promise.resolve(await MongoDBToolSet.updateMany(this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Update multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs UpdateOne object array 
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async updateBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("updateBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - Update multiple documents to database in ordered way
   * 
   * @param {Array} docs UpdateOne object array 
   * @returns {promise}
   */
  async updateBulkOrdered(docs) { return Promise.resolve(await MongoDBToolSet.updateBulkOrdered(this.collectionName, docs, this.connString)); }
  
  /**
   * Static method - Update multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs UpdateOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async updateBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("updateBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - Update multiple documents to database in unordered way
   * 
   * @param {Array} docs UpdateOne object array
   * @returns {promise}
   */
  async updateBulkUnOrdered(docs) { return Promise.resolve(await MongoDBToolSet.updateBulkUnOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - Delete one document at database
   * 
   * @param {string} collectionName Collection name
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async deleteOne(collectionName, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("deleteOne", collectionName, undefined, filter, connString)); }

  /**
   * Instance method - Delete one document at database
   * 
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @returns {promise}
   */
  async deleteOne(filter) { return Promise.resolve(await MongoDBToolSet.deleteOne(this.collectionName, filter, this.connString)); }

  /**
   * Static method - Delete many document at database
   * 
   * @param {string} collectionName Collection name
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async deleteMany(collectionName, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("deleteMany", collectionName, undefined, filter, connString)); }

  /**
   * Instance method - Delete many document at database
   * 
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @returns {promise}
   */
  async deleteMany(filter) { return Promise.resolve(await MongoDBToolSet.deleteMany(this.collectionName, filter, this.connString)); }

  /**
   * Static method - Delete multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs DeleteOne object array 
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async deleteBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("deleteBulk", collectionName, docs, true, connString)); }
  
   /**
    * Instance method - Delete multiple documents to database in ordered way
    * 
    * @param {Array} docs DeleteOne object array 
    * @returns {promise}
    */
  async deleteBulkOrdered(docs) { return Promise.resolve(await MongoDBToolSet.deleteBulkOrdered(this.collectionName, docs, this.connString)); }
   
   /**
    * Static method - Delete multiple documents to database in unordered way
    * 
    * @param {string} collectionName Collection name
    * @param {Array} docs DeleteOne object array
    * @param {string} connString Database connection string
    * @returns {promise}
    */
  static async deleteBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("deleteBulk", collectionName, docs, false, connString)); }
   
   /**
    * Instance method - Delete multiple documents to database in unordered way
    * 
    * @param {Array} docs DeleteOne object array
    * @returns {promise}
    */
  async deleteBulkUnOrdered(docs) { return Promise.resolve(await MongoDBToolSet.deleteBulkUnOrdered(this.collectionName, docs, this.connString)); }

  /**
   * Static method - BulkWrite operations to database in ordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs BulkWrite object array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async allBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("allBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - BulkWrite operations to database in ordered way
   * 
   * @param {Array} docs BulkWrite object array
   * @returns {promise}
   */
  async allBulkOrdered(docs) { return Promise.resolve(await MongoDBToolSet.allBulkOrdered(this.collectionName, docs, this.connString)); }
  
  /**
   * Static method - BulkWrite operations to database in unordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs BulkWrite object array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async allBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("allBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - BulkWrite operations to database in unordered way
   * 
   * @param {Array} docs BulkWrite object array
   * @returns {promise}
   */
  async allBulkUnOrdered(docs) { return Promise.resolve(await MongoDBToolSet.allBulkUnOrdered(this.collectionName, docs, this.connString)); }
}

module.exports = MongoDBToolSet;