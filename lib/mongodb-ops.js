'use strict';

const MongoClient = require('mongodb').MongoClient;

// writeConcern is not supported in this class
//
class MongoDBOps {
  static dbClient;

  constructor(connString) {
    if (!connString) { throw new Error("missing-connection-string"); }
    this.connString = connString;
  }

  // dbClient will be at static level for reusability
  //
  static async openDBConn(connString, poolSize) {
    if (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state === "connected") { return Promise.resolve(); }
    if (!connString) { throw new Error("missing-connection-string"); }

    MongoDBOps.dbClient = await MongoClient.connect(connString, { 
      poolSize: poolSize ? poolSize : 5,
      useNewUrlParser: true, 
      useUnifiedTopology: true,
    });

    return Promise.resolve();
  }

  async openDBConn() {
    return Promise.resolve(await MongoDBOps.openDBConn(this.connString));
  }

  // Get documents from MongoDB
  // https://docs.mongodb.com/manual/reference/method/db.collection.find/
  // https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/
  // 
  // queryExp - Specifies selection filter using query operators - https://docs.mongodb.com/manual/reference/operator/
  // projection - https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection
  // sort - https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort
  // pagination - { startIndex:<integer>, endIndex:<integer> }
  // isGetCount - boolean. True is to get the number of doc count based on the queryExp
  //
  static async getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount, connString) {
    if (!MongoDBOps.dbClient || (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state !== "connected")) { await MongoDBOps.openDBConn(connString); }   // reopen the db connection if it is not connected
    let docs, db = MongoDBOps.dbClient.db();  // Create a new Db instance sharing the current socket connections

    if (isAggregate) { docs = await db.collection(collectionName).aggregate(queryExp).toArray(); }
    else {
      queryExp = typeof queryExp !== 'undefined' ? queryExp : {};
      projection = typeof projection !== 'undefined' ? { projection: projection } : { projection: {} };
      let skip = pagination ? pagination.startIndex - 1 : undefined;
      let limit = pagination ? pagination.endIndex - pagination.startIndex + 1 : undefined;
      
      if (isGetCount) { docs = await db.collection(collectionName).countDocuments(queryExp); }
      else if (sort && pagination) { docs = await db.collection(collectionName).find(queryExp, projection).sort(sort).skip(skip).limit(limit).toArray(); }
      else if (!sort && pagination) { docs = await db.collection(collectionName).find(queryExp, projection).skip(skip).limit(limit).toArray(); }
      else if (sort && !pagination) { docs = await db.collection(collectionName).find(queryExp, projection).sort(sort).toArray(); }
      else { docs = await db.collection(collectionName).find(queryExp, projection).toArray(); }
    }

    return Promise.resolve(docs);
  }

  async getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, queryExp, isAggregate, projection, sort, pagination, isGetCount, this.connString));
  }

  // write document to MongoDB
  // https://docs.mongodb.com/manual/reference/method/db.collection.insertOne/
  // https://docs.mongodb.com/manual/reference/method/db.collection.replaceOne/
  // https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/
  //
  // type: insertOne, replaceOne or updateOne
  // 
  static async writeData(type, collectionName, doc, filter, connString) {
    if (!MongoDBOps.dbClient || (MongoDBOps.dbClient && MongoDBOps.dbClient.topology.s.state !== "connected")) { await MongoDBOps.openDBConn(connString); }   // reopen the db connection if it is not connected

    try {
      let result, db = MongoDBOps.dbClient.db();  // Create a new Db instance sharing the current socket connections

      switch(type) {
        case "insertOne": result = await db.collection(collectionName).insertOne(doc); break;
        case "replaceOne": result = await db.collection(collectionName).replaceOne(filter, doc); break;
        case "updateOne": result = await db.collection(collectionName).updateOne(filter, doc); break;
        default: throw new Error("invalid-writeData-type");
      }
      return Promise.resolve(result);
    }
    catch (err) {
      let errmsg = typeof err.errmsg === 'undefined' ? err.message : err.errmsg;
      return Promise.reject(errmsg);
    }
  }

  async writeData(type, collectionName, doc, filter) {
    return Promise.resolve(await MongoDBOps.writeData(type, collectionName, doc, filter, this.connString));
  }

  // write document to MongoDB via BulkOps / BulkWrite
  // - https://mongodb.github.io/node-mongodb-native/3.6/api/BulkOperationBase.html
  // - https://docs.mongodb.com/manual/reference/method/db.collection.bulkWrite/
  //
  // type: insertBulk, replaceBulk, updateBulk, allBulk
  // 
  static async writeBulkData(type, collectionName, docs, ordered, connString) {
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
        case "allBulk":
          // allowed bulkWrite operations include insertOne, replaceOne, updateOne, updateMany, deleteOne, deleteMany
          break;
        default: throw new Error("invalid-writeBulkData-type");
      }
      result = await db.collection(collectionName).bulkWrite(docs, { ordered: ordered });
      return Promise.resolve(result);
    }
    catch (err) {
      let errmsg = typeof err.errmsg === 'undefined' ? err.message : err.errmsg;
      errmsg = err.result ? err.result : errmsg;
      return Promise.resolve(errmsg);
    }
  }

  async writeBulkData(type, collectionName, docs, ordered) {
    return Promise.resolve(await MongoDBOps.writeBulkData(type, collectionName, docs, ordered, this.connString));
  }
}

module.exports = MongoDBOps;