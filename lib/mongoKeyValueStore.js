/*
 Copyright 2018 Zhao Chaoyi All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0

*/

const debug = require('debug')('MongoKVS');
const mongodb = require('mongodb');

const { MongoClient } = mongodb;

module.exports = class MongoKVS {
  constructor(options) {
    debug('Enter Constructor, options are %j', options);
    if (!options.dbname) {
      throw new Error('Missing Required dbname in options');
    }
    if (!options.collectionName) {
      throw new Error('Missing Required collectionName in options');
    }
    if (!options.url) {
      throw new Error('Missing Required url in options');
    }
    if (options.auth) {
      this.auth = options.auth;
    }
    this.url = options.url;
    this.dbname = options.dbname;
    this.collectionName = options.collectionName;
    return Promise.resolve(this);
  }

  connect() {
    debug('Connect to %s', this.url);
    return new Promise((resolve, reject) => {
      MongoClient.connect(this.url, { useNewUrlParser: true }, (err, client) => {
        if (err) {
          reject(err);
        }
        this.client = client;
        this.db = client.db(this.dbname);
        this.collection = this.db.collection(this.collectionName);
        resolve();
      });
    });
  }

  // eslint-disable-next-line consistent-return
  close() {
    if (this.client.isConnected()) {
      this.collection = null;
      this.db = null;
      return this.client.close();
    }
  }

  async getValue(name) {
    debug('getValue with name %s', name);
    try {
      if (!this.client || !this.client.isConnected() || !this.collection) {
        await this.connect();
      }
      const res = await this.collection.findOne({ _id: name });
      await this.close();

      debug('getValue find -> %j <- from db', res);
      if (!res) {
        return null;
      }
      return res.member;
    } catch (e) {
      debug('getValue error %s', e.message);
      throw e;
    }
  }

  async setValue(name, value) {
    try {
      debug('setValue with name %s, value %j', name, value);
      if (!this.client || !this.client.isConnected() || !this.collection) {
        await this.connect();
      }
      await this.collection.findOneAndUpdate(
        { _id: name },
        { _id: name, member: value },
        { upsert: true },
      );
      await this.close();
      return;
    } catch (e) {
      debug('setValue error %s', e.message);
      throw e;
    }
  }
};
