const postgres = require('postgres');
const utils = require('./utils');

class Sqlclient {
  constructor(opt) {
    if (opt.bxpwd) {
      opt.password = utils.decryptBx(opt.bxpwd);
    }
    this.opt = opt;
    this.sql = null;
    this.pool = null;
  }

  async connect() {
    const hosts = this.opt.host || this.opt.hosts;
    if (!hosts) throw new Error('Параметр host (или hosts) обязателен');

    const port = this.opt.port || 5432;
    const hostList = hosts.split(',')
      .map(h => h.trim())
      .filter(Boolean)
      .map(h => `${h}:${port}`)
      .join(',');

    const url = `postgres://${hostList}/${this.opt.database}`;

    const options = {
      username: this.opt.user || this.opt.username,
      password: this.opt.password,

      target_session_attrs: this.opt.target_session_attrs || 'primary',

      max: this.opt.max || 20,
      idle_timeout: this.opt.idle_timeout || 45,
      max_lifetime: this.opt.max_lifetime || 180,
      connect_timeout: this.opt.connect_timeout || this.opt.connectionTimeoutMillis || 10,
      query_timeout: this.opt.query_timeout || 30,

      ssl: this.opt.ssl || false,
      prepare: true,

      types: {
        bigint: {
          to: 20,
          from: [20],
          serialize: (value) => String(value),
          parse: (value) => Number(value)
        }
      }
    };

    this.sql = postgres(url, options);

    this.pool = {
      query: (config, callback) => this._safeExecute(config, callback)
    };

    //console.log(`Sqlclient connected → target_session_attrs=${options.target_session_attrs}, max_lifetime=${options.max_lifetime}s`);
    return Promise.resolve();
  }

  // === ЕДИНЫЙ безопасный метод ===
  async _safeExecute(config, callback) {
    const text = typeof config === 'string' 
      ? config 
      : (config?.text || config?.sql || '');
    
    const values = Array.isArray(config?.values) ? config.values : [];

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const result = await this.sql.unsafe(text, values);
        const res = {
          rows: result,
          rowCount: result.count ?? result.length,
          command: text.trim().split(' ')[0].toUpperCase(),
        };
        if (callback) callback(null, res);
        return res;

      } catch (err) {
        if (attempt === 1 && this._isFailoverError(err)) {
          console.log(`Failover detected (attempt ${attempt}) → reconnecting...`);
          await this._reconnect();
          continue;
        }

        if (callback) callback(err);
        throw err;
      }
    }
  }

  _isFailoverError(err) {
    return (
      err.code === '25006' ||
      err.code === '57P03' ||
      err.code === '08006' ||
      err.code === 'ECONNRESET' ||
      (err.message && (
        err.message.toLowerCase().includes('read-only') ||
        err.message.toLowerCase().includes('recovery is in progress') ||
        err.message.toLowerCase().includes('target session attrs')
      ))
    );
  }

  async _reconnect() {
    this.close();
    await new Promise(r => setTimeout(r, 1000));
    await this.connect();
  }

  prepareQuery(queryObj) {
    let queryStr;
    if (typeof queryObj === 'string') {
      queryStr = queryObj;
    } else if (queryObj.sql) {
      queryStr = queryObj.sql;
    } else if (queryObj.ids) {
      const idarr = Array.isArray(queryObj.ids) ? queryObj.ids : queryObj.ids.split(',');
      queryStr = utils.getQueryStrId(queryObj, idarr);
    } else {
      queryStr = '';
    }
    return queryStr;
  }

  // Теперь и прямой query тоже защищён
  async query(queryStr) {
    if (!queryStr || typeof queryStr !== 'string') {
      throw new Error('Invalid queryStr');
    }
    // Оборачиваем в безопасный метод
    const res = await this._safeExecute(queryStr);
    return res.rows;
  }

  close() {
    if (this.sql) {
      this.sql.end({ timeout: 5 }).catch(() => {});
      this.sql = null;
      this.pool = null;
    }
  }
}

module.exports = Sqlclient;