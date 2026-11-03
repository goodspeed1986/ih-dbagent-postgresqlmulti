/**
 * PostgreSQL client на базе postgres.js
 * Полная поддержка HA + автоматическое переподключение при failover/switchover
 */

const postgres = require('postgres');

module.exports = {
  pool: null,
  sql: null,
  dbopt: null,
  logger: console,

  async createPoolToDatabase(dbopt, logger = console) {
    if (!dbopt.database || !(dbopt.user || dbopt.username) || !dbopt.password) {
      throw new Error('Обязательные параметры: database, user/username, password');
    }

    this.dbopt = { ...dbopt };
    this.logger = logger;

    function buildPostgresMultiHost(hostsStr, port = 5432) {
      if (typeof hostsStr !== 'string' || !hostsStr.trim()) {
        throw new Error('Параметр host (или hosts) обязателен');
      }
      const hosts = hostsStr
        .split(',')
        .map(h => h.trim())
        .filter(Boolean)
        .map(h => `${h}:${port}`);
      return `postgres://${hosts.join(',')}`;
    }

    const url = buildPostgresMultiHost(dbopt.host || dbopt.hosts, dbopt.port || 5432);

    const options = {
      database: dbopt.database,
      username: dbopt.username || dbopt.user,
      password: dbopt.password,

      target_session_attrs: dbopt.target_session_attrs || 'primary',

      max: dbopt.max || 20,
      idle_timeout: dbopt.idle_timeout || 45,
      max_lifetime: dbopt.max_lifetime || 180,
      connect_timeout: dbopt.connect_timeout || 10,
      query_timeout: dbopt.query_timeout || 30,

      ssl: dbopt.ssl || false,
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

    try {
      this.sql = postgres(url, options);

      // Проверяем реальное соединение
      await this.sql`SELECT 1`;

      this.pool = {
        query: (sqlOrConfig, values, callback) =>
          this._safeExecute(sqlOrConfig, values, callback)
      };

      this.run = async (sql, values = []) => this._safeExecute(sql, values);
      this.query = async (sql, values = []) => {
        const res = await this._safeExecute(sql, values);
        return res.rows;
      };

      this.logger.log(
        `PostgreSQL pool created → target_session_attrs=${options.target_session_attrs}, max_lifetime=${options.max_lifetime}s`
      );
      return this.pool;
    } catch (err) {
      this.logger.log(`Невозможно установить соединение с PostgreSQL: ${err.message}`);
      try { await this.sql?.end({ timeout: 1 }); } catch (_) { }
      this.sql = null;
      this.pool = null;
      throw err;
    }
  },

  // === ЕДИНЫЙ метод с обработкой ошибок и reconnect ===
  async _safeExecute(sqlOrConfig, values, callback) {
    let text = typeof sqlOrConfig === 'string' ? sqlOrConfig : sqlOrConfig?.text;
    let params = Array.isArray(values) ? values : (sqlOrConfig?.values || []);

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const result = await this.sql.unsafe(text, params);
        const res = {
          rows: result,
          rowCount: result.count ?? result.length,
          command: text.trim().split(' ')[0].toUpperCase(),
        };
        if (callback) callback(null, res);
        return res;
      } catch (err) {
        if (attempt === 1 && this._isFailoverError(err)) {
          this.logger.log(`Failover detected (attempt ${attempt}) → reconnecting...`, 2);
          await this._reconnect();
          continue;
        }

        if (callback) callback(err);
        throw err;
      }
    }
  },

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
  },

  async _reconnect() {
    try {
      await this.sql?.end({ timeout: 2 });
    } catch (e) { }

    await new Promise(r => setTimeout(r, 1000));
    return this.createPoolToDatabase(this.dbopt, this.logger);
  },

  /**
  * Максимально быстрая bulk-вставка
  * Использует prepared statement + unnest
  */
async insertRecords(table, records) {
  if (!records?.length) return { rowCount: 0 };

  const tsArr  = records.map(r => r.ts);
  const idArr  = records.map(r => r.id);
  const valArr = records.map(r => r.val);
  const qArr   = records.map(r => r.q != null ? r.q : 0);

  let query;

  if (table === 'strrecords') {
    // val — text / string
    query = this.sql`
      INSERT INTO ${this.sql(table)} (ts, id, val, q, tstz)
      SELECT * FROM unnest(
        ${tsArr}::bigint[],
        ${idArr}::int[],
        ${valArr}::text[],
        ${qArr}::smallint[],
        ${tsArr}::timestamptz[]
      ) AS t(ts, id, val, q, tstz)
    `;
  } else if (table === 'records') {
    // val — real
    query = this.sql`
      INSERT INTO ${this.sql(table)} (ts, id, val, q, tstz)
      SELECT * FROM unnest(
        ${tsArr}::bigint[],
        ${idArr}::int[],
        ${valArr}::real[],
        ${qArr}::smallint[],
        ${tsArr}::timestamptz[]
      ) AS t(ts, id, val, q, tstz)
    `;
  } else {
    // на будущее: можно добавить другие таблицы
    throw new Error(`Unsupported table for bulk insert: ${table}`);
  }

  try {
    const result = await query;
    return {
      rows: result,
      rowCount: result.count ?? result.length,
      command: 'INSERT'
    };
  } catch (err) {
    if (this._isFailoverError(err)) {
      this.logger.log(`Failover detected during bulk insert → reconnecting...`, 2);
      await this._reconnect();

      const result = await query;
      return {
        rows: result,
        rowCount: result.count ?? result.length,
        command: 'INSERT'
      };
    }
    throw err;
  }
},

  async end() {
    if (this.sql) {
      await this.sql.end({ timeout: 5 });
      this.sql = null;
      this.pool = null;
    }
  }
};