/**
 * PostgreSQL client на базе postgres.js
 * Полная поддержка HA + автоматическое переподключение при failover/switchover
 */
let postgres;

async function loadPostgres() {
  if (!postgres) {
    const mod = await import('postgres');
    postgres = mod.default;
  }
  return postgres;
}

//const postgres = require('postgres');

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
      const postgres = await loadPostgres();
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
    this.logger.log("_isFailoverError " + err, 1)
    const networkErrors = [
      'ECONNRESET',
      'ECONNREFUSED',
      'ETIMEDOUT',
      'EHOSTUNREACH',
      'ENETUNREACH',
      'EPIPE',
      'CONNECT_TIMEOUT',
      '08000',
      '08001',
      '08003',
      '08006',
      '08007',
      '57P03',
      '25006'
    ];

    return (
      networkErrors.includes(err.code) ||
      (err.message && (
        err.message.toLowerCase().includes('read-only') ||
        err.message.toLowerCase().includes('recovery is in progress') ||
        err.message.toLowerCase().includes('connection')
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

    const tsArr = [];
    const idArr = [];
    const valArr = [];
    const qArr = [];
    const tstzArr = [];

    for (const r of records) {
      tsArr.push(r.ts);
      idArr.push(r.id);
      valArr.push(r.val);
      qArr.push(r.q ?? 0);
      tstzArr.push(new Date(Number(r.ts)).toISOString());
    }

    const execute = async () => {
      let query;

      if (table === 'strrecords') {
        query = this.sql`
          INSERT INTO ${this.sql(table)} (ts, id, val, q, tstz)
          SELECT * FROM unnest(
            ${tsArr}::bigint[],
            ${idArr}::int[],
            ${valArr}::text[],
            ${qArr}::smallint[],
            ${tstzArr}::timestamptz[]
          ) AS t(ts, id, val, q, tstz)
        `;
      } else if (table === 'records') {
        query = this.sql`
          INSERT INTO ${this.sql(table)} (ts, id, val, q, tstz)
          SELECT * FROM unnest(
            ${tsArr}::bigint[],
            ${idArr}::int[],
            ${valArr}::real[],
            ${qArr}::smallint[],
            ${tstzArr}::timestamptz[]
          ) AS t(ts, id, val, q, tstz)
        `;
      } else {
        throw new Error(`Unsupported table: ${table}`);
      }

      const result = await query;

      return {
        rows: result,
        rowCount: result.count ?? result.length,
        command: 'INSERT'
      };
    };

    try {
      return await execute();
    } catch (err) {
      if (!this._isFailoverError(err)) throw err;

      this.logger.log('Failover during bulk insert. Reconnecting...');

      await this._reconnect();

      // Создаём новый запрос на новом соединении
      return await execute();
    }
  },
  async end() {
    if (this.sql) {
      await this.sql.end();
      this.sql = null;
      this.pool = null;
    }
  }
};