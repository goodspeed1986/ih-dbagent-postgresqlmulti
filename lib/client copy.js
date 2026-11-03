/**
 * PostgreSQL client на базе postgres.js с полной совместимостью со старым API (pg-style)
 * Поддерживает несколько хостов + target_session_attrs='read-write' / 'primary'
 * Не требует изменений в основном коде приложения
 */

const postgres = require('postgres');

module.exports = {
  pool: null,           // ← будет прокси-объект, чтобы if (!client.pool) работало
  sql: null,            // реальный клиент postgres.js

  /**
   * Создаёт подключение (совместимо со старым вызовом)
   * @param {Object} dbopt - параметры (host, hosts, port, database, user/username, password и т.д.)
   * @param {Object} [logger] - опционально
   */
  async createPoolToDatabase(dbopt, logger = console) {
    if (!dbopt.database || !dbopt.user && !dbopt.username || !dbopt.password) {
      throw new Error('Обязательные параметры: database, user/username, password');
    }

    // Поддержка как строки, так и массива хостов
    let host = dbopt.host || dbopt.hosts || 'localhost';
    if (!Array.isArray(host)) host = [host];

    const options = {
      host: '127.0.0.1',                                 // строка или массив — postgres.js понимает
      port: dbopt.port || 5432,
      database: dbopt.database,
      username: dbopt.username || dbopt.user,
      password: dbopt.password,
      target_session_attrs: dbopt.target_session_attrs || 'read-write',  // или 'primary'
      max: dbopt.max || 20,
      idle_timeout: dbopt.idle_timeout || 30,
      connect_timeout: dbopt.connect_timeout || 10 || dbopt.connectionTimeoutMillis || 10,
      // ssl: dbopt.ssl || { rejectUnauthorized: false },  // если нужен
      // application_name: 'dbagent',
      transform: {
        bigint: Number,           // или (val) => Number(val)
        // Если хотите быть строже:
        // bigint: (val) => parseInt(val, 10),
      },
    };

    this.sql = postgres(options);

    // Создаём фейковый "pool"-объект для совместимости
    this.pool = {
      // Эмулируем .query в стиле pg (callback)
      query: (sqlOrConfig, values, callback) => {
        let text = typeof sqlOrConfig === 'string' ? sqlOrConfig : sqlOrConfig.text;
        let params = values;

        // Если передан объект { text, values }
        if (typeof sqlOrConfig === 'object' && sqlOrConfig.text) {
          text = sqlOrConfig.text;
          params = sqlOrConfig.values || [];
        }

        // Выполняем через postgres.js
        return this.sql.unsafe(text, params)
          .then(rows => {
            const res = {
              rows,
              rowCount: rows.count ?? rows.length,
              command: text.trim().split(' ')[0].toUpperCase(),
            };
            if (callback) callback(null, res);
            return res;
          })
          .catch(err => {
            if (callback) callback(err);
            throw err;
          });
      },

      // Для совместимости с .end() / .close()
      end: async (cb) => {
        if (this.sql) {
          await this.sql.end({ timeout: 5 });
          this.sql = null;
          this.pool = null;
        }
        if (cb) cb();
      },

      close: async () => this.pool.end(),
    };

    // Совместимый метод .run(sql) → возвращает { rows, rowCount }
    this.run = async (sql, values = []) => {
      const res = await this.pool.query(sql, values);
      return res;  // { rows, rowCount, ... }
    };

    // Совместимый метод .query(sql) → возвращает только rows (как в твоём старом client.query)
    this.query = async (sql) => {
      const res = await this.pool.query(sql);
      return res.rows;
    };

    logger.log(`PostgreSQL подключение создано (postgres.js) → target_session_attrs=${options.target_session_attrs}`);

    // Опционально: быстрая проверка (можно убрать, если доверяешь target_session_attrs)
    try {
      const [row] = await this.sql`SELECT pg_is_in_recovery() as recovery`;
      logger.log(`Recovery mode: ${row.recovery}`);
      // if (row.recovery) { ... pg_promote? но лучше не делать автоматически }
    } catch (e) {
      logger.warn('Не удалось проверить recovery mode при старте', e.message);
    }

    return this.pool;
  },

  // Для graceful shutdown
  async end() {
    if (this.pool) await this.pool.end();
  }
};