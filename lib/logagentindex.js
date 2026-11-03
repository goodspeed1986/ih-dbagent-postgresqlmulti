const util = require('util');

const client = require('./client');
const utils = require('./utils');

const tableNames = ['mainlog', 'pluginlog', 'devicelog', 'iseclog'];

module.exports = async function(channel, opt, logger) {
  const maxlogrecords = opt.maxlogrecords || 100000;
  let compressedTablesSet;
  try {
    if (!opt.database) throw { message: 'Missing database name for logs!' };

    await client.createPoolToDatabase(opt, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    for (const name of tableNames) {
      await client.query(getCreateTableStr(name), name);

      await client.query(`SELECT create_hypertable('${name}','ts', 
        chunk_time_interval => 86400000, 
        if_not_exists => TRUE);`);

      await client.query('CREATE INDEX IF NOT EXISTS ' + name + '_tsid ON ' + name + ' (tsid);');
    }

    compressedTablesSet = await utils.getCompressedTables(client, logger);
    logger.log('compressedTablesSet: '+util.inspect(compressedTablesSet), 2);

    sendDBSize(); // Отправить статистику первый раз
    setInterval(async () => sendDBSize(), 300000); // 300 сек = 5 мин

    // Проверить таблицу mainlog на дублирование записей
    checkTsidUnique('mainlog');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, query, payload);
      if (type == 'read') return read(id, query);
      if (type == 'run') return run(id, query);
      if (type == 'settings') return del(payload);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.end();
    });
  } catch (err) {
    processExit(1, err);
  }

  async function showGroups(name) {
    const result = await client.query(getGroupQuery(name));
    logger.log(name + ' group: ' + util.inspect(result), 1);
  }

 
  async function checkTsidUnique(tableName) {
    try {
      const sql = 'SELECT Count (tsid) count, tsid from ' + tableName + ' group by tsid having Count (tsid)>1';
      const result = await client.query(sql);

      if (result.length > 0) {
        logger.log('checkTsidUnique ' + tableName + ' result length = ' + result.length + ': ' + util.inspect(result));
        /*
        for (let rec of result) {
          const selSql = `SELECT ctid, tsid, ts from ${tableName} WHERE tsid = '${rec.tsid}'`;
          logger.log(selSql, 2);
          const resx = await client.query(selSql);
          logger.log('tsid = ' + rec.tsid + ' result=' + util.inspect(resx), 2);
          if (!resx || !resx.length) continue;

          let idx = 1;
          for (let recx of resx) {
            let newTsid = String(recx.ts) + '_' + String(idx).padStart(5, '0');
            idx += 1;
            const upSql = `UPDATE ${tableName} SET tsid='${newTsid}' WHERE ctid = '${recx.ctid}'`;
            await client.query(upSql);
            logger.log('Update ' + rec.tsid + 'to ' + newTsid, 2);
          }
        }
        */
      } else {
        logger.log('checkTsidUnique OK', 1);
      }
    } catch (err) {
      logger.log('ERROR: checkTsidUnique ' + util.inspect(err));
    }
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Objects} queryObj - {table}
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, queryObj, payload) {
    const table = queryObj && queryObj.table ? queryObj.table : 'mainlog';
    const columns = getColumns(table);
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const values1 = values.map(i => `(${i})`).join(', ');
    const sql = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ' + values1;
    logger.log('Write sql: ' + sql, 2);
    try {
      const changes = await client.query(sql);
      logger.log('Write query id=' + id + ', changes=' + changes, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function read(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in read query: ' + util.inspect(queryObj) };
      logger.log('Read: ' + sql, 2);
      const result = await client.query(sql);
      logger.log(' Result length = ' + result.length, 2);

      send({ id, query: queryObj, payload: result });
    } catch (err) {
      sendError(id, err);
    }
  }

  async function run(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in run query: ' + util.inspect(queryObj) };

      const changes = await client.query(sql);
      logger.log(`${sql}  Row(s) affected: ${changes}`, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  // {devicelog:[{level, days},..], pluginlog:{level,days}}
  async function del(payload) {
    if (!payload || !payload.rp || typeof payload.rp != 'object') return;
    for (const name of Object.keys(payload.rp)) {
      // Если есть такая таблица - обработать
      if (tableNames.includes(name)) {
        const arr = payload.rp[name];
        for (const item of arr) {
          await deleteRecordsByLevel(name, item.days, item.level);
        }
      }
    }
  }

  async function deleteRecordsByLevel(tableName, archDay, level) {
    if (compressedTablesSet.has(tableName)) return;
    if (!archDay || archDay<=0) archDay = 1;
    const archDepth = archDay * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE level = ${level} AND ts<${delTime}`;

    try {
      const changes = await client.query(sql);
      logger.log(`${tableName}  Level=${level} Archday=${archDay}  Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  async function deleteRecordsMax(tableName) {
    if (compressedTablesSet.has(tableName)) return;
    // Оставляем только данные за 1 день
    const archDepth = 1 * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE ts<${delTime}`;
    const mes = 'Number of records exceeded ' + maxlogrecords + '!! All except the last day data was deleted!!';

    try {
      const changes = await client.query(sql);
      logger.log(`${tableName}  ${mes} Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  function send(message) {
    channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.end();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }

  async function sendDBSize() {
    if (!process.connected) return;

    let data = {
      size: 0
    };
    const needDelete = [];
    for (const name of tableNames) {
      const sqlQuery = `SELECT hypertable_size('${name}') ;`;
      const dbSizeArr = await client.query(sqlQuery);
      let fileSize = dbSizeArr[0].hypertable_size / 1024 / 1024;
      data.size += fileSize;
      logger.log('Table ' + name + ' size' + fileSize, 1);
      // const result = await client.query('SELECT Count (*) count From ' + name);
      // const count = result ? result[0].count : 0;
      const count = await getTableRecordsCount(name);
      if (count > 1000) {
        await showGroups(name);
      }
      data[name] = count;
      if (maxlogrecords > 0 && count > maxlogrecords && name != 'mainlog') needDelete.push(name);
    }
    data.size = parseInt(data.size * 100, 10) / 100;
    // Отправить фактическое состояние
    if (process.connected) process.send({ type: 'procinfo', data });

    if (!needDelete.length) return;

    for (const name of needDelete) {
      await deleteRecordsMax(name);
    }
  }

  async function getTableRecordsCount(name) {
    const result = await client.query('SELECT Count (*) count From ' + name);
    return result ? result[0].count : 0;
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'devicelog':
      result = 'did TEXT,prop TEXT,val TEXT,txt TEXT, ts bigint NOT NULL,tsid TEXT,cmd TEXT,sender TEXT';
      break;
    case 'pluginlog':
      result = 'unit TEXT, txt TEXT,level INTEGER, ts bigint NOT NULL, tsid TEXT, sender TEXT';
      break;

    case 'iseclog':
      result =
        'type TEXT, msg TEXT, subjid TEXT, subjname TEXT, result TEXT, changed TEXT, ip TEXT, class TEXT, app TEXT, version TEXT, objid TEXT, objname TEXT, level INTEGER, ts bigint NOT NULL, tsid TEXT,sender TEXT';
      break;

    default:
      result = 'tags TEXT, did TEXT, location TEXT, txt TEXT, level INTEGER, ts bigint NOT NULL,tsid TEXT,sender TEXT';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'devicelog':
      return ['did', 'prop', 'val', 'txt', 'ts', 'tsid', 'cmd', 'sender'];

    case 'pluginlog':
      return ['unit', 'txt', 'level', 'ts', 'tsid', 'sender'];

    case 'iseclog':
      return [
        'type',
        'msg',
        'subjid',
        'subjname',
        'objid',
        'objname',
        'result',
        'changed',
        'ip',
        'app',
        'class',
        'version',
        'level',
        'ts',
        'tsid',
        'sender'
      ];

    default:
      return ['tags', 'did', 'location', 'txt', 'level', 'ts', 'tsid', 'sender'];
  }
}

function getGroupQuery(tableName) {
  switch (tableName) {
    case 'devicelog':
      return 'SELECT did, Count (tsid) count from devicelog group by did';

    case 'pluginlog':
      return 'SELECT unit, Count (tsid) count from pluginlog group by unit';

    default:
      return 'SELECT level, Count (tsid) count from ' + tableName + ' group by level';
  }
}
/*
function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss / 1024);
  const memheap = Math.floor(mu.heapTotal / 1024);
  const memhuse = Math.floor(mu.heapUsed / 1024);
  if (process.connected) process.send({ type: 'procinfo', data: { state: 1, memrss, memheap, memhuse } });
}

function sendSettingsRequest() {
  if (process.connected) process.send({ id: 'settings', type: 'settings' });
}
*/
