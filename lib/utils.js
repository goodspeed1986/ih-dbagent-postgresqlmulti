/**
 *  utils.js
 */

const util = require('util');
const fs = require('fs');

function getOptions(str, node) {
  if (!str) throw { message: 'No options in command line!' };
  let jsondata = str;
  if (str.endsWith('config.json')) {
    // Передали путь
    if (!fs.existsSync(str)) throw { message: 'File not found ' + str };
    jsondata = fs.readFileSync(str, 'utf8');
  }
  const res = JSON.parse(jsondata);
  return res && res[node] ? res[node] : {};
}

function decryptBx(str) {
  return str ? Buffer.from(str, 'base64').toString() : '';
}

/**
 * Формировать массив для записи в БД
 *
 * @param {Array of Objects} - data - [{ts:1423476765, dn:DD1, prop:'value', val:123},..]
 * @param {Array of Strings}  columns - массив имен столбцов ['ts','id','val']
 * @param {timestamp} tdate - опционально - дата, за которую надо брать данные
 *
 * Возвращает массив массивов в порядке массива столбцов: [[1423476765, 2, 123],[..]]
 **/
function formValues(data, columns, tdate) {
  let result = [];
  let arr;
  let clmn;

  for (let i = 0; i < data.length; i++) {
    // if (!data[i].ts || (tdate && !hut.isTheSameDate(new Date(data[i].ts), tdate))) continue;
    arr = [];
    for (let j = 0; j < columns.length; j++) {
      clmn = columns[j];
      // проверяет есть ли такое свойство из колонок в запросе на запись
      if (clmn == '"end"') {
        data[i][clmn] = data[i].end;
      }

      if (data[i][clmn] == null || typeof data[i][clmn] === 'undefined') {
        // data[i][clmn] = null;
        arr.push('NULL');
      } else if (typeof data[i][clmn] === 'number') {
        arr.push(data[i][clmn]);
      } else if (clmn == 'tstz') {
        arr.push(data[i][clmn]);
      } else {
        arr.push("'" + data[i][clmn] + "'");
      }
    }
    result.push(arr);
  }
  return result;
}

/**
 * Формировать данные, считанные из БД, для отдачи на график
 *
 * @param {Array of Objects} records - [{dn,prop,ts,val},...]
 * @param {Array} idarr [5,6,7]
 * @return {Array of Arrays} : [[1608574986578, null,42],[1608574986580, 24,null],...]
 */
function recordsForTrend(records, idarr) {
  if (!idarr || !idarr.length || !records || !records.length) return [];

  // const dArr = dnarr.map(item => item.split('.')[0]);
  const dArr = idarr;
  const rarr = [];
  const len = dArr.length;
  let last_ts;

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];

    if (!rec || !rec.id || !rec.ts) continue;

    // const dn_prop = rec.dn + '.' + rec.prop;
    const dn_indx = dArr.findIndex(el => el == rec.id);
    // const dn_indx = dArr.findIndex(el => el == rec.dn);
    if (dn_indx < 0) continue;

    let data;
    const ts = rec.ts;
    // multiple series data combine
    if (ts != last_ts) {
      data = new Array(len + 1).fill(null);
      data[0] = ts;
      last_ts = ts;
    } else {
      data = rarr.pop();
    }
    // data[1 + dn_indx] = Number(rec.val);
    data[1 + dn_indx] = rec.val;
    rarr.push(data);
  }
  return rarr;
}
function recordsFor(records, queryObj) {
  if (!records || !records.length) return [];
  let arr;
  let idarr;
  let idarrObj = {};
  arr = queryObj.dn_prop.split(',');
  idarr = queryObj.ids.split(',');
  idarr.forEach((item, i) => {
    idarrObj[item] = arr[i];
  });
  return records.map(item => {
    let splitted = idarrObj[item.id].split('.');
    return { ts: item.ts, dn: splitted[0], prop: splitted[1], val: item.val };
  });
}
function getQueryStrId(query, idarr) {
  // Время и значения должны быть в одинарных скобках, имена полей в двойных!!
  const from = query.start;
  const to = query.end;
  let table = query.table || 'records';
  let fields = '*';
  let nnfield = 'val';
  if (query.catable && query.cafield) {
    table = query.catable;
    fields = 'id, ts, '+query.cafield+' as val';
    nnfield = query.cafield;
  }
  let res = `select ${fields} from ${table} ${formWhereQueryId(idarr, from, to, 'ts', 'ts', query.notnull, nnfield)} order by ts`;
  return res;
}

function getQueryStrDn(query, dnarr) {
  // Время и значения должны быть в одинарных скобках, имена полей в двойных!!
  const from = query.start;
  const to = query.end;
  let res = `select * from timeline ${formWhereQueryDn(dnarr, from, to, '"end"', 'start')} order by start`;
  return res;
}

function formWhereQueryDn(dnarr, from, to, ts_start_name = 'ts', ts_end_name = 'ts', notnull) {
  let query = '';
  let first = true;

  if (dnarr && dnarr.length > 0) {
    if (dnarr.length == 1) {
      query += dnAndProp(dnarr[0]);
      first = false;
    } else {
      query += ' ( ';
      for (let i = 0; i < dnarr.length; i++) {
        if (dnarr[i]) {
          query += isFirst(' OR ') + ' (' + dnAndProp(dnarr[i]) + ')';
        }
      }
      query += ' ) ';
    }
  }

  if (from) {
    query += isFirst(' AND ') + ' ' + ts_start_name + ' >= ' + from;
  }

  if (to) {
    query += isFirst(' AND ') + ' ' + ts_end_name + ' <= ' + to;
  }

  if (notnull) {
    query += isFirst(' AND ') + ' val is NOT NULL';
  }

  return query ? ' WHERE ' + query : '';

  function isFirst(op) {
    return first ? ((first = false), '') : op;
  }

  function dnAndProp(dn_prop) {
    if (dn_prop.indexOf('.') > 0) {
      const splited = dn_prop.split('.');
      return " dn = '" + splited[0] + "' AND prop = '" + splited[1] + "' ";
    }
    // Иначе это просто dn
    return " dn = '" + dn_prop + "'";
  }
}

function formWhereQueryId(idarr, from, to, ts_start_name = 'ts', ts_end_name = 'ts', notnull, nnfield = 'val') {
  let query = '';
  let first = true;

  if (idarr && idarr.length > 0) {
    if (idarr.length == 1) {
      query += idQuery(idarr[0]);
      first = false;
    } else {
      query += ' ( ';
      for (let i = 0; i < idarr.length; i++) {
        if (idarr[i]) {
          query += isFirst(' OR ') + ' (' + idQuery(idarr[i]) + ')';
        }
      }
      query += ' ) ';
    }
  }

  if (from) {
    query += isFirst(' AND ') + ' ' + ts_start_name + ' >= ' + from;
  }

  if (to) {
    query += isFirst(' AND ') + ' ' + ts_end_name + ' <= ' + to;
  }

  if (notnull) {
    // query += isFirst(' AND ') + ' val is NOT NULL';
    query += isFirst(' AND ') + nnfield+' is NOT NULL';
  }

  return query ? ' WHERE ' + query : '';

  function isFirst(op) {
    return first ? ((first = false), '') : op;
  }

  function idQuery(id) {
    // Иначе это просто id
    return ' id = ' + id + ' ';
  }
}

function getShortErrStr(e) {
  if (typeof e == 'object') return e.message ? getErrTail(e.message) : JSON.stringify(e);
  if (typeof e == 'string') return e.indexOf('\n') ? e.split('\n').shift() : e;
  return String(e);

  function getErrTail(str) {
    let idx = str.lastIndexOf('error:');
    return idx > 0 ? str.substr(idx + 6) : str;
  }
}

function getDateStr() {
  const dt = new Date();
  return (
    pad(dt.getDate()) +
    '.' +
    pad(dt.getMonth() + 1) +
    ' ' +
    pad(dt.getHours()) +
    ':' +
    pad(dt.getMinutes()) +
    ':' +
    pad(dt.getSeconds()) +
    '.' +
    pad(dt.getMilliseconds(), 3)
  );
}

function pad(str, len = 2) {
  return String(str).padStart(len, '0');
}

/*
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
*/

function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss / 1024);
  const memheap = Math.floor(mu.heapTotal / 1024);
  const memhuse = Math.floor(mu.heapUsed / 1024);
  if (process.connected) process.send({ type: 'procinfo', data: { state: 1, memrss, memheap, memhuse } });
}

 /*
  SELECT 
   hypertable_schema AS schema_name,
   hypertable_name AS table_name,
   config AS policy_config  -- JSONB с деталями политики, включая интервал сжатия
  FROM timescaledb_information.jobs 
  WHERE job_type = 'policy_compression';
  */
async function getCompressedTables(client, logger) {
  const tablesSet = new Set();
  try {
    const sql = `SELECT 
    hypertable_schema AS schema_name,
    hypertable_name AS table_name,
    config AS policy_config 
    FROM timescaledb_information.jobs 
    WHERE proc_name = 'policy_compression'`;

    const result = await client.query(sql);
    logger.log('select policy_compression result: ' + util.inspect(result));
    if (result && result.length) {
      result.forEach(rec => {
        tablesSet.add(rec.table_name)
      })
    }
  } catch (err) {
    logger.log('ERROR: getCompressedTables ' + util.inspect(err));
  }
  return tablesSet;
}

module.exports = {
  getOptions,
  decryptBx,
  formValues,
  recordsForTrend,
  recordsFor,
  getQueryStrId,
  getQueryStrDn,
  formWhereQueryId,
  formWhereQueryDn,
  getShortErrStr,
  getDateStr,
  sendProcessInfo,
  getCompressedTables
};
