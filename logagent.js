/**
 * logagent.js
 *
 * Точка входа при запуске дочернего процесса для работы с логами
 * Входной параметр - конфигурация как строка JSON: {
 *   dbPath: <полный путь к БД, включая имя файла>
 *   logfile: <полный путь к файлу лога процесса>
 *   loglevel: <уровень логирования>
 * }
 */

// const util = require('util');
const path = require('path');

const logger = require('./logger');
const utils = require('./lib/utils');
const logagent_main = require('./lib/logagentindex');

try {
  let opt = utils.getOptions(process.argv[2], 'logagent');

  const logfile = opt.logfile || path.join(__dirname, 'ih_postgresql_logagent.log');
  let loglevel = !opt.loglevel || isNaN(opt.loglevel) ? 0 : Number(opt.loglevel);

  logger.start(logfile, loglevel);
  // console.log('Start logagent postgresql. Options: ' + JSON.stringify(opt));
  logger.log('Start logagent postgresql. Options: ' + JSON.stringify(opt));

  if (opt.bxpwd) {
    opt.password = utils.decryptBx(opt.bxpwd);
  }

  delete opt.logfile;
  delete opt.loglevel;

  utils.sendProcessInfo();
  setInterval(utils.sendProcessInfo, 10000);

  sendSettingsRequest();
  setInterval(sendSettingsRequest, 10800000); // 3 часа = 10800 сек

  logagent_main(process, opt, logger);
} catch (e) {
  process.send({ error: utils.getShortErrStr(e) });
  setTimeout(() => {
    process.exit(99);
  }, 500);
}

function sendSettingsRequest() {
  if (process.connected) process.send({ id: 'settings', type: 'settings' });
}
