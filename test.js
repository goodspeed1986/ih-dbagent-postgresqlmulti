const {Client, Pool} = require('pg');
const util = require('util');
const client = new Client({
    user: 'ih_user',
    host: 'localhost',
    database: 'hist',
    password: 'ihpassword',
    port: 5432,
  })

  client.connect();
 /* const query = {
    text: `SELECT * FROM records  WHERE  dn = 'Dimm_001' AND  ts >= 1646110666231 AND  ts <= 1646144266231 ORDER BY ts;`,
    rowMode: 'array'
  }*/
  const query = {
    text: `SELECT table_name, pg_size_pretty( pg_relation_size(quote_ident(table_name)) )
    FROM information_schema.tables
    WHERE table_name = 'records'`,
    rowMode: 'array'
  }
client.query(query, (err, res) => {
    console.log(util.inspect(res.rows))
    client.end()
  })