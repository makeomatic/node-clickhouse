const ClickHouse = require('../src/clickhouse');

const DB_NAME = 'node_clickhouse_test_compat';
const noop = () => {};

describe('api compatibility', () => {
  const host = process.env.CLICKHOUSE_HOST || '127.0.0.1';
  const port = process.env.CLICKHOUSE_PORT || 8123;
  const user = process.env.CLICKHOUSE_USER || 'new_user';
  const password = process.env.CLICKHOUSE_PASSWORD || 'new_password';

  const clickhouseOptions = {
    host,
    port,
    user,
    password,
  };

  const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: DB_NAME } });

  before(() => (
    new ClickHouse(clickhouseOptions)
      .querying(`CREATE DATABASE IF NOT EXISTS "${DB_NAME}"`)
  ));

  after(() => (
    ch.querying(`DROP DATABASE IF EXISTS "${DB_NAME}"`)
  ));

  describe('should emit "end" event', () => {
    before(() => {
      return ch.querying('CREATE TABLE x (date Date) Engine=Memory');
    });

    it('on insert done', (done) => {
      const chStream = ch.query('INSERT INTO x', { format: 'JSONEachRow' });
      chStream.on('data', noop);
      chStream.on('error', done);
      chStream.on('end', () => done());

      chStream.write({ date: '2000-01-01' });
      chStream.end();
    });

    describe('on SELECT done. With default format', () => {
      it('HTTP GET', (done) => {
        const stream = ch.query('SELECT * FROM system.numbers LIMIT 0', { readonly: true });
        stream.on('data', noop);
        stream.on('end', () => done());
        stream.end();
      });

      it('HTTP POST', (done) => {
        const stream = ch.query('SELECT * FROM system.numbers LIMIT 0', { readonly: false });
        stream.on('data', noop);
        stream.on('end', () => done());
        stream.end();
      });
    });
  });
});
