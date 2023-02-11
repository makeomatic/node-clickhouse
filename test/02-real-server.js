const assert = require('assert');
const { once } = require('events');
const ClickHouse = require('../src/clickhouse');

describe('real server', () => {
  const host = process.env.CLICKHOUSE_HOST || '127.0.0.1';
  const port = process.env.CLICKHOUSE_PORT || 8123;
  const user = process.env.CLICKHOUSE_USER || 'new_user';
  const password = process.env.CLIKHOUSE_PASSWORD || 'new_password';

  const clickhouseOptions = {
    host,
    port,
    user,
    password,
  };

  let dbCreated = false;

  it('pings', (done) => {
    const ch = new ClickHouse({ host, port });
    ch.ping((err, ok) => {
      assert.ifError(err);
      assert.equal(ok, 'Ok.\n', "ping response should be 'Ok.\\n'");
      done();
    });
  });

  it('pinging using promise interface', () => {
    const ch = new ClickHouse({ host, port });
    return ch.pinging();
  });

  it('pinging using promise interface with bad connection option', async () => {
    assert.throws(() => new ClickHouse());
  });

  it('pings with options as host', (done) => {
    const ch = new ClickHouse(host);
    ch.ping((err, ok) => {
      assert.ifError(err);
      assert.equal(ok, 'Ok.\n', "ping response should be 'Ok.\\n'");
      done();
    });
  });

  it('returns error', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
    const stream = ch.query('ABCDEFGHIJKLMN', { syncParser: true });
    await once(stream, 'error');
  });

  it('authorises by credentials', () => {
    const ch = new ClickHouse(clickhouseOptions);
    return ch.querying('SELECT 1');
  });

  it('selects from system columns', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    ch.query('SELECT * FROM system.columns', done);
  });

  it('selects from system columns no more than 10 rows throws exception', async () => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { max_rows_to_read: 10 } });
    await assert.rejects(ch.querying('SELECT * FROM system.columns'));
  });

  it('creates a database', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    ch.query('CREATE DATABASE node_clickhouse_test', (err) => {
      if (err) {
        done(err);
        return;
      }
      dbCreated = true;
      done();
    });
  });

  it('creates a table', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    ch.query('CREATE TABLE node_clickhouse_test.t (a UInt8) ENGINE = Memory', done);
  });

  it('drops a table', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: 'node_clickhouse_test' } });
    ch.query('DROP TABLE t', done);
  });

  it('creates a table', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: 'node_clickhouse_test' } });
    ch.query('CREATE TABLE t (a UInt8) ENGINE = Memory', done);
  });

  it('inserts some data', async () => {
    const ch = new ClickHouse(clickhouseOptions);
    await ch.querying('INSERT INTO t VALUES (1),(2),(3)', { queryOptions: { database: 'node_clickhouse_test' } });
    await new Promise((resolve) => setTimeout(resolve, 500));
  });

  it('gets back data', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    const rows = [];
    const stream = ch.query('select a FROM t', { queryOptions: { database: 'node_clickhouse_test' } });

    stream.on('data', (row) => {
      rows.push(row);
    });

    stream.on('end', () => {
      assert(rows.length === 3);
      done();
    });
  });

  after((done) => {
    if (!dbCreated) {
      done();
      return;
    }

    const ch = new ClickHouse(clickhouseOptions);
    ch.query('DROP DATABASE node_clickhouse_test', done);
  });
});
