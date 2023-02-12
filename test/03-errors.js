const assert = require('assert');
const { once } = require('events');
const ClickHouse = require('../src/clickhouse');

describe('error parsing', () => {
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

  it('will not throw on http error', async () => {
    const ch = new ClickHouse({ host, port: 59999, readonly: true });
    const stream = ch.query('ABCDEFGHIJKLMN', { syncParser: true });
    await once(stream, 'error');
  });

  it.skip('returns error for unknown sql', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
    const stream = ch.query('ABCDEFGHIJKLMN', { syncParser: true });
    const [err] = await once(stream, 'error');
    assert(err);
    assert(err.message.match(/Syntax error/));
    assert.equal(err.lineno, 1, 'line number should eq 1');
    assert.equal(err.colno, 1, 'col  number should eq 1');
  });

  it.skip('returns error with line/col for sql with garbage', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
    const stream = ch.query('CREATE\n\t\tABCDEFGHIJKLMN', { syncParser: true });
    const [err] = await once(stream, 'error');
    assert(err);
    assert(err.message.match(/Syntax error/));
    assert.equal(err.lineno, 2, 'line number should eq 2');
    assert.equal(err.colno, 3, 'col  number should eq 3');
  });

  it('returns error for empty sql', async () => {
    const ch = new ClickHouse({ ...clickhouseOptions, readonly: true });
    const stream = ch.query('-- nothing here', { syncParser: true });
    const [err] = await once(stream, 'error');
    assert(err);
    assert(err.message.match(/Empty query/) || err.message.match(/Syntax error/), err);
  });

  it('returns error for unknown table', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
    const stream = ch.query('SELECT * FROM xxx', { syncParser: true });
    const [err] = await once(stream, 'error');
    assert(err);
    assert.ifError(err.message.match(/Syntax error/));
  });
});
