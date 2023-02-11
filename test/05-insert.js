const assert = require('assert');
const fs = require('fs');
const crypto = require('crypto');
const ClickHouse = require('../src/clickhouse');

const { encodeRow } = require('../src/process-db-value');

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function generateData(format, fileName, cb) {
  const rs = fs.createWriteStream(fileName);
  for (let i = 0; i < 10; i += 1) {
    rs.write(
      encodeRow([
        Math.ceil(Math.random() * 1000),
        Math.random() * 1000,
        crypto.randomBytes(20).toString('hex'),
        randomDate(new Date(2012, 0, 1), new Date()),
      ], format)
    );
  }

  rs.end(cb);
}

const testDate = new Date();
const testDateISO = testDate.toISOString().replace(/\..*/, '').replace('T', ' ');

describe('insert data', function testSuite() {
  this.timeout(5000);
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
  const dbName = 'node_clickhouse_test_insert';

  before(async () => {
    const ch = new ClickHouse(clickhouseOptions);
    const okFn = async () => {};
    await ch.querying(`DROP DATABASE ${dbName}`).then(okFn, okFn);
    await ch.querying(`CREATE DATABASE ${dbName}`);
    dbCreated = true;
  });

  it('creates a table', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });
    ch.query('CREATE TABLE t (a UInt8) ENGINE = Memory', done);
  });

  it('inserts some prepared data using stream', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    const stream = ch.query('INSERT INTO t', { queryOptions: { database: dbName } }, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t', { syncParser: true, queryOptions: { database: dbName } }, (_, result) => {
        assert.equal(result.data[0][0], 8);
        assert.equal(result.data[1][0], 73);
        assert.equal(result.data[2][0], 42);

        done();
      });
    });

    stream.write('8');
    stream.write('73');
    stream.write(Buffer.from('42'));
    stream.end();
  });

  it('inserts some data', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    ch.query('INSERT INTO t VALUES (1),(2),(3)', { queryOptions: { database: dbName } }, (err) => {
      assert(!err, err);

      done();
    });
  });

  it('inserts some data using promise', () => {
    const ch = new ClickHouse(clickhouseOptions);
    return ch.querying('INSERT INTO t VALUES (1),(2),(3)', { queryOptions: { database: dbName } });
  });

  it('creates a table 2', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });
    ch.query('CREATE TABLE t2 (a UInt8, b Float32, x Nullable(String), z DateTime) ENGINE = Memory', (err) => {
      assert(!err);

      done();
    });
  });

  it('inserts data from array using stream', (done) => {
    const ch = new ClickHouse(clickhouseOptions);

    const stream = ch.query('INSERT INTO t2', { queryOptions: { database: dbName } }, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t2', { syncParser: true, queryOptions: { database: dbName } }, (_, result) => {
        assert.equal(result.data[0][0], 1);
        assert.equal(result.data[0][1], 2.22);
        assert.equal(result.data[0][2], null);
        assert.equal(result.data[0][3], testDateISO);

        assert.equal(result.data[1][0], 20);
        assert.equal(result.data[1][1], 1.11);
        assert.equal(result.data[1][2], 'wrqwefqwef');
        assert.equal(result.data[1][3], '2017-07-07 12:12:12');

        done();
      });
    });

    stream.write([1, 2.22, null, testDate]);
    stream.write('20\t1.11\twrqwefqwef\t2017-07-07 12:12:12');

    // stream.write ([0, Infinity, null, new Date ()]);
    // stream.write ([23, NaN, "yyy", new Date ()]);

    stream.end();
  });

  it('creates a table 3', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });
    ch.query('CREATE TABLE t3 (a UInt8, b Float32, x Nullable(String), z DateTime) ENGINE = Memory', (err) => {
      assert(!err);

      done();
    });
  });

  it('inserts data from array of objects using stream', (done) => {
    const ch = new ClickHouse(clickhouseOptions);

    const stream = ch.query('INSERT INTO t3', { format: 'JSONEachRow', queryOptions: { database: dbName } }, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t3', { syncParser: true, queryOptions: { database: dbName } }, (_, result) => {
        assert.equal(result.data[0][0], 1);
        assert.equal(result.data[0][1], 2.22);
        assert.equal(result.data[0][2], null);
        assert.equal(result.data[0][3], testDateISO);

        assert.equal(result.data[1][0], 20);
        assert.equal(result.data[1][1], 1.11);
        assert.equal(result.data[1][2], 'wrqwefqwef');
        assert.equal(result.data[1][3], '2017-07-07 12:12:12');

        assert.equal(result.data.length, 2);

        done();
      });
    });

    stream.write({
      a: 1, b: 2.22, x: null, z: testDate,
    });
    stream.write({
      a: 20, b: 1.11, x: 'wrqwefqwef', z: '2017-07-07 12:12:12',
    });

    // stream.write ([0, Infinity, null, new Date ()]);
    // stream.write ([23, NaN, "yyy", new Date ()]);

    stream.end();
  });

  it('inserts from select', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });

    ch.query('INSERT INTO t3 SELECT * FROM t2', {}, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t3', { syncParser: true, queryOptions: { database: dbName } }, (_, result) => {
        assert.equal(result.data[2][0], 1);
        assert.equal(result.data[2][1], 2.22);
        assert.equal(result.data[2][2], null);
        assert.equal(result.data[2][3], testDateISO);

        assert.equal(result.data[3][0], 20);
        assert.equal(result.data[3][1], 1.11);
        assert.equal(result.data[3][2], 'wrqwefqwef');
        assert.equal(result.data[3][3], '2017-07-07 12:12:12');

        assert.equal(result.data.length, 4);

        done();
      });
    });
  });

  it('piping data from csv file', (done) => {
    const ch = new ClickHouse(clickhouseOptions);

    const csvFileName = __filename.replace('.js', '.csv');

    function processFileStream(fileStream) {
      const stream = ch.query('INSERT INTO t3', { format: 'CSV', queryOptions: { database: dbName } }, (err) => {
        assert(!err, err);

        ch.query('SELECT * FROM t3', { syncParser: true, queryOptions: { database: dbName } }, (err2) => {
          assert(!err2, err2);

          fs.unlink(csvFileName, () => {
            done();
          });
        });
      });

      fileStream.pipe(stream);

      stream.on('error', done);
    }

    fs.stat(csvFileName, () => {
      return generateData('CSV', csvFileName, () => {
        processFileStream(fs.createReadStream(csvFileName));
      });
    });
  });

  it('piping data from tsv file', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    const tsvFileName = __filename.replace('.js', '.tsv');

    function processFileStream(fileStream) {
      const stream = ch.query('INSERT INTO t3', { format: 'TabSeparated', queryOptions: { database: dbName } }, (err) => {
        assert(!err, err);

        ch.query('SELECT * FROM t3', { syncParser: true, queryOptions: { database: dbName } }, (err2) => {
          assert(!err2, err2);

          fs.unlink(tsvFileName, () => {
            done();
          });
        });
      });

      fileStream.pipe(stream);

      stream.on('error', done);
    }

    fs.stat(tsvFileName, () => {
      return generateData('TSV', tsvFileName, () => {
        processFileStream(fs.createReadStream(tsvFileName));
      });
    });
  });

  it('creates a table 4', (done) => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });
    ch.query('CREATE TABLE t4 (arrayString Array(String), arrayInt Array(UInt32)) ENGINE = Memory', done);
  });

  it('inserts array with format JSON using stream', (done) => {
    const ch = new ClickHouse(clickhouseOptions);

    const stream = ch.query('INSERT INTO t4', { queryOptions: { database: dbName }, format: 'JSONEachRow' }, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t4', { syncParser: true, queryOptions: { database: dbName }, dataObjects: 'JSON' }, (_, result) => {
        assert.deepEqual(result.data[0].arrayString, ['first', 'second']);
        assert.deepEqual(result.data[0].arrayInt, [1, 0, 100]);

        done();
      });
    });

    stream.write({
      arrayString: ['first', 'second'],
      arrayInt: [1, 0, 100],
    });

    stream.end();
  });

  it('creates a table 5', () => {
    const ch = new ClickHouse({ ...clickhouseOptions, queryOptions: { database: dbName } });
    return ch.querying('CREATE TABLE t5 (a UInt8, b Float32, x Nullable(String), z DateTime) ENGINE = Memory');
  });

  it('inserts csv with FORMAT clause', (done) => {
    const ch = new ClickHouse(clickhouseOptions);
    const stream = ch.query('INSERT INTO t5 FORMAT CSV', { queryOptions: { database: dbName } }, (err) => {
      assert(!err, err);

      ch.query('SELECT * FROM t5', { syncParser: true, queryOptions: { database: dbName } }, (_, result) => {
        assert.equal(result.data[0][0], 0);
        assert.equal(result.data[0][1], 0);
        assert.equal(result.data[0][2], null);
        assert.equal(result.data[0][3], '1970-01-02 00:00:00');
        assert.equal(result.data[1][0], 1);
        assert.equal(result.data[1][1], 1.5);
        assert.equal(result.data[1][2], '1');
        assert.equal(result.data[1][3], '2050-01-01 00:00:00');

        done();
      });
    });
    stream.write('0,0,\\N,"1970-01-02 00:00:00"\n1,1.5,"1","2050-01-01 00:00:00"');
    stream.end();
  });

  it('select data with FORMAT clause', async () => {
    const ch = new ClickHouse(clickhouseOptions);
    const data = await ch.querying('SELECT * FROM t5 FORMAT Values', { queryOptions: { database: dbName } });
    assert.equal(data, '(0,0,NULL,\'1970-01-02 00:00:00\'),(1,1.5,\'1\',\'2050-01-01 00:00:00\')');
  });

  it('select data with GET method and FORMAT clause', async () => {
    const ch = new ClickHouse({ ...clickhouseOptions, useQueryString: true });
    const data = await ch.querying('SELECT * FROM t5 FORMAT Values', { queryOptions: { database: dbName } });
    assert.equal(data, '(0,0,NULL,\'1970-01-02 00:00:00\'),(1,1.5,\'1\',\'2050-01-01 00:00:00\')');
  });

  it('select data with GET method and format option', async () => {
    const ch = new ClickHouse({ ...clickhouseOptions, useQueryString: true });
    const data = await ch.querying('SELECT * FROM t5', { queryOptions: { database: dbName }, format: 'Values' });
    assert.equal(data, '(0,0,NULL,\'1970-01-02 00:00:00\'),(1,1.5,\'1\',\'2050-01-01 00:00:00\')');
  });

  after((done) => {
    if (!dbCreated) return done;

    const ch = new ClickHouse(clickhouseOptions);
    return ch.query(`DROP DATABASE ${dbName}`, done);
  });
});
