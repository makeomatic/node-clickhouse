/**
 * @constructor
 * @extends {Error}
 * @param {string} message
 */
class ClickhouseError extends Error {
  constructor(message) {
    super(message);
    Error.captureStackTrace(this, ClickhouseError);

    for (const line of message.split(/,\s+(?=e\.)/gm).values()) {
      const f = line.trim().split(/\n/gm).join('');
      let m = f.match(/^(?:Error: )?Code: (\d+)$/);
      if (m) {
        /**
         * @type {number}
         */
        this.code = parseInt(m[1], 10);
      }

      m = f.match(/^e\.displayText\(\) = ([A-Za-z0-9:]+:) ([^]+)/m);
      // console.info(f);
      if (m) {
        // e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
        // e.displayText() = DB::Exception: Syntax error: failed at position 10 ('ABCDEFGHIJKLMN') (line 2, col 3): ABCDEFGHIJKLMN
        [, this.scope, this.message] = m;
        m = this.message.match(/Syntax error: (?:failed at position (\d+)(?:\s*(?:\([^)]*\)\s*)?\(line\s*(\d+),\s+col\s*(\d+)\))?)/);

        if (m) {
          // console.log('!!! syntax error: pos %s line %s col %s', m[1], m[2], m[3]);

          /**
           * @type {number}
           */
          this.lineno = parseInt(m[2] || 1, 10);

          /**
           * @type {number}
           */
          this.colno = parseInt(m[3] || m[1], 10);
        }
        return;
      }

      m = f.match(/^e\.what\(\) = (.*)/);
      if (m) {
        /**
         * @type {string}
         */
        [, this.type] = m;
        return;
      }
    }
  }
}

/**
 * Parses clickhouse error
 * @param {string} e
 * @returns ClickhouseError
 */
function parseError(e) {
  return new ClickhouseError(e.toString('utf8'));
}

module.exports = parseError;
