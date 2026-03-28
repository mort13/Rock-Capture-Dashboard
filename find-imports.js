const fs = require('fs');
const files = [
  'node_modules/apache-arrow/Arrow.dom.mjs',
  'node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser.mjs',
];
const bare = new Set();
for (const f of files) {
  const src = fs.readFileSync(f, 'utf8');
  // match: from "specifier" or import "specifier"
  const re = /(?:from|import)\s+"([^"]+)"/g;
  let m;
  while ((m = re.exec(src)) !== null) {
    const s = m[1];
    if (!s.startsWith('.') && !s.startsWith('/')) bare.add(s);
  }
}
console.log([...bare].sort().join('\n'));
