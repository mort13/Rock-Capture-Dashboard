const duckdb = require('duckdb');
const db = new duckdb.Database(':memory:');
const conn = db.connect();

conn.all("DESCRIBE SELECT * FROM read_parquet('Databank/Rock-Capture-Database/scans.parquet') LIMIT 0", (err, rows) => {
  if (err) { console.error('scans error:', err); } else { console.log('=== scans ==='); rows.forEach(r => console.log(r.column_name, r.column_type)); }
  conn.all("DESCRIBE SELECT * FROM read_parquet('Databank/Rock-Capture-Database/compositions.parquet') LIMIT 0", (err2, rows2) => {
    if (err2) { console.error('compositions error:', err2); } else { console.log('\n=== compositions ==='); rows2.forEach(r => console.log(r.column_name, r.column_type)); }
  });
});
