import test from 'node:test';import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';import {createHash} from 'node:crypto';
import {mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {readSqlInboxArrivalsSince} from '../read-sql.mjs';import {convertSqlCursor} from '../receive.mjs';
test('explicit historical reader preserves original identity, pages without skipping and retains recipient boundaries',()=>{
 const root=mkdtempSync(join(tmpdir(),'koios-historical-source-')),dbPath=join(root,'original.sqlite');
 try{const db=new DatabaseSync(dbPath);db.exec('CREATE TABLE activitypub_verified_arrivals(id INTEGER PRIMARY KEY,activity_json TEXT,observed_at REAL,recipient TEXT)');const put=db.prepare('INSERT INTO activitypub_verified_arrivals VALUES(?,?,?,?)');for(let i=1;i<=257;i++)put.run(i,JSON.stringify({id:'https://source.example/'+i,type:'Create'}),i+0.25,i%2?'koios-news':'other-enterprise');db.close();
 const original=readFileSync(dbPath),expectedSha256=createHash('sha256').update(original).digest('hex');
 const first=readSqlInboxArrivalsSince({dbPath,lastId:0,expectedSha256});assert.equal(first.rows.length,256);assert.equal(first.lastId,256);assert.equal(first.rows[0].recipient,'koios-news');assert.equal(first.rows[1].recipient,'other-enterprise');assert.equal(first.rows[0].observedAt,1.25);
 const next=readSqlInboxArrivalsSince({dbPath,lastId:first.lastId,expectedSha256});assert.equal(next.rows.length,1);assert.equal(next.lastId,257);assert.deepEqual(readFileSync(dbPath),original);
 assert.throws(()=>readSqlInboxArrivalsSince({dbPath,lastId:0,expectedSha256:'0'.repeat(64)}),/identity/);writeFileSync(dbPath+'-wal','unreceived');assert.throws(()=>readSqlInboxArrivalsSince({dbPath,lastId:0,expectedSha256}),/transaction custody/);assert.deepEqual(readFileSync(dbPath),original);
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('receipt-time heuristics cannot advance a historical cursor into native custody',()=>{assert.throws(()=>convertSqlCursor(),/independently verified source-to-native/);});
