// Explicit offline reading only. No runtime or audience clearance is produced.
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,openSync,readSync,closeSync,mkdtempSync,copyFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';import {join} from 'node:path';
function identity(file){
 const held=lstatSync(file);if(!held.isFile()||held.isSymbolicLink()||held.size>256*1024*1024)throw new Error('Historical inbox requires bounded regular source custody');
 const hash=createHash('sha256'),fd=openSync(file,'r'),buffer=Buffer.alloc(1024*1024);let total=0;
 try{for(;;){const size=readSync(fd,buffer,0,buffer.length,null);if(!size)break;total+=size;if(total>256*1024*1024)throw new Error('Historical inbox source grew beyond its bound');hash.update(buffer.subarray(0,size));}return hash.digest('hex');}finally{closeSync(fd);}
}
export function readSqlInboxArrivalsSince({dbPath,lastId,expectedSha256}){
 if(!Number.isSafeInteger(lastId)||lastId<0||!/^[a-f0-9]{64}$/u.test(expectedSha256??''))throw new Error('Historical inbox requires exact source identity and a valid cursor');
 if(!existsSync(dbPath))throw new Error('Historical inbox source is missing');
 for(const suffix of ['-wal','-journal'])if(existsSync(dbPath+suffix)){const held=lstatSync(dbPath+suffix);if(!held.isFile()||held.isSymbolicLink()||held.size!==0)throw new Error('Historical inbox transaction custody is unresolved');}
 if(identity(dbPath)!==expectedSha256)throw new Error('Historical inbox source identity changed');
 const scratch=mkdtempSync(join(tmpdir(),'koios-offline-inbox-')),snapshot=join(scratch,'source.sqlite');let db;
 try{
  copyFileSync(dbPath,snapshot);if(identity(snapshot)!==expectedSha256)throw new Error('Historical inbox snapshot identity changed');
  db=new DatabaseSync(snapshot,{readOnly:true});db.exec('PRAGMA query_only=on');db.exec('BEGIN');
  const rows=[];let bytes=0;
  for(const row of db.prepare('SELECT id,activity_json,observed_at,recipient FROM activitypub_verified_arrivals WHERE id>? ORDER BY id ASC LIMIT 256').iterate(lastId)){
   const size=Buffer.byteLength(row.activity_json);if(size>4*1024*1024)throw new Error('Historical inbox representation exceeds its bound');if(bytes+size>4*1024*1024)break;bytes+=size;
   rows.push({id:row.id,activity:JSON.parse(row.activity_json),observedAt:row.observed_at,recipient:row.recipient});
  }
  db.exec('COMMIT');if(identity(dbPath)!==expectedSha256)throw new Error('Historical original inbox source changed');
  return {rows,lastId:rows.length?rows.at(-1).id:lastId};
 }finally{db?.close();rmSync(scratch,{recursive:true,force:true});}
}
