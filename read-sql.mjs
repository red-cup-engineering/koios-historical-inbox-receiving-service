// Explicit offline reading only. No runtime or audience clearance is produced.
import {readLegacySqlite} from '@red-cup-engineering/computer-system-engine/legacy-sqlite-reader';
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,openSync,readSync,closeSync} from 'node:fs';
const TABLE='activitypub_verified_arrivals';
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
 // The file is received by computer-system-engine's standalone, read-only, immutable, bounded reader; no JS here opens a database.
 const rows=readLegacySqlite(dbPath,[{table:TABLE,order:'id'}],(legacy)=>{
  if(!legacy.has(TABLE))throw new Error('Historical inbox source holds no verified arrivals');
  const found=[];let bytes=0;
  for(const row of legacy.rows(TABLE)){
   if(row.id<=lastId)continue;if(found.length>=256)break;
   const size=Buffer.byteLength(row.activity_json);if(size>4*1024*1024)throw new Error('Historical inbox representation exceeds its bound');if(bytes+size>4*1024*1024)break;bytes+=size;
   found.push({id:row.id,activity:JSON.parse(row.activity_json),observedAt:row.observed_at,recipient:row.recipient});
  }
  return found;
 });
 if(identity(dbPath)!==expectedSha256)throw new Error('Historical original inbox source changed');
 return {rows,lastId:rows.length?rows.at(-1).id:lastId};
}
