import { afterEach, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
import { createSnapshotEngine } from '../api/snapshotEngine';
import type { Snapshot } from '../api/snapshotScreen';
import type { ScreenerRunRequest } from '../types/screener';
const manifest=JSON.parse(readFileSync('public/data/current.json','utf8'));
const snapshot:Snapshot=JSON.parse(readFileSync(`public${manifest.datasetUrl}`,'utf8'));
const source={revision:snapshot.revision,url:'/stocks.json'};
const request:ScreenerRunRequest={asOfDate:snapshot.asOfDate,universe:'mainboard',page:1,pageSize:50,
  expressionTree:{type:'group',operator:'all',children:[]},sort:{field:'symbol',direction:'asc'}};
afterEach(()=>{vi.unstubAllGlobals();vi.resetModules();});
it('loads a revision once for concurrent screen and comparison tasks',async()=>{
 const loader=vi.fn(async()=>snapshot),run=createSnapshotEngine(loader);
 const [page,comparison]=await Promise.all([
  run({type:'screen',source,request}),run({type:'compare',source,symbols:[snapshot.stocks[0].symbol,'NOT_A_STOCK']})]);
 expect(loader).toHaveBeenCalledTimes(1);
 expect(page.type).toBe('screen');
 if(comparison.type==='compare') expect(comparison.result.invalidSymbols).toEqual(['NOT_A_STOCK']);
});
it('reuses matches when only pagination changes',async()=>{
 const stocks=[...snapshot.stocks];
 const filter=vi.spyOn(stocks,'filter');
 const run=createSnapshotEngine(async()=>({...snapshot,stocks}));
 const a=await run({type:'screen',source,request});
 const b=await run({type:'screen',source,request:{...request,page:2}});
 expect(filter).toHaveBeenCalledTimes(1);
 if(a.type==='screen'&&b.type==='screen') {
  expect(a.result?.matchCount).toBe(b.result?.matchCount);
  expect(a.result?.rows[0].symbol).not.toBe(b.result?.rows[0].symbol);
 }
 await run({type:'screen',source,request:{...request,sort:{field:'symbol',direction:'desc'}}});
 expect(filter).toHaveBeenCalledTimes(2);
});
it('retries a failed load and evicts old revisions',async()=>{
 const loader=vi.fn().mockRejectedValueOnce(new Error('offline')).mockImplementation(async(s)=>({...snapshot,revision:s.revision}));
 const run=createSnapshotEngine(loader);
 await expect(run({type:'screen',source,request})).rejects.toThrow('offline');
 await run({type:'screen',source,request});
 for(const id of ['a','b'])await run({type:'screen',source:{...source,revision:id.repeat(64)},request});
 await run({type:'screen',source,request});
 expect(loader).toHaveBeenCalledTimes(5);
});
it('unsupported conditions retain revision metadata for server fallback',async()=>{
 const run=createSnapshotEngine(async()=>snapshot);
 const result=await run({type:'screen',source,request:{...request,expressionTree:{type:'condition',condition:{instanceId:'x',conditionId:'PRICE_VS_SMA',parameters:{period:50,persistDays:10,comparison:'ABOVE'}}}}});
 expect(result.type==='screen'&&result.result).toBeNull();
 expect(result.type==='screen'&&result.revision).toBe(snapshot.revision);
});
it('fetches and decodes advertised gzip snapshots',async()=>{
 const compressed=gzipSync(JSON.stringify(snapshot));
 vi.stubGlobal('DecompressionStream',(await import('node:stream/web')).DecompressionStream);
 vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(compressed)));
 const result=await createSnapshotEngine()({type:'screen',source:{...source,url:'/stocks.json.gz'},request});
 expect(result.type==='screen'&&result.result?.matchCount).toBe(snapshot.totalStocks);
});
it('rejects snapshot session mismatches',async()=>{
 vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(JSON.stringify(snapshot))));
 await expect(createSnapshotEngine()({type:'screen',source:{...source,sessionDate:'2020-01-01'},request})).rejects.toThrow('revision/session mismatch');
});
it('routes concurrent worker replies by id and rejects pending tasks on failure',async()=>{
 class TestWorker {
  static last:TestWorker;
  onmessage?: (event:any)=>void; onerror?:()=>void;
  onmessageerror?:()=>void;
  tasks:any[]=[];terminate=vi.fn();
  constructor(){TestWorker.last=this;}
  postMessage(message:any){this.tasks.push(message);}
 }
 vi.stubGlobal('Worker',TestWorker);
 const {runSnapshotTask}=await import('../api/snapshotClient');
 const first=runSnapshotTask({type:'screen',source,request});
 const second=runSnapshotTask({type:'screen',source,request:{...request,page:2}});
 const worker=TestWorker.last;
 const result={type:'screen',result:null,revision:source.revision,sessionDate:snapshot.asOfDate};
 worker.onmessage!({data:{id:worker.tasks[1].id,result}});
 await expect(second).resolves.toEqual(result);
 const failure=expect(first).rejects.toThrow('worker failed');worker.onerror!();await failure;
 expect(worker.terminate).toHaveBeenCalledOnce();
});
