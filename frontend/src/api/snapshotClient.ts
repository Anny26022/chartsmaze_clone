import type { SnapshotResult, SnapshotTask } from './snapshotEngine';
let worker: Worker | undefined;
let sequence = 0;
const pending = new Map<number,{resolve:(result:SnapshotResult)=>void;reject:(error:Error)=>void;timer:ReturnType<typeof setTimeout>}>();
let fallback: Promise<ReturnType<typeof import('./snapshotEngine')['createSnapshotEngine']>> | undefined;
function stop(message: string) {
  worker?.terminate(); worker = undefined;
  for (const item of pending.values()) { clearTimeout(item.timer); item.reject(new Error(message)); }
  pending.clear();
}
export function runSnapshotTask(task: SnapshotTask): Promise<SnapshotResult> {
  if (typeof Worker === 'undefined') {
    fallback ??= import('./snapshotEngine').then(module => module.createSnapshotEngine());
    return fallback.then(run => run(task));
  }
  if (!worker) {
    worker = new Worker(new URL('./snapshot.worker.ts',import.meta.url),{type:'module'});
    worker.onmessage = (event: MessageEvent<{id:number;result:SnapshotResult;error?:string}>) => {
      const item = pending.get(event.data.id);
      if (!item) return;
      pending.delete(event.data.id);clearTimeout(item.timer);
      if (event.data.error) item.reject(new Error(event.data.error));
      else item.resolve(event.data.result);
    };
    worker.onerror = () => stop('Scanner worker failed. Please try again.');
    worker.onmessageerror = () => stop('Scanner worker returned an invalid response.');
  }
  return new Promise((resolve,reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => stop('Scanner loading timed out. Please try again.'),60000);
    pending.set(id,{resolve,reject,timer});
    try { worker!.postMessage({id,task}); }
    catch { stop('Unable to start scanner processing.'); }
  });
}
