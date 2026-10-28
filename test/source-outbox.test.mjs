import assert from "node:assert/strict";
import test from "node:test";
import http from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";

test("source outbox retains failed/paused deliveries, catches up beyond 20 messages and blocks conflicting receipts", { timeout: 15_000 }, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-outbox-"));
  const tokenFile = path.join(root, "credential"); await fs.writeFile(tokenFile, crypto.randomBytes(32).toString("base64url"), { mode: 0o600 });
  let calls = 0;
  const server = http.createServer(async (req, res) => {
    for await (const chunk of req) { /* drain */ }
    calls++;
    const status = calls === 1 ? 503 : calls === 2 ? 409 : calls === 3 ? 202 : 409;
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(calls === 2 ? { error: "subscription_paused" } : calls === 3 ? { receiptId: "durable-receipt", state: "queued" } : { error: "event_conflict" }));
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(async () => { await new Promise((r) => server.close(r)); await fs.rm(root, { recursive: true, force: true }); });
  const script = `
import asyncio, json, sys
from pathlib import Path
from omp_outbox import Outbox
from telegram_source import collect
async def main():
    directory, url, token_file = sys.argv[1:]
    file=Path(directory)/'outbox.sqlite'
    box=Outbox(file, 'test-source')
    body={'eventId':'source:1','type':'source.test','data':{'value':1}}
    box.enqueue(body,cursor=1)
    assert box.cursor()==1
    assert not await box.flush_one(url,token_file)
    assert box.status()['counts']=={'pending':1}
    box.close(); box=Outbox(file,'test-source')
    await asyncio.sleep(1.1)
    assert not await box.flush_one(url,token_file)
    assert box.status()['counts']=={'pending':1}
    await asyncio.sleep(2.1)
    assert await box.flush_one(url,token_file)
    assert box.status()['counts']=={'accepted':1}
    box.enqueue({'eventId':'conflict','type':'source.test','data':{}},cursor=2)
    assert not await box.flush_one(url,token_file)
    assert box.status()['counts'].get('blocked')==1
    assert box.cursor()==2
    assert box.next() is None
    class Message:
        def __init__(self,id): self.id=id
    class Client:
        async def iter_messages(self,entity,**kwargs):
            assert kwargs['min_id']==2 and kwargs['reverse']
            assert 'limit' not in kwargs
            for i in range(3,79): yield Message(i)
    await collect(Client(),None,100,box,lambda chat,msg: {'eventId':f'test:{msg.id}','type':'test.message','data':{}} if msg.id%2 else None)
    assert box.cursor()==78
    assert box.status()['counts'].get('pending')==38
    # Live arrival does not skip historical progress.
    box.enqueue({'eventId':'live:100','type':'test.message','data':{}})
    assert box.cursor()==78
    box.close()
    print(json.dumps({'retainedOnFailure':True,'pausedWasNotDuplicate':True,'blockedConflict':True,'catchupMessages':76,'cursor':78}))
asyncio.run(main())`;
  const result = await new Promise((resolve, reject) => {
    const child = spawn("python3", ["-c", script, root, `http://127.0.0.1:${server.address().port}/v1/events/test`, tokenFile], { env: { ...process.env, PYTHONPATH: path.resolve(import.meta.dirname, "../clients"), PYTHONDONTWRITEBYTECODE: "1" }, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = ""; child.stdout.on("data", (x) => { stdout += x; }); child.stderr.on("data", (x) => { stderr += x; });
    child.once("error", reject); child.once("exit", (code) => code === 0 ? resolve(JSON.parse(stdout)) : reject(new Error(stderr)));
  });
  assert.equal(result.catchupMessages, 76); assert.equal(result.cursor, 78); assert.equal(calls, 4);
});
