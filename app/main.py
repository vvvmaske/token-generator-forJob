import asyncio, hashlib, hmac, json, os, uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import asyncpg, httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

DB=os.getenv('DATABASE_URL','postgresql://app:app@localhost:5432/generator')
SECRET=os.getenv('WEBHOOK_SECRET','dev-secret-change-me').encode()
TIMEOUT=int(os.getenv('PROVIDER_TIMEOUT','60'))
URLS={'alpha':os.getenv('ALPHA_URL','http://localhost:8101'),'beta':os.getenv('BETA_URL','http://localhost:8102')}
pool=None
class Generate(BaseModel): user_id:str; model_slug:str; prompt:str; ref_images:list[str]=Field(default_factory=list)
class Pay(BaseModel): payment_id:str; user_id:str; tokens:int; status:str

def sign(body:bytes): return hmac.new(SECRET,body,hashlib.sha256).hexdigest()
def valid_sig(body,sig): return hmac.compare_digest(sign(body),sig or '')

async def db(): return pool

async def dispatch(task_id, leg):
    """Send an attempt; synchronous provider failures use the same transition as webhook failures."""
    async with pool.acquire() as c:
        async with c.transaction():
            task=await c.fetchrow('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',task_id)
            if not task or task['status']!='running': return
            old=await c.fetchrow('SELECT status FROM attempts WHERE task_id=$1 AND leg_id=$2',task_id,leg['id'])
            if old: return
            attempt=await c.fetchrow("INSERT INTO attempts(task_id,leg_id,provider,status) VALUES($1,$2,$3,'sending') RETURNING id",task_id,leg['id'],leg['provider'])
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            res=await client.post(URLS[leg['provider']]+ '/tasks',json={'task_id':str(task_id),'prompt':(await pool.fetchrow('SELECT prompt,ref_images FROM tasks WHERE id=$1',task_id))['prompt']})
            res.raise_for_status(); external=res.json()['external_id']
        async with pool.acquire() as c:
            await c.execute("UPDATE attempts SET external_id=$2,status='waiting' WHERE id=$1 AND status='sending'",attempt['id'],external)
            await c.execute('UPDATE tasks SET current_leg=$2 WHERE id=$1 AND status=\'running\'',task_id,leg['priority'])
    except Exception:
        await finish_attempt(task_id,attempt['id'],'failed','technical',None)

async def choose_and_dispatch(task_id, previous_error=None):
    async with pool.acquire() as c:
        task=await c.fetchrow('SELECT * FROM tasks WHERE id=$1',task_id)
        if not task or task['status']!='running': return
        legs=await c.fetch("SELECT l.* FROM legs l WHERE l.model_slug=$1 AND l.priority>$2 AND l.max_ref_images >= $3 ORDER BY l.priority",task['model_slug'],task['current_leg'],len(json.loads(task['ref_images']) if isinstance(task['ref_images'], str) else task['ref_images']))
        leg=next((x for x in legs if not (previous_error=='moderation' and x['skip_on_moderation'])),None)
    if leg: await dispatch(task_id,leg)
    else: await refund(task_id)

async def refund(task_id):
    async with pool.acquire() as c:
        async with c.transaction():
            t=await c.fetchrow('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',task_id)
            if not t or t['status']!='running': return
            price=await c.fetchval('SELECT price FROM models WHERE slug=$1',t['model_slug'])
            await c.execute("INSERT INTO ledger(user_id,amount,reason,reference) VALUES($1,$2,'refund',$3) ON CONFLICT DO NOTHING",t['user_id'],price,str(task_id))
            await c.execute('UPDATE users SET balance=balance+$2 WHERE id=$1',t['user_id'],price)
            await c.execute("UPDATE tasks SET status='failed' WHERE id=$1",task_id)

async def finish_attempt(task_id,attempt_id,status,error=None,result=None):
    next_error=None
    async with pool.acquire() as c:
        async with c.transaction():
            t=await c.fetchrow('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',task_id)
            a=await c.fetchrow('SELECT * FROM attempts WHERE id=$1 FOR UPDATE',attempt_id)
            if not t or not a or t['status']!='running' or a['status'] not in ('waiting','sending'): return
            await c.execute('UPDATE attempts SET status=$2,error_type=$3,finished_at=now() WHERE id=$1',attempt_id,status,error)
            if status=='completed': await c.execute("UPDATE tasks SET status='completed',result_url=$2 WHERE id=$1",task_id,result); return
            if error=='payload': await c.execute("UPDATE tasks SET last_error='payload' WHERE id=$1",task_id); do_refund=True
            else:
                await c.execute('UPDATE tasks SET current_leg=$2,last_error=$3 WHERE id=$1',task_id,a['leg_id'] and (await c.fetchval('SELECT priority FROM legs WHERE id=$1',a['leg_id'])),error)
                next_error=error; do_refund=False
    if do_refund: await refund(task_id)
    else: await choose_and_dispatch(task_id,next_error)

async def watchdog():
    while True:
        try:
            async with pool.acquire() as c:
                rows=await c.fetch("SELECT a.id,a.task_id FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.status IN ('waiting','sending') AND a.started_at < now()-($1::int * interval '1 second') AND t.status='running' FOR UPDATE OF a SKIP LOCKED LIMIT 50",TIMEOUT)
            for a in rows: await finish_attempt(a['task_id'],a['id'],'failed','technical')
        except Exception: pass
        await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app):
    global pool
    pool=await asyncpg.create_pool(DB,min_size=2,max_size=10)
    worker=asyncio.create_task(watchdog())
    yield
    worker.cancel(); await pool.close()
app=FastAPI(lifespan=lifespan)

@app.post('/generate',status_code=202)
async def generate(x:Generate):
    tid=uuid.uuid4()
    async with pool.acquire() as c:
        async with c.transaction():
            model=await c.fetchrow('SELECT * FROM models WHERE slug=$1',x.model_slug)
            if not model: raise HTTPException(404,'unknown model')
            legs=await c.fetch("SELECT * FROM legs WHERE model_slug=$1 AND max_ref_images >= $2 ORDER BY priority",x.model_slug,len(x.ref_images))
            if not legs: raise HTTPException(400,'no compatible provider leg')
            await c.execute('INSERT INTO users(id) VALUES($1) ON CONFLICT DO NOTHING',x.user_id)
            u=await c.fetchrow('SELECT balance FROM users WHERE id=$1 FOR UPDATE',x.user_id)
            active=await c.fetchval("SELECT count(*) FROM tasks WHERE user_id=$1 AND status='running'",x.user_id)
            if active>=3: raise HTTPException(429,'too many active tasks')
            if u['balance']<model['price']: raise HTTPException(402,'insufficient tokens')
            await c.execute('UPDATE users SET balance=balance-$2 WHERE id=$1',x.user_id,model['price'])
            await c.execute("INSERT INTO ledger(user_id,amount,reason,reference) VALUES($1,$2,'generation',$3)",x.user_id,-model['price'],str(tid))
            await c.execute("INSERT INTO tasks(id,user_id,model_slug,prompt,ref_images,status,current_leg) VALUES($1,$2,$3,$4,$5,'running',-1)",tid,x.user_id,x.model_slug,x.prompt,json.dumps(x.ref_images))
    asyncio.create_task(dispatch(tid,legs[0])); return {'task_id':str(tid)}

@app.post('/webhooks/providers/{name}')
async def provider_hook(name:str,request:Request):
    raw=await request.body()
    if not valid_sig(raw,request.headers.get('X-Signature')): raise HTTPException(401,'invalid signature')
    data=json.loads(raw)
    if name not in URLS or data.get('status') not in ('completed','failed'):
        raise HTTPException(400,'invalid provider event')
    async with pool.acquire() as c: a=await c.fetchrow('SELECT id,task_id FROM attempts WHERE provider=$1 AND external_id=$2',name,data['external_id'])
    if not a: return {'ok':True}
    await finish_attempt(a['task_id'],a['id'],data['status'],data.get('error_type'),data.get('url')); return {'ok':True}

@app.post('/webhooks/payments')
async def payment_hook(request:Request):
    raw=await request.body()
    if not valid_sig(raw,request.headers.get('X-Signature')): raise HTTPException(401,'invalid signature')
    p=Pay.model_validate_json(raw)
    if p.status!='paid' or p.tokens<=0: return {'ok':True}
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute('INSERT INTO users(id) VALUES($1) ON CONFLICT DO NOTHING',p.user_id)
            inserted=await c.fetchval("INSERT INTO payments(payment_id,user_id,tokens,status) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING payment_id",p.payment_id,p.user_id,p.tokens,p.status)
            if inserted:
                await c.execute("INSERT INTO ledger(user_id,amount,reason,reference) VALUES($1,$2,'payment',$3)",p.user_id,p.tokens,p.payment_id)
                await c.execute('UPDATE users SET balance=balance+$2 WHERE id=$1',p.user_id,p.tokens)
    return {'ok':True}

@app.get('/tasks/{tid}')
async def task_get(tid:uuid.UUID):
    async with pool.acquire() as c:
        t=await c.fetchrow('SELECT * FROM tasks WHERE id=$1',tid)
        if not t: raise HTTPException(404)
        attempts=await c.fetch("SELECT provider,external_id,error_type,status,started_at,finished_at FROM attempts WHERE task_id=$1 ORDER BY id",tid)
    return {'task_id':str(tid),'status':t['status'],'current_leg':t['current_leg'],'result_url':t['result_url'],'attempts':[dict(a) for a in attempts]}

@app.get('/users/{uid}/balance')
async def balance(uid:str):
    async with pool.acquire() as c:
        u=await c.fetchrow('SELECT balance FROM users WHERE id=$1',uid)
        if not u: raise HTTPException(404)
        rows=await c.fetch('SELECT amount,reason,reference,created_at FROM ledger WHERE user_id=$1 ORDER BY id DESC LIMIT 50',uid)
    return {'balance':u['balance'],'ledger':[dict(r) for r in rows]}



