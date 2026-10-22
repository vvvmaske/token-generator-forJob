"""Compose integration tests. Start the stack and run: pytest -q."""
import asyncio, hashlib, hmac, json, os, uuid
import asyncpg, httpx, pytest

BASE=os.getenv('TEST_URL','http://localhost:8000')
DB=os.getenv('TEST_DATABASE_URL','postgresql://app:app@localhost:5432/generator')
SECRET=os.getenv('WEBHOOK_SECRET','dev-secret-change-me')
def signed(value):
    raw=json.dumps(value,separators=(',',':')).encode()
    return raw,hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest()
async def pay(c,user,tokens=100):
    raw,sig=signed({'payment_id':str(uuid.uuid4()),'user_id':user,'tokens':tokens,'status':'paid'})
    assert (await c.post('/webhooks/payments',content=raw,headers={'X-Signature':sig})).status_code==200
async def wait_task(c,tid,status,timeout=20):
    for _ in range(timeout*2):
        result=(await c.get(f'/tasks/{tid}')).json()
        if result['status']==status:return result
        await asyncio.sleep(.5)
    pytest.fail(f'task did not reach {status}: {result}')

@pytest.mark.asyncio
async def test_twenty_concurrent_same_payment_credit_once():
    user='pay-'+str(uuid.uuid4()); payment=str(uuid.uuid4())
    async with httpx.AsyncClient(base_url=BASE) as c:
        raw,sig=signed({'payment_id':payment,'user_id':user,'tokens':100,'status':'paid'})
        responses=await asyncio.gather(*[c.post('/webhooks/payments',content=raw,headers={'X-Signature':sig}) for _ in range(20)])
        assert all(r.status_code==200 for r in responses)
        assert (await c.get(f'/users/{user}/balance')).json()['balance']==100

@pytest.mark.asyncio
async def test_first_leg_technical_failure_falls_back_and_keeps_two_attempts():
    user='chain-'+str(uuid.uuid4())
    async with httpx.AsyncClient(base_url=BASE) as c:
        await pay(c,user)
        r=await c.post('/generate',json={'user_id':user,'model_slug':'image-fast','prompt':'[fail:alpha:technical]','ref_images':[]})
        assert r.status_code==202
        result=await wait_task(c,r.json()['task_id'],'completed')
        assert [a['provider'] for a in result['attempts']]==['alpha','beta']

@pytest.mark.asyncio
async def test_ten_concurrent_last_leg_failures_refund_once():
    user='refund-'+str(uuid.uuid4())
    async with httpx.AsyncClient(base_url=BASE) as c:
        await pay(c,user)
        r=await c.post('/generate',json={'user_id':user,'model_slug':'video-long','prompt':'[fail:beta:technical] [fail:alpha:technical]','ref_images':[]})
        assert r.status_code==202
        result=await wait_task(c,r.json()['task_id'],'failed')
        attempt=result['attempts'][-1]; event={'external_id':attempt['external_id'],'status':'failed','error_type':'technical'}
        raw,sig=signed(event)
        responses=await asyncio.gather(*[c.post('/webhooks/providers/alpha',content=raw,headers={'X-Signature':sig}) for _ in range(10)])
        assert all(x.status_code==200 for x in responses)
        balance=(await c.get(f'/users/{user}/balance')).json()
        assert balance['balance']==100
        assert sum(1 for x in balance['ledger'] if x['reason']=='refund')==1

@pytest.mark.asyncio
async def test_late_completed_after_watchdog_is_ignored_after_refund():
    user='late-'+str(uuid.uuid4())
    async with httpx.AsyncClient(base_url=BASE) as c:
        await pay(c,user)
        r=await c.post('/generate',json={'user_id':user,'model_slug':'video-long','prompt':'[silent:beta] [silent:alpha]','ref_images':[]})
        assert r.status_code==202; tid=r.json()['task_id']
        # Expire the current attempt directly, so the test is fast and deterministic.
        result=None
        for _ in range(40):
            result=(await c.get(f'/tasks/{tid}')).json()
            if result['attempts'] and result['attempts'][-1]['external_id']: break
            await asyncio.sleep(.25)
        assert result and result['attempts']
        conn=await asyncpg.connect(DB)
        try:
            await conn.execute("UPDATE attempts SET started_at=now()-interval '2 minutes' WHERE task_id=$1 AND status='waiting'",uuid.UUID(tid))
        finally: await conn.close()
        # Re-expire each newly dispatched leg; this avoids waiting for the production 60s timeout.
        result=None
        for _ in range(40):
            result=(await c.get(f'/tasks/{tid}')).json()
            if result['status']=='failed': break
            conn=await asyncpg.connect(DB)
            try:
                await conn.execute("UPDATE attempts SET started_at=now()-interval '2 minutes' WHERE task_id=$1 AND status='waiting'",uuid.UUID(tid))
            finally: await conn.close()
            await asyncio.sleep(.5)
        assert result['status']=='failed', result
        last=result['attempts'][-1]
        event={'external_id':last['external_id'],'status':'completed','url':'https://late.invalid/result'}
        raw,sig=signed(event)
        assert (await c.post('/webhooks/providers/alpha',content=raw,headers={'X-Signature':sig})).status_code==200
        final=(await c.get(f'/tasks/{tid}')).json()
        assert final['status']=='failed' and final['result_url'] is None
        balance=(await c.get(f'/users/{user}/balance')).json()
        assert balance['balance']==100
        assert sum(1 for x in balance['ledger'] if x['reason']=='refund')==1

