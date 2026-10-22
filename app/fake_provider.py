import asyncio, hashlib, hmac, os, random, uuid
import httpx
from fastapi import FastAPI, Request

def provider(name):
    app=FastAPI()
    @app.post('/tasks',status_code=202)
    async def create(req:Request):
        cfg=os.getenv('FAKE_'+name.upper()+'_SYNC','ok')
        if cfg=='503': return __import__('fastapi').responses.JSONResponse({'error':'unavailable'},status_code=503)
        if cfg=='timeout': await asyncio.sleep(30)
        ext=str(uuid.uuid4()); body=await req.json()
        async def callback():
            await asyncio.sleep(float(os.getenv('FAKE_DELAY','3')))
            # Prompt markers make failure paths deterministic in integration tests.
            failure=os.getenv('FAKE_'+name.upper()+'_ERROR','')
            prompt=body.get('prompt','')
            for error in ('technical','moderation','payload'):
                if f'[fail:{name}:{error}]' in prompt or f'[fail:{error}]' in prompt:
                    failure=error
            if failure:
                event={'external_id':ext,'status':'failed','error_type':failure}
            else: event={'external_id':ext,'status':'completed','url':'https://files.invalid/'+ext}
            raw=__import__('json').dumps(event,separators=(',',':')).encode()
            secret=os.getenv('WEBHOOK_SECRET','dev-secret-change-me')
            sig=hmac.new(secret.encode(),raw,hashlib.sha256).hexdigest()
            async with httpx.AsyncClient() as c:
                try:
                    await c.post(os.getenv('WEBHOOK_URL','http://localhost:8000/webhooks/providers/'+name),content=raw,headers={'X-Signature':sig})
                    if os.getenv('FAKE_DUPLICATE')=='1': await c.post(os.getenv('WEBHOOK_URL','http://localhost:8000/webhooks/providers/'+name),content=raw,headers={'X-Signature':sig})
                except Exception: pass
        if os.getenv('FAKE_SILENT')!='1' and f'[silent:{name}]' not in body.get('prompt',''): asyncio.create_task(callback())
        return {'external_id':ext}
    return app
alpha_app=provider('alpha')
beta_app=provider('beta')
