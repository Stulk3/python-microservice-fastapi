from fastapi import FastAPI, HTTPException
from app.api.models import CastOut, CastIn, CastUpdate
from app.api import db_manager
from aio_pika import IncomingMessage

app = FastAPI()
rabbitmq_client = RabbitMQClient(host="localhost", username="guest", password="guest")

@app.on_event("startup")
async def startup_event():
   await rabbitmq_client.connect()

@app.on_event("shutdown")
async def shutdown_event():
   await rabbitmq_client.close()

@app.post('/', response_model=CastOut, status_code=201)
async def create_cast(payload: CastIn):
   cast_id = await db_manager.add_cast(payload)

   response = {
       'id': cast_id,
       **payload.dict()
   }

   await rabbitmq_client.send_message("casts_exchange", "create_cast", response)

   return response

@app.get('/{id}/', response_model=CastOut)
async def get_cast(id: int):
   cast = await db_manager.get_cast(id)
   if not cast:
       raise HTTPException(status_code=404, detail="Cast not found")

   await rabbitmq_client.send_message("casts_exchange", "get_cast", cast)

   return cast
