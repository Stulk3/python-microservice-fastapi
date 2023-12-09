from aio_pika import connect_robust, ExchangeType, IncomingMessage
import asyncio

class RabbitMQClient:
   def __init__(self, host, username, password):
       self.host = host
       self.username = username
       self.password = password
       self.loop = asyncio.get_event_loop()

   async def connect(self):
       self.connection = await connect_robust(f"amqp://{self.username}:{self.password}@{self.host}", loop=self.loop)
       self.channel = await self.connection.channel()

   async def close(self):
       await self.connection.close()

   async def send_message(self, exchange_name, routing_key, message):
       exchange = await self.channel.declare_exchange(exchange_name, ExchangeType.DIRECT)
       await exchange.publish(
           message,
           routing_key=routing_key
       )
