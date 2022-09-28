from __future__ import annotations

import ujson
import websockets
import asyncio

from logger_ import logger
from handler import send_ping


class WSClient:
    '''
    Клиент взаимодействия с серверами по websocket

    url: адрес сервера
    send_data: отправляемые при подключении данные на сервер
    ping_interval: через сколько секунд отправить пинг на сервер
    handler: обработчик полученных данных
    db_client: объект клиента бд, передается в обработчик ws
    connect: объект соеденения с бд, передается в обработчик ws
    pair: валютная пара, передается в обработчик ws
    name_exchange: название биржи, передается в обработчик ws
    '''
    def __init__(self, url: str, send_data: dict, ping_interval: int=None, handler: HandlerWS=None, db_client: ClientDataBase=None, pool: asyncpg.pool.Pool=None, pair: str=None, name_exchange: str=None):
        self.url = url
        self.send_data = send_data
        self.ping_interval = ping_interval
        self.handler = handler
        self.db_client = db_client
        self.pool = pool
        self.pair = pair
        self.name_exchange = name_exchange

    async def run_socket(self):

        try:
            async with websockets.connect(self.url, open_timeout=30, ping_timeout=None) as websocket:
                await websocket.send(ujson.dumps(self.send_data))

                asyncio.create_task(send_ping(websocket, self.ping_interval, self.name_exchange)) # запуск функции отправки ping

                while True:

                    recv = await websocket.recv()
                    await self.handler(self.pair, websocket, recv, self.name_exchange, self.db_client, self.pool).run_handler()  # запись цен в бд

        # блок переподключения websocket
        except websockets.exceptions.ConnectionClosedError as connection_closed_error:
            logger.warning(f'Попытка переподключения: {connection_closed_error}: {self.name_exchange}: {self.pair}')
            await self.run_socket()

        except websockets.exceptions.ConnectionClosedOK as connection_closed_ok:
            logger.warning(f'Попытка переподключения: {connection_closed_ok}: {self.name_exchange}: {self.pair}')
            await self.run_socket()
        except Exception as exception:
            logger.warning('Прочая ошибка в модуле ws_client', exception)





