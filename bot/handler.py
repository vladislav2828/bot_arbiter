'''
    Данный модуль отвечает за обработку получаемых данных от сервера по вебсокету,
    а также подготавливает данные для отправки на сервер для вебсокет соеденения.
    Также есть класс Arbitrator который отвечает за поиск связок арбитражных сделок, как только они находятся, отправляются пользователям тг.
    В методе __init__ можно изменить минимальный % спреда.

'''

from __future__ import annotations
import random
import asyncio
import time
from decimal import Decimal

import ujson
import ast
import gzip

from logger_ import logger
from db import dict_fetch_query, dict_execute_query



def handler_exception(func: object) -> object:
    # Декоратор обработчик исключений.
    async def wrapper(*args):
        try:
            await func(*args)

        except KeyError as key_error:
            logger.warning(f'{key_error}: {func.__name__}')

        except AttributeError as attribute_error:
            logger.warning(f'{attribute_error}: {func.__name__}')

        except IndexError as index_error:
            logger.warning(f'{index_error}: {func.__name__}')

        except TypeError as type_error:
            logger.warning(f'{type_error}: {func.__name__}')

        except Exception as exception:
            logger.warning(f'{exception}: {func.__name__}')

    return wrapper


class Handler:
    '''
    Обработчик отправляемых данных на сервер для подключения websocket
    obj: объект с базы данных
    pair: пара символов
    name: название биржи
    Возвращает готовый объект для отправки на сервер
    '''

    def __init__(self, obj: dict, pair: str, name: str) -> dict:
        self.obj = ujson.loads(obj)
        self.pair = pair
        self.name = name

    def handler_bitfinex(self):
        self.obj.update({"symbol": self.pair})
        return self.obj

    def handler_bitstamp(self):
        self.obj.get('data').update({"channel": self.pair})
        return self.obj

    def handler_bybit(self):
        self.obj.get('params').update({"symbol": self.pair})
        return self.obj

    def handler_ftx(self):
        self.obj.update({"market": self.pair})
        return self.obj

    def handler_huobi(self):
        self.obj.update({"sub": self.pair})
        self.obj.update({"id": str(random.randint(1, 100_000000))})
        return self.obj

    def handler_binance(self):
        self.obj.update({"params": [self.pair]})
        self.obj.update({"id": random.randint(1, 100_000000)})
        return self.obj

    def handler_exmo(self):
        self.obj.update({"topics": [self.pair]})
        return self.obj

    def handler_poloniex(self):
        self.obj.update({"symbols": [self.pair]})
        return self.obj

    def return_handler(self):
        self.dict_handler = {"Bitfinex": self.handler_bitfinex,
                             "Bitstamp": self.handler_bitstamp,
                             "Bybit": self.handler_bybit,
                             "FTX": self.handler_ftx,
                             "Huobi": self.handler_huobi,
                             "Binance": self.handler_binance,
                             "Exmo": self.handler_exmo,
                             "Poloniex": self.handler_poloniex}

        return self.dict_handler[self.name]()


async def additional_processing_huobi(websocket: websockets.legacy.client.WebSocketClientProtocol, ws_obj):
    """
    Дополнительная функция для отправки ping на биржу Huobi в бинарном формате.
    websocket: активный websocket
    ws_obj: получаемые данные с потока
    """
    ws_obj = ujson.loads(gzip.decompress(ws_obj))
    if ws_obj.get('ping'):
        await websocket.send(ujson.dumps({'pong': ws_obj.get('ping')}))


@handler_exception
async def send_ping(websocket: websockets.legacy.client.WebSocketClientProtocol, ping_interval: int, name_exchange: str):
    '''Функция отправки ping'''
    while True:
        dict_pings = {"Poloniex": ujson.dumps({'event': 'ping'}),
                      "FTX": ujson.dumps({'op': 'ping'}),
                      "Bybit": ujson.dumps({'op': 'ping'}),
                      "Bitfinex": ujson.dumps({"event": "ping", "cid": random.randint(1, 100_000000)}),
                      "Bitstamp": ujson.dumps({"event": "bts:heartbeat"})
                      }

        if dict_pings.get(name_exchange):
            await asyncio.sleep(ping_interval)
            await websocket.send(dict_pings[name_exchange])

        else:
            break


class HandlerWS:
    """
    Обработчик websocket
    pair: валютная пара
    ws_obj: входящие данные от сервера
    name: название биржи
    db_client: объект клиента базы данных
    connect: объект соеденения с бд
    """

    def __init__(self, pair: str, socket: websockets.legacy.client.WebSocketClientProtocol, ws_obj: object, name: str, db_client: ClientDataBase, pool: asyncpg.pool.Pool):
        self.pair = pair
        self.socket = socket
        self.ws_obj = ws_obj
        self.name = name
        self.db_client = db_client
        self.pool = pool

    @handler_exception
    async def handler_bitfinex(self):
        self.ws_obj = ast.literal_eval(self.ws_obj)
        if self.ws_obj[1][2] < 0:
            await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], str(self.ws_obj[1][0]), self.pair, self.name)
        else:
            await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], str(self.ws_obj[1][0]), self.pair, self.name)

    @handler_exception
    async def handler_bitstamp(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], self.ws_obj['data']['bids'][0][0], self.pair,
                                           self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], self.ws_obj['data']['asks'][0][0], self.pair,
                                           self.name)

    @handler_exception
    async def handler_bybit(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], self.ws_obj['data']['bidPrice'], self.pair, self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], self.ws_obj['data']['askPrice'], self.pair, self.name)

    @handler_exception
    async def handler_ftx(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        if self.ws_obj['data']['bids']:
            await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], str(self.ws_obj['data']['bids'][0][0]), self.pair, self.name)
        if self.ws_obj['data']['asks']:
            await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], str(self.ws_obj['data']['asks'][0][0]), self.pair, self.name)

    @handler_exception
    async def handler_huobi(self):
        self.ws_obj = ujson.loads(gzip.decompress(self.ws_obj))
        if self.ws_obj.get('ping'):
            await self.socket.send(ujson.dumps({'pong': self.ws_obj.get('ping')}))

        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], str(self.ws_obj['tick']['bids'][0][0]), self.pair, self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], str(self.ws_obj['tick']['asks'][0][0]), self.pair, self.name)

    @handler_exception
    async def handler_binance(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], self.ws_obj['b'], self.pair, self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], self.ws_obj['a'], self.pair, self.name)

    @handler_exception
    async def handler_exmo(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], self.ws_obj['data']['bid'][0][0], self.pair, self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], self.ws_obj['data']['ask'][0][0], self.pair, self.name)

    @handler_exception
    async def handler_poloniex(self):
        self.ws_obj = ujson.loads(self.ws_obj)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_bids'], self.ws_obj['data'][0]['bids'][0][0], self.pair,
                                           self.name)
        await self.db_client.run_execute(self.pool, dict_execute_query['update_price_asks'], self.ws_obj['data'][0]['asks'][0][0], self.pair,
                                           self.name)

    async def run_handler(self):
        self.dict_handler = {"Bitfinex": self.handler_bitfinex,
                             "Bitstamp": self.handler_bitstamp,
                             "Bybit": self.handler_bybit,
                             "FTX": self.handler_ftx,
                             "Huobi": self.handler_huobi,
                             "Binance": self.handler_binance,
                             "Exmo": self.handler_exmo,
                             "Poloniex": self.handler_poloniex}

        await self.dict_handler[self.name]()


class Arbitrator:
    '''
    Класс арбитраж, находит связки.
    Как только найдена новая связка, рассылается всем пользователям тг.
    db_client : экземпляр ClientDataBase.
    pool : активный пул подключений к бд.
    pair : валютная пара.
    bot : экземпляр telegram.Bot.
    percent : минимальный процент спреда, можно указать желаемый процент, по умолчанию 1%.

    '''
    def __init__(self, db_client: ClientDataBase, pool: asyncpg.pool.Pool, pair: str, bot: aiogram.bot.bot.Bot, percent=1):
        self.db_client = db_client
        self.pool = pool
        self.pair = pair
        self.bot = bot
        self.percent = percent
        self.bids = []
        self.asks = []
        self.finished_bundle = [None, None, None, None]

    @handler_exception
    async def removing_old_links(self):
        '''
            Функция проверки ранее отправленых связок а также удаление, для избежание повторных отправок связок
        '''
        time_message = await self.db_client.run_fetch(self.pool, dict_fetch_query['all_time_message']) # получаю время всех отпрвленых связок из бд

        if time_message:  # если есть связки в бд
            res_time = time.time() - time_message[0][0] # фиксирую сколько прошло времени с отправки связки

            if res_time > 440:  # если прошло больше 440 сек.
                await self.db_client.run_execute(self.pool, dict_execute_query['delete_message_by_time'], time_message[0][0]) # удаляю эту связку из бд
                logger.info(f'Удалена старая связка {time_message[0][0]}')

    #@handler_exception
    async def spread_calculation(self) -> tuple:
        '''
            Функция подсчета спреда
        '''
        await asyncio.sleep(5)
        self.obj_prices = await self.db_client.run_fetch(self.pool, dict_fetch_query['obj_price_by_actual_pair'], self.pair) # получаю цены со всех бирж по каждому символу

        for i in self.obj_prices:  # добавляю их в списки bids/asks
            self.bids.append(Decimal(i[4]))
            self.asks.append(Decimal(i[3]))

        self.price_bids = min(self.bids)  # получаю минимальную цену покупки
        self.price_asks = max(self.asks)  # получаю максимальную цену продажи
        self.spread = 100 * (self.price_asks - self.price_bids) / self.price_bids # высчитываю спред
        return self.spread, self.obj_prices # возвращает спред и объект цены

    @handler_exception
    async def ligament_formation(self, spread: float, obj_price: list):
        '''
            Функция подготовки готовой связки
        '''
        if float(spread) > self.percent: # если спред больше чем необходимый процент
            for price in obj_price: # прохожу по ценовым объектам бирж для определения кому пренадлежат min/max цены и добавляю в список результат название биржи
                price = list(price) # преобразую объект с ценой в list

                if str(self.price_bids) in price and price.index(str(self.price_bids)) == 4: # если цена покупки есть в объекте и она соответствует цене покупке
                    self.finished_bundle[0] = price[0] # добавляю название биржи-покупки

                if str(self.price_asks) in price and price.index(str(self.price_asks)) == 3: # если цена продажи есть в объекте и она соответствует цене продажи
                    self.finished_bundle[1] = price[0] # добавляю название биржи-продажи

            self.finished_bundle[2] = price[1]  # добавляю пару
            self.finished_bundle[3] = spread  # добавляю итоговый спред
            logger.info(f'Найдена новая связка {self.finished_bundle}')

    @handler_exception
    async def bundle_mailing(self):
        '''
            Функция отправки готовой связки
        '''
        dict_exchanges = {value[0]: value[1] for value in await self.db_client.run_fetch(self.pool, dict_fetch_query['all_exchanges'])} # словарь с названием биржи и ссылкой

        message = f'Найдена связка!\n' \
                  f'Пара <b>{self.finished_bundle[2].upper()}</b>\n' \
                  f'Купить на  <a href="{dict_exchanges[self.finished_bundle[0]]}"><b>{self.finished_bundle[0]}</b></a>\n' \
                  f'Продать на <a href="{dict_exchanges[self.finished_bundle[1]]}"><b>{self.finished_bundle[1]}</b></a>\n' \
                  f'Спред {"%.2f" % float(self.finished_bundle[3])}%'  # отправляемое сообщение пользователю

        time_temprory_message = await self.db_client.run_fetch(self.pool, dict_fetch_query['time_message'], self.finished_bundle[2], self.finished_bundle[0], self.finished_bundle[1])  # получаю время схожих связок
        present_time = int(time.time())

        if self.finished_bundle[0] != self.finished_bundle[1]:  # если название бирж покупки/продажи не совпадают

            if time_temprory_message:  # если есть схожие связки
                countdown = (present_time - int(
                    list(time_temprory_message)[0][0]))  # высчитываю прошедшее время с отправки связки

                if countdown > 440 and countdown < 450: # если прошло время,для избежания повторной отправки связки

                    for i in await self.db_client.run_fetch(self.pool, dict_fetch_query['all_id']):  # получаю id пользователй
                        await self.bot.send_message(i[0], message)  # отправляю информацию о связке

                    await self.db_client.run_execute(self.pool, dict_execute_query['delete_message_by_time'], time_temprory_message[0][0])  # удаляю старую связку чтоб повторно не отрпавлялась
                    await self.db_client.run_execute(self.pool, dict_execute_query['add_temprory_message'], self.finished_bundle[2], self.finished_bundle[0], self.finished_bundle[1], present_time)  # добавляю новую связку в бд
                    self.finished_bundle = [None, None, None, None] # очищаю список

                else: pass

            else: # если нет схожих связок
                for i in await self.db_client.run_fetch(self.pool, dict_fetch_query['all_id']):  # получаю id пользователй
                    await self.bot.send_message(i[0], message)  # отправляю информацию о связке

                await self.db_client.run_execute(self.pool, dict_execute_query['add_temprory_message'], self.finished_bundle[2], self.finished_bundle[0], self.finished_bundle[1], present_time)
                self.finished_bundle = [None, None, None, None]  # очищаю список



    @handler_exception
    async def calculation(self):
        '''
        Функция запуска всей логики связанной с поиском и отправкой связок.
        Как только находится связка, отправляется всем пользователям тг.
        '''
        while True:

            await self.removing_old_links()
            task = asyncio.create_task(self.spread_calculation())
            res_spread_calculation = await task
            spread, obj_price = res_spread_calculation

            await self.ligament_formation(spread, obj_price)
            await self.bundle_mailing()

            self.bids.clear()
            self.asks.clear()

