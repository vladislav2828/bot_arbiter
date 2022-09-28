'''
    Данный модуль для взаимодействия с бд.
    Для полноценной работы бота создано 4 таблицы:

    1. exchanges: таблица с информацией о бирже
    -----------------------------------------
    | name | url | ws | sub | ping_interval |
    -----------------------------------------

    2. pair_and_prices: таблица с ценами валютных пар
    ----------------------------------------------------
    | name_exchange | actual_pair | pair | bids | asks |
    ----------------------------------------------------

    3. temprory_message: таблица с временной информацией о отправленных всязках
    -----------------------------
    | pair | buy | sell | time_ |
    -----------------------------

    4. users: таблица с пользователями
    ----------------------------
    | id | username | data_reg |
    ----------------------------

'''



import asyncpg


dict_fetch_query = {'all_exchanges': 'SELECT * FROM exchanges',
                    'obj_price_by_name_exchange': 'SELECT * FROM pair_and_prices WHERE name_exchange = $1',
                    'obj_price_by_actual_pair': 'SELECT * FROM pair_and_prices WHERE actual_pair = $1',
                    'all_actual_pair': 'SELECT actual_pair FROM pair_and_prices',
                    'user_by_id': 'SELECT * FROM users WHERE id = $1',
                    'all_id': 'SELECT id FROM users',
                    'time_message': 'SELECT time_ FROM temprory_message WHERE pair = $1 AND buy = $2 AND sell = $3',
                    'all_time_message': 'SELECT time_ FROM temprory_message'
                    } # словарь с запросами fetch

dict_execute_query = {'delete_message_by_time': 'DELETE FROM temprory_message WHERE time_ = $1',
                      'update_price_bids': 'UPDATE pair_and_prices SET bids = $1 WHERE actual_pair = $2 AND name_exchange = $3',
                      'update_price_asks': 'UPDATE pair_and_prices SET asks = $1 WHERE actual_pair = $2 AND name_exchange = $3',
                      'add_user': 'INSERT INTO users (id, username, data_reg) VALUES ($1, $2, $3)',
                      'add_temprory_message': 'INSERT INTO temprory_message (pair, buy, sell, time_) VALUES ($1, $2, $3, $4)',
                      'add_exchange': 'INSERT INTO exchanges (name, url, ws, sub, ping_interval) VALUES ($1, $2, $3, $4, $5)',
                      'add_obj_price': 'INSERT INTO pair_and_prices (name_exchange, actual_pair, pair, bids, asks) VALUES ($1, $2, $3, $4, $5)'
                      } # словарь с иными запросами


class ClientDataBase:
    '''
    Клиент взаимодействия с базой данных
    '''

    async def run_fetch(self, pool: asyncpg.pool.Pool, query: str, *args) -> list:

        if isinstance(query, str):
            async with pool.acquire() as connect:
                return await connect.fetch(query, *args)

        else: raise TypeError('query должен быть строкой')

    async def run_execute(self, pool: asyncpg.pool.Pool, query: str, *args):

        if isinstance(query, str):
            async with pool.acquire() as connect:
                return await connect.fetch(query, *args)

        else: raise TypeError('query должен быть строкой')
