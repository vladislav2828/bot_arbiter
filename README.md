# Бот-криптоарбитраж 

### Получает цены с разных бирж, высчитывает спред для криптоарбитража.
___
### Начало работ:

+ Вы можете воспользоваться готовыми образами с DockerHub
    + *docker pull valoha/bot_arbiter:latest*
    + *docker pull valoha/bot_db_postgres:latest*

**ИЛИ**

+ Необходимо установить все зависимости
  + ```python 
    pip install -r requirements.txt
    ```
+ Далее необходимо создать таблицы в бд PostgreSQL.


    table: <exchanges> таблица бирж

| name: str | url: str | ws: str | sub: json | ping_interval: int  |
|-----------|----------|---------|-----------|---------------------|
| Binance             | https://www.binance.com/ru | wss://stream.binance.com:9443/ws/stream? | {"method": "SUBSCRIBE", "params": [], "id": null} | None |

    table: <pair_and_prices> таблица цен

| name_exchange: str | actual_pair: str | pair: str | bids: str: | asks: str |
|--------------------|------------------|-----------|------------|-----------|
| Binance | btcusdt | xrpusdt@bookTicker | None | None      |
| Bybit | btcusdt | BTCUSDT | None  | None      | 

    table: <temprory_message> таблица временное хранение устаревших связок

| pair: str | buy: str | sell: str | time_: int |
|-----------|----------|-----------|------------|

    table: <users> таблица с пользователями

| id: int | username: str: | data_red: int |
|---------|----------------|---------------|


  + Вы также можете установить минимальный % спреда в классе, по умолчанию 1%
    + class Arbitrator в модуле handler.py 
    + ```python 
      def __init__(self, db_client: ClientDataBase, pool: asyncpg.pool.Pool, pair: str, bot: aiogram.bot.bot.Bot, percent=1):
      ```
    + параметр percent: укажите нужный вам минимальный %


### В модуле logic.py
  +  PASSWORD_DB = os.getenv(указать пароль из файла .env) 
  +  USER_DB = os.getenv(указать имя пользователя бд из файла .env) 
  +  NAME_DB = os.getenv(указать имя бд из файла .env)

  + В фунции run_schedule можно установить время напоминание о поиске связок.