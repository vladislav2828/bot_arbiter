import unittest
import os

from dotenv import load_dotenv
import asyncpg

from bot.db import ClientDataBase

load_dotenv()

class MyTestCase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.client_db = ClientDataBase()

    async def test_0_run_fetch(self):
        async with asyncpg.create_pool(user=os.getenv('user_db'), password=os.getenv('password_db'), database=os.getenv('name_db'), host='127.0.0.1', max_size=1000) as pool:
            self.assertEqual(type(await self.client_db.run_fetch(pool, '')), list)

    async def test_1_run_fetch(self):
        async with asyncpg.create_pool(user=os.getenv('user_db'), password=os.getenv('password_db'), database=os.getenv('name_db'), host='127.0.0.1', max_size=1000) as pool:
            with self.assertRaises(TypeError) as context:
                await self.client_db.run_fetch(pool, int)
                self.assertTrue('query должен быть строкой', context)

    async def test_0_run_execute(self):
        async with asyncpg.create_pool(user=os.getenv('user_db'), password=os.getenv('password_db'), database=os.getenv('name_db'), host='127.0.0.1', max_size=1000) as pool:
            with self.assertRaises(TypeError) as context:
                await self.client_db.run_execute(pool, int)
                self.assertTrue('query должен быть строкой', context)



if __name__ == '__main__':
    unittest.main()
