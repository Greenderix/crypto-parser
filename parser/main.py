import requests
from config import api_route, api_ids, api_currencies, api_requests_rate, pg_dsn, redis_dsn
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import redis
import json
from loguru import logger

def init_db():
    logger.info('Create tables')
    with psycopg2.connect(dsn=pg_dsn) as conn:
        with conn.cursor() as curs:
            curs.execute("""
                CREATE TABLE IF NOT EXISTS public.markets
                (
                    id            SERIAL,
                    name          TEXT  NOT NULL,
                    currency      TEXT  NOT NULL,
                    current_price FLOAT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS markets_name_currency_uindex
                    ON public.markets (name, currency);
            """)
            conn.commit()
    logger.info('Table exist')

def parse_currencies():
    for currency in api_currencies:
        logger.info(f'Upd PostgreSQL data')
        response = requests.get(api_route, params={'ids': ",".join(api_ids), 'vs_currency': currency})
        if response.status_code == 200:
            for coin in response.json():
                name = coin['name']
                price = coin['current_price']
                with psycopg2.connect(dsn=pg_dsn) as conn:
                    with conn.cursor() as curs:
                        curs.execute('''
                            WITH t1 AS (
                                SELECT id
                                FROM markets
                                WHERE name = %s AND currency = %s
                            ), 
                            i AS (
                                INSERT INTO markets (name, currency, current_price)
                                SELECT %s, %s, %s
                                WHERE NOT EXISTS (SELECT 1 FROM t1)
                            ),
                            u AS (
                                UPDATE markets
                                SET current_price = %s
                                WHERE id = (SELECT id FROM t1)
                            )
                            SELECT NULL
                        ''', (name, currency, name, currency, price, price))
                        conn.commit()
            logger.info('Upd')
        else:
            logger.warning('API limit')
        time.sleep(api_requests_rate)

def update_redis():
    logger.info('Upd Redis data')
    redis_conn = redis.from_url(redis_dsn)
    with psycopg2.connect(dsn=pg_dsn, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as curs:
            curs.execute('SELECT * FROM markets ORDER BY id')
            markets = curs.fetchall()
    redis_conn.set('markets', json.dumps(markets))
    redis_conn.close()
    logger.info('Upd')

if __name__ == '__main__':
    init_db()
    try:
        while True:
            parse_currencies()
            update_redis()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
