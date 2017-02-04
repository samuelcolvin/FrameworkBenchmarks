import os
import multiprocessing
from pathlib import Path

import aiohttp_jinja2
import aiopg.sa
import asyncpg
import jinja2
from aiohttp import web
from sqlalchemy.engine.url import URL

from .views import (
    json,
    single_database_query_orm,
    multiple_database_queries_orm,
    fortunes,
    updates,
    plaintext,

    single_database_query_raw,
    multiple_database_queries_raw,
    fortunes_raw,
    updates_raw,
)

RAW_CONNECTION = os.getenv('CONNECTION', 'CRM').upper() == 'RAW'

THIS_DIR = Path(__file__).parent


def pg_dsn() -> str:
    """
    :return: DSN url suitable for sqlalchemy and aiopg.
    """
    return str(URL(
        database='hello_world',
        password=os.getenv('PGPASS', 'benchmarkdbpass'),
        host=os.getenv('DBHOST', 'localhost'),
        port='5432',
        username=os.getenv('PGUSER', 'benchmarkdbuser'),
        drivername='postgres',
    ))


async def startup(app: web.Application):
    dsn = pg_dsn()
    # work out how many connection we're allowed per pool
    workers = multiprocessing.cpu_count()
    # from toolset/setup/linux/databases/postgresql/postgresql.conf
    max_connections = 2000
    # 10 gives us a little leeway
    max_size = int(max_connections / workers - 10)
    if RAW_CONNECTION:
        app['pg'] = await asyncpg.create_pool(dsn=dsn, min_size=max_size, max_size=max_size, loop=app.loop)
    else:
        app['pg'] = await aiopg.sa.create_engine(dsn=dsn, minsize=max_size, maxsize=max_size, loop=app.loop)


async def cleanup(app: web.Application):
    app['aiopg_engine'].close()
    await app['aiopg_engine'].wait_closed()
    await app['asyncpg_pool'].close()


def setup_routes(app):
    if RAW_CONNECTION:
        app.router.add_get('/db', single_database_query_raw)
        app.router.add_get('/queries/{queries}', multiple_database_queries_raw)
        app.router.add_get('/fortunes', fortunes_raw)
        app.router.add_get('/updates/{queries}', updates_raw)
    else:
        app.router.add_get('/json', json)
        app.router.add_get('/db', single_database_query_orm)
        app.router.add_get('/queries/{queries}', multiple_database_queries_orm)
        app.router.add_get('/fortunes', fortunes)
        app.router.add_get('/updates/{queries}', updates)
        app.router.add_get('/plaintext', plaintext)


def create_app(loop):
    app = web.Application(loop=loop)

    jinja2_loader = jinja2.FileSystemLoader(str(THIS_DIR / 'templates'))
    aiohttp_jinja2.setup(app, loader=jinja2_loader)

    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    setup_routes(app)
    return app
