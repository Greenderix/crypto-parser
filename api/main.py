import json
from fastapi import FastAPI
from fastapi.openapi.models import Info
from pydantic import BaseModel
import aioredis
from models import CoursesModel, CourseModel

from config import redis_dsn


app = FastAPI(
    title="course-checker"
)

app.openapi = Info(
    title="domain-checker",
    version="1.0.0",
    description="Wеb-сервис для парсинга курса",
    terms_of_service="https://t.me/evgenysoloz",
    contact={
        "name": "Евгений Солозобов",
        "url": "https://t.me/evgenysoloz",
    },
)

redis_conn: aioredis.client.Redis | None = None

@app.on_event('startup')
async def startup_event():
    global redis_conn
    redis_conn = await aioredis.from_url(redis_dsn)


@app.on_event('shutdown')
async def shutdown_event():
    await redis_conn.close()


@app.get("/courses")
async def courses() -> CoursesModel:
    result = None
    while not result:
        result = await redis_conn.get('markets')
    markets = json.loads(result)
    courses_data = CoursesModel(
        exchanger='coingecko',
        courses=[
            CourseModel(
                direction=f'{market["name"]} - {market["currency"]}',
                value=market['current_price']
            ) for market in markets
        ]
    )
    return courses_data
