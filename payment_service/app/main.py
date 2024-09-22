# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from typing import AsyncGenerator
from aiokafka import AIOKafkaConsumer
from app.routes.payment_routes import payment_router

from fastapi import FastAPI
from app.db import create_db_and_tables
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(
    title="User and Admin Service",
    description="API for managing users",
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Users", "description": "Operations with users."}
    ]
)

@app.get("/")
def home():
    return "Welcome to Payment service"

app.include_router(payment_router, prefix="/api/payment")