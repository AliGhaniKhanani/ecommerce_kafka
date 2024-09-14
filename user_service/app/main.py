# main.py
from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated
from app import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select, Sequence
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import uuid
from sqlmodel import SQLModel, Field
from pydantic import BaseModel, EmailStr
from typing import AsyncGenerator, List, Union, Optional, Annotated
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from datetime import timedelta, datetime
from jose import JWTError, jwt
import ssl
from .settings import SASL_PLAIN_PASSWORD
from app.core.crud import create_user, get_user, login, signup, get_users


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_name: Optional[str] = Field(default=None)
    email: str
    address: str
    phone_number: int
    country: str
    password: str


ALGORITHM = "HS256"
SECRET_KEY = "user access token"


# class Userupdate(SQLModel):
#     user_name: str | None = None
#     email: str | None = None
#     address: str | None = None
#     phone_number:int | None = None
#     country: str | None = None


connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_REQUIRED


async def consume_messages():
    consumer = AIOKafkaConsumer(
        'user_topic',
        bootstrap_servers="kafka-mart.servicebus.windows.net:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",

        sasl_plain_password=SASL_PLAIN_PASSWORD,
        group_id="user-group",
        ssl_context=context,
    )

    # Start the consumer.
    await consumer.start()
    try:
        async for message in consumer:
            print("Raw Form", message)
            print(f"Consumer Received message: {
                  message.value.decode('utf-8')} on topic {message.topic}")

            message = json.loads(message.value.decode("utf-8"))
            print("Message", message)
            print("Type", (type(message)))

            # # partition_name = message.get("partition_name")

            with next(get_session()) as session:
                print("Processing message in session context")

                if message["partition_name"] == "update_topic":
                    update_user_in_db(update_user=User(
                        **message["user_data"]), session=session)
                    print("User Info Updated")
                elif message["partition_name"] == "add_topic":
                    add_new_user_in_db(user_data=User(
                        **message["user_data"]), session=session)
                    print("New User Added")

    finally:
        await consumer.stop()
        print("Consumer stopped")


def update_user_in_db(update_user: User, session: Session):
    print("Update Function Called")
    session.add(update_user)
    session.commit()
    session.refresh(update_user)
    return update_user


def add_new_user_in_db(user_data: User, session: Session):
    print("Adding Function Called")
    session.add(user_data)
    session.commit()
    session.refresh(user_data)
    return user_data


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating tables..")
    # loop.run_until_complete(consume_messages('todos', 'broker:19092'))
    task = asyncio.create_task(consume_messages())
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan, title="User Service",
              version="0.0.1",
              servers=[
                  {
                      "url": "",  # ADD NGROK URL Here Before Creating GPT Action
                      "description": "Development Server"
                  }
              ])

# Kafka Producer as a dependency


async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


def get_session():
    with Session(engine) as session:
        yield session


@app.get("/")
def read_root():
    return {"User-Service"}


@app.post("/add_user", response_model=User)
async def create_user(user: User, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]) -> User:
    if user:
        return user


def create_access_token(subject: str, expire_time: timedelta) -> str:
    expire = datetime.utcnow() + expire_time
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Authentication and Generating Token


@app.post("/api/login")
async def login_request(tokens: Annotated[dict, Depends(login)]):
    if tokens:
        return tokens

@app.post("/api/signup")
async def signup_request(tokens: Annotated[dict, Depends(signup)]):
    if tokens:
        return tokens


# Authorising

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="Login")


@app.get("/list_users", response_model=list[User])
def list_users(token: Annotated[str, Depends(oauth2_scheme)], session: Annotated[Session, Depends(get_session)]):
    users = session.exec(select(User)).all()
    return users


@app.put("/update_user")
async def update_user(user_update: User, user_id: int,  session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):

    statement = select(User).where(User.id == user_id)
    result = session.exec(statement)
    user = result.one_or_none()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    print("User before update:", user)

    # Fetch the data to be updated
    update_data = user_update.model_dump(exclude_unset=True)
    print("Update data:", update_data)

    for key, value in update_data.items():
        #     # if isinstance(value, str) and value.strip() in invalid_values:
        #     #     continue  # Skip invalid string values
        #     # if isinstance(value, int) and value == 0:
        #     #     continue  # Skip invalid integer values (0)

        print(f"Updating {key} to {value}")  # Debug print
        setattr(user, key, value)

    message = {
        "partition_name": "update_topic",
        "user_data": user.model_dump()
    }
    await producer.send_and_wait(topic="user_topic", value=json.dumps(message).encode('utf-8'))
    print("Producing Update Topic:", message)
    return user


# @app.delete("/delete_user", response_model=UserResxponse)
# def delete_user(user_id: int,session: Annotated[Session, Depends(get_session)]):
#         user = session.get(User, user_id)
#         if user is None:
#             raise HTTPException(status_code=404, detail="User not found")
#         print("User Deleted")
#         session.delete(user)
#         session.commit()
#         return user
