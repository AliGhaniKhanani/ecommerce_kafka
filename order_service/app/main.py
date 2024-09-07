# main.py
from contextlib import asynccontextmanager
from app import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select, Sequence
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator, List, Union, Optional, Annotated
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import uuid
from datetime import datetime
from pydantic import constr
import re, random
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer


class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(index=True)
    user_id: int
    email: str
    product_name: str
    product_id: int #= Field(foreign_key="product.product_id")
    quantity: int
    total_price: int
    status: str 
    city: str
    phone: str
    created_at: datetime 
    

    
class OrderUpdate(SQLModel):
    email: Optional[str] = None
    product_id: Optional[int] = None
    product_name: Optional[str] = None
    quantity: Optional[int] = None
    total_price: Optional[int] = None
    status: Optional[str] = None
    city: Optional[str] = None
    phone: Optional[str] = None
    
    
class MyOrder(SQLModel):
    order_id: int = Field(index=True)
    user_id: int
    email: str
    product_id: int
    quantity: int
    total_price: int
    status: str 
    city: str
    phone: str
    created_at: datetime 
    
    
connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)

def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)

async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-todos-group",
        auto_offset_reset='earliest'
    )


    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print(f"Received message: {message.value.decode()} on topic {message.topic}")
            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
        

@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")
    # loop.run_until_complete(consume_messages('todos', 'broker:19092'))
    # task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan, title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])


def get_session():
    with Session(engine) as session:
        yield session
        
        
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://127.0.0.1:8000/Login")  # URL of the authentication service
     



@app.get("/")
def read_root():
    return {"Order Service"}


def generate_order_number() -> int:
    return random.randint(10000000, 99999999)


def phone_number_validation(phone: str):
    if not re.match(r'^\d{10,15}$', phone):
        raise HTTPException(status_code=422, detail="Phone number must be between 10 and 15 digits long and contain only numbers")
    return phone


@app.post("/create_order", response_model=Order)
def create_order(token: Annotated[str, Depends(oauth2_scheme)], CreateOrder: Order, session: Session = Depends(get_session)):
    new_order = Order(
        order_id=generate_order_number(),
        user_id=CreateOrder.user_id,
        email=CreateOrder.email,
        product_id=CreateOrder.product_id,
        product_name=CreateOrder.product_name,
        quantity=CreateOrder.quantity,
        total_price=CreateOrder.total_price,
        status=CreateOrder.status,
        phone=phone_number_validation(CreateOrder.phone),
        city=CreateOrder.city,
        created_at = datetime.now()
    )
    session.add(new_order)
    session.commit()
    session.refresh(new_order)
    return new_order



@app.put("/update_order/{order_id}", response_model=Order)
def update_order(order_id: int, order_update: OrderUpdate, session: Session = Depends(get_session)):
    statement = select(Order).where(Order.order_id == order_id)
    result = session.exec(statement)
    order = result.first()
    
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    update_data = order_update.model_dump(exclude_unset=True)
    
    if 'phone' in update_data and not re.match(r'^\d{10,15}$', update_data['phone']):
        raise HTTPException(status_code=422, detail="Phone number must be between 10 and 15 digits long and contain only numbers")

    for key, value in update_data.items():
        setattr(order, key, value)
    
    session.add(order)
    session.commit()
    session.refresh(order)
    return order



@app.get("/list_Order", response_model=list[Order])
def list_order(session: Annotated[Session, Depends(get_session)]):
        users = session.exec(select(Order)).all()
        return users
    


@app.delete("/delete_order/{order_id}", response_model=Order)
def delete_order(order_id: int, session: Session = Depends(get_session)):
    statement = select(Order).where(Order.order_id == order_id)
    result = session.exec(statement)
    order = result.first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    print("Order Deleted")
    session.delete(order)
    session.commit()
    return order
    
    
# Kafka Producer as a dependency
    # async def get_kafka_producer():
    # producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    # await producer.start()
    # try:
    #     yield producer
    # finally:
    #     await producer.stop()