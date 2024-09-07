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



class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id : int
    product_name: str = Field(index=True)
    product_description: str
    product_category: str
    product_price: int
    product_quantity: int
 
    
class ProductUpdate(SQLModel):
    product_name: Optional[str] 
    product_description: Optional[str] 
    product_category: Optional[str] 
    product_price: Optional[int] 
    product_quantity: Optional[int]



connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

# recycle connections after 5 minutes
# to correspond with the compute scale down

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
        group_id="my-group",
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

app = FastAPI(lifespan=lifespan, title="Product Service", 
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


@app.get("/")
def read_root():
    return {"Products-Service"}


@app.post("/add_product/", response_model=Product)
def create_product(product: Product, session: Annotated[Session, Depends(get_session)]):
    new_product = Product(
        product_id=product.product_id,
        product_name=product.product_name,
        product_description=product.product_description,
        product_price=product.product_price,
        product_quantity=product.product_quantity,
        product_category=product.product_category
    )
    session.add(new_product)
    session.commit()
    session.refresh(new_product)
    return new_product


@app.get("/get_product", response_model=List[Product])
def get_product(productcategory: Optional[str] = None, session: Session = Depends(get_session)):
    if productcategory:
        statement = select(Product).where(Product.product_category == productcategory)
    else:
        statement = select(Product)
    
    result = session.exec(statement)
    products = result.all()

    if not products:
        if productcategory:
            raise HTTPException(status_code=404, detail=f"{productcategory} Category not found")
        else:
            raise HTTPException(status_code=404, detail="No products found")
    return products
    
    
@app.delete("/delete_product", response_model=Product)
def delete_product(product_id: int,session: Annotated[Session, Depends(get_session)]):
        statement = select(Product).where(Product.product_id == product_id) 
        result = session.exec(statement)
        product = result.first()
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        print("Product Deleted")
        session.delete(product)
        session.commit()
        return product
        
        
@app.put("/update_Product", response_model=Product)
def update_product(product_id: int, product_update: ProductUpdate, session: Annotated[Session, Depends(get_session)]):
    statement = select(Product).where(Product.product_id == product_id)
    result = session.exec(statement)
    product = result.first()
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    
    print("Product before update:", product)
    
    # Fetch the data to be updated
    update_data = product_update.model_dump(exclude_unset=True)
    print("Update data:", update_data)
    
    # List of invalid update values
    invalid_values = [None, "", "string"]

    for key, value in update_data.items():
        if isinstance(value, str) and value.strip() in invalid_values:
            continue  # Skip invalid string values
        if isinstance(value, int) and value == 0:
            continue  # Skip invalid integer values (0)
        
        print(f"Updating {key} to {value}")  # Debug print
        setattr(product, key, value)
        
        
    session.add(product)
    session.commit()
    session.refresh(product)
    
    # Print the product after updating for debugging
    print("Product after update:", product)

    return product
# Kafka Producer as a dependency
# async def get_kafka_producer():
#     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#     await producer.start()
#     try:
#         yield producer
#     finally:
#         await producer.stop()

# @app.post("/add_user/", response_model=UserBase)
# async def create_todo(todo: UserBase, session: Annotated[Session, Depends(get_session)])->UserBase:
#         # todo_dict = {field: getattr(todo, field) for field in todo.dict()}
#         # todo_json = json.dumps(todo_dict).encode("utf-8")
#         # print("todoJSON:", todo_json)
#         # Produce message
#         # await producer.send_and_wait("todos", todo_json)
#         # session.add(todo)
#         # session.commit()
#         # session.refresh(todo)
#         return todo


# @app.get("/todos/", response_model=list[Todo])
# def read_todos(session: Annotated[Session, Depends(get_session)]):
#         todos = session.exec(select(Todo)).all()
#         return todos
