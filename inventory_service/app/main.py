# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI,  Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaConsumer
from sqlmodel import SQLModel, Field, Session, create_engine, select
from typing import Optional, Annotated
import asyncio
import json
from app import settings
from pydantic import BaseModel, EmailStr



# async def consume_messages(topic, bootstrap_servers):
#     # Create a consumer instance.
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="my-todos-group",
#         auto_offset_reset='earliest'
#     )

    # Start the consumer.
    # await consumer.start()
    # try:
    #     # Continuously listen for messages.
    #     async for message in consumer:
    #         print(f"Received message: {message.value.decode()} on topic {message.topic}")
    #         # Here you can add code to process each message.
    #         # Example: parse the message, store it in a database, etc.
    # finally:
    #     # Ensure to close the consumer when done.
    #     await consumer.stop()


# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
# loop = asyncio.get_event_loop()

connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

# recycle connections after 5 minutes
# to correspond with the compute scale down
engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300, echo=True
)

def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")
    create_db_and_tables()
    # task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    yield

class Inventory(SQLModel, table=True):  
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id : int = Field(default=None)
    item_description: str
    category: str
    quantity : int
    stock_level: str
    
class InventoryUpdate(SQLModel):
    item_description: Optional[str] = None
    category: Optional[str] = None
    quantity: Optional[int] = None
    stock_level: Optional[str] = None


app = FastAPI(lifespan=lifespan, title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])

def get_session():
    with Session(engine) as session:
        yield session

@app.get("/")
def read_root():
    return {"Inventory-Service"}


@app.post("/add_inventory", response_model=Inventory)
def create_inventory(createInventory: Inventory, session: Session = Depends(get_session)):
    new_inventory = Inventory(
    product_id=createInventory.product_id,
    stock_level=createInventory.stock_level,
    quantity=createInventory.quantity,
    category=createInventory.category,
    item_description=createInventory.item_description
    
    )
    session.add(new_inventory)
    session.commit()
    session.refresh(new_inventory)
    return new_inventory


@app.delete("/delete_inventory", response_model=Inventory)
def delete_inventory(product_id: int, session: Annotated[Session, Depends(get_session)]):
        statement = select(Inventory).where(Inventory.product_id == product_id)
        result = session.exec(statement)
        product = result.first()
        if product is None:
            raise HTTPException(status_code=404, detail="Inventory not found")
        print("Inventory Deleted")
        session.delete(product)
        session.commit()
        return product
    
    
    
@app.get("/list_inventory", response_model=list[Inventory])
def list_inventory(session: Annotated[Session, Depends(get_session)]):
        users = session.exec(select(Inventory)).all()
        return users
    

@app.put("/update_inventory", response_model=Inventory)
def update_inventory(product_id: int, inventory_update: InventoryUpdate, session: Annotated[Session, Depends(get_session)]):
    statement = select(Inventory).where(Inventory.product_id == product_id)
    result = session.exec(statement)
    product = result.first()
    if product is None:
        raise HTTPException(status_code=404, detail="Inventory not found")
    
    for key, value in inventory_update.dict(exclude_unset=True).items():
        setattr(product, key, value)

    session.add(product)
    session.commit()
    session.refresh(product)
    return product
        
        







   
