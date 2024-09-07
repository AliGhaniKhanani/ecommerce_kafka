# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaConsumer
import asyncio
import json
from sqlmodel import SQLModel, Field, Session, create_engine
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-todos-group",
        auto_offset_reset='earliest'
    )

    # # Start the consumer.
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
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    # print("Creating tables..")
    # task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    yield


app = FastAPI(lifespan=lifespan, title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])


SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_ADDRESS = 'alikhanani18@gmail.com'
EMAIL_PASSWORD = 'ydbx qouz mojo rggi'


@app.get("/")
def read_root():
    return {"Notification App"}


def send_email(to_address: str, subject: str, html_message: str, text_message: str):
    # Create the email container with both plain text and HTML parts
    msg = MIMEMultipart("alternative")
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = to_address
    msg['Subject'] = subject

    # Attach the plain text message
    msg.attach(MIMEText(text_message, 'plain'))

    # Attach the HTML message
    msg.attach(MIMEText(html_message, 'html'))

    # Connect to the SMTP server and send the email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()  # Start TLS encryption
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)  # Log in to the SMTP server
        server.sendmail(EMAIL_ADDRESS, to_address, msg.as_string())  # Send the email

# Define the request model
class EmailRequest(SQLModel):
    to: str
    subject: str
    body: str


@app.post("/send-email/")
async def send_email_endpoint(email_request: EmailRequest):
    try:
        # Define the plain text message
        text_message = email_request.body
        
        # Create an HTML message (you can customize this as needed)
        html_message = f"""
        <html>
          <body>
            <h1>{email_request.subject}</h1>
            <p>{email_request.body}</p>
          </body>
        </html>
        """

        # Send the email
        send_email(email_request.to, email_request.subject, html_message, text_message)
        return {"message": "Email sent successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Kafka Producer as a dependency
# async def get_kafka_producer():
#     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#     await producer.start()
#     try:
#         yield producer
#     finally:
#         await producer.stop()