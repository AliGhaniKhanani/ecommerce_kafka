from fastapi import APIRouter, HTTPException, Request
from app.controller.payment_controller import add_payment
from app.models import PaymentForm
from fastapi.responses import RedirectResponse
import stripe

payment_router = APIRouter()

# checkout route using stripe
@payment_router.get("/checkout")
async def checkout( payment_form: PaymentForm ):
    stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{
            'price_data': {
                "currency": payment_form.currency,
                "product_data": {
                    "name": payment_form.product_name,
                }
            },
            'quantity': payment_form.quantity,
        }],
        success_url='http://localhost:8000/success',
        cancel_url='http://localhost:8000/cancel',
    )
    return {"message": "Payment checkout"}

@payment_router.post("/webhook")
async def stripe_webhook(request: Request):
    payload = await request.json()
    try:
        event = stripe.Event.construct_from(payload, stripe.api_key)
        print("Event: ", event)
    except ValueError as ve:
        print(f"Value error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except stripe.SignatureVerificationError as e:
        print(f"Signature verification error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        print(session)
        add_payment(session)
    elif event["type"] == "checkout.session.cancelled":
        session = event["data"]["object"]
        print(session)
    return {"status": "success"}
     