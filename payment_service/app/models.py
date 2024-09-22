from sqlmodel import SQLModel, Field
from typing import Optional 

class PaymentBase(SQLModel):
    order_id: int
    amount: float
    currency: str
    status: str
    type: str = Field(default="card")

class PaymentForm(PaymentBase):
    product_name: str
    quantity: int

class Payment(PaymentBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    