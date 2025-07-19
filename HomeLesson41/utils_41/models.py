from pydantic import BaseModel, Field
from datetime import date


class UserBase(BaseModel):
    id_user: str = Field(..., max_length=20)
    fullname: str
    email: str
    phone: str


class ProductBase(BaseModel):
    product: str
    price_per_unit: float


class OrderBase(BaseModel):
    id_order: int
    id_user: str
    id_product: int
    quantity: int
    date_orders: date
