from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class EcommerceEvent(BaseModel):
    id: str
    product_id: str
    amount: float
    created_at: datetime
    status: Optional[str] = "NEW"
