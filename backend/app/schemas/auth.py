from pydantic import BaseModel, ConfigDict, EmailStr, Field
from typing import Optional
from datetime import datetime

from app.schemas.enums import UserRole


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: Optional[str] = Field(default=None, max_length=150)
    role: UserRole = UserRole.developer
    team: Optional[str] = Field(default=None, max_length=120)


class UserProfileResponse(BaseModel):
    id: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: UserRole
    team: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
