from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime
from app.schemas.enums import DeploymentEventType, DeploymentStatus

class DeploymentEventBase(BaseModel):
    event_type: DeploymentEventType
    event_status: Optional[DeploymentStatus] = None
    source: str
    message: str
    details: Optional[Dict[str, Any]] = None

class DeploymentEventCreate(DeploymentEventBase):
    pass

class DeploymentEventResponse(DeploymentEventBase):
    id: int
    deployment_id: str
    created_at: datetime
    actor_user_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
