from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List
from datetime import datetime
from app.schemas.enums import DeploymentEnvironment, DeploymentPolicy, DeploymentStatus
from app.schemas.event import DeploymentEventResponse

class DeploymentBase(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=120)
    image: str = Field(..., min_length=1, max_length=255)
    environment: DeploymentEnvironment
    policy: DeploymentPolicy = DeploymentPolicy.replace
    k8s_namespace: Optional[str] = None
    k8s_resource_name: Optional[str] = None
    health_path: Optional[str] = Field(default='/health', description="Path for liveness probe, e.g. / or /health")
    container_port: Optional[int] = Field(default=8000, description="Port the container listens on")
    env_vars: Optional[dict] = None

class DeploymentCreate(DeploymentBase):
    pass

class DeploymentStatusUpdate(BaseModel):
    status: DeploymentStatus
    success: Optional[bool] = None
    error_message: Optional[str] = None
    rollback_required: Optional[bool] = None
    rollback_performed: Optional[bool] = None
    notes: Optional[str] = None

class DeploymentResponse(BaseModel):
    id: str
    service_name: str
    image: str
    environment: DeploymentEnvironment
    policy: DeploymentPolicy
    status: DeploymentStatus

    requested_by_user_id: Optional[str] = None
    requested_by_name: Optional[str] = None
    requested_at: datetime
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    queue_name: Optional[str] = None
    message_id: Optional[str] = None
    worker_id: Optional[str] = None
    retry_count: int

    k8s_namespace: Optional[str] = None
    k8s_resource_name: Optional[str] = None
    rollout_revision: Optional[int] = None
    rollback_required: bool
    rollback_performed: bool
    success: Optional[bool] = None
    error_message: Optional[str] = None
    notes: Optional[str] = None

    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class DeploymentWithEventsResponse(DeploymentResponse):
    events: List[DeploymentEventResponse] = []
