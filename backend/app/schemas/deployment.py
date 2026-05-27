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
    is_compose: Optional[bool] = Field(default=False, description="Whether this is a docker-compose deployment")

class DeploymentCreate(DeploymentBase):
    previous_deployment_id: Optional[str] = Field(
        default=None,
        description="ID del deployment anterior (si aplica)"
    )

class RepoDeployRequest(BaseModel):
    repo_url: str = Field(..., description="URL of the Git repository to clone")
    docker_context_path: str = Field(default=".", description="Path inside the repo where the Dockerfile is located")
    service_name: str = Field(..., description="Name of the service (used for Kubernetes deployment/ingress)")
    cluster_name: str = Field(default="paas-demo")
    image_name: str = Field(..., description="Name for the built Docker image")
    image_version: str = Field(default="latest")
    env_file_content: Optional[str] = Field(default=None, description="Content of .env file needed for build")
    is_compose: bool = Field(default=False, description="Whether to use docker-compose instead of Dockerfile")

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
    previous_deployment_id: Optional[str] = None
    is_compose: bool = False

    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class DeploymentWithEventsResponse(DeploymentResponse):
    events: List[DeploymentEventResponse] = []
