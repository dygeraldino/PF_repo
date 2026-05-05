import enum
import uuid
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, Text, Enum
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base

class DeploymentEnvironment(str, enum.Enum):
    staging = 'staging'
    production = 'production'

class DeploymentPolicy(str, enum.Enum):
    replace = 'replace'
    canary = 'canary'

class DeploymentStatus(str, enum.Enum):
    PENDING = 'PENDING'
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    ROLLED_BACK = 'ROLLED_BACK'
    CANCELLED = 'CANCELLED'

class DeploymentEventType(str, enum.Enum):
    REQUEST_CREATED = 'REQUEST_CREATED'
    ENQUEUED = 'ENQUEUED'
    STARTED = 'STARTED'
    HEALTHCHECK_OK = 'HEALTHCHECK_OK'
    HEALTHCHECK_FAIL = 'HEALTHCHECK_FAIL'
    ROLLBACK_STARTED = 'ROLLBACK_STARTED'
    ROLLBACK_OK = 'ROLLBACK_OK'
    ROLLBACK_FAIL = 'ROLLBACK_FAIL'
    FINISHED = 'FINISHED'
    ERROR = 'ERROR'

class Deployment(Base):
    __tablename__ = "deployments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name = Column(String(120), nullable=False)
    image = Column(String(255), nullable=False)
    environment = Column(Enum(DeploymentEnvironment, name="deployment_environment"), nullable=False)
    policy = Column(Enum(DeploymentPolicy, name="deployment_policy"), nullable=False, default=DeploymentPolicy.replace)
    status = Column(Enum(DeploymentStatus, name="deployment_status"), nullable=False, default=DeploymentStatus.PENDING)

    requested_by_user_id = Column(UUID(as_uuid=True), ForeignKey("user_profiles.id", ondelete="SET NULL"))
    requested_by_name = Column(String(120))
    requested_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    queued_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))

    queue_name = Column(String(80))
    message_id = Column(UUID(as_uuid=True))
    worker_id = Column(String(120))
    retry_count = Column(Integer, nullable=False, default=0)

    k8s_namespace = Column(String(120))
    k8s_resource_name = Column(String(120))
    rollout_revision = Column(Integer)

    rollback_required = Column(Boolean, nullable=False, default=False)
    rollback_performed = Column(Boolean, nullable=False, default=False)
    success = Column(Boolean)
    error_message = Column(Text)
    notes = Column(Text)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    events = relationship("DeploymentEvent", back_populates="deployment", cascade="all, delete")

class DeploymentEvent(Base):
    __tablename__ = "deployment_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deployment_id = Column(UUID(as_uuid=True), ForeignKey("deployments.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(Enum(DeploymentEventType, name="deployment_event_type"), nullable=False)
    event_status = Column(Enum(DeploymentStatus, name="deployment_status"))
    source = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    details = Column(JSONB)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    actor_user_id = Column(UUID(as_uuid=True), ForeignKey("user_profiles.id", ondelete="SET NULL"))

    deployment = relationship("Deployment", back_populates="events")

class DeploymentMetric(Base):
    __tablename__ = "deployment_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deployment_id = Column(UUID(as_uuid=True), ForeignKey("deployments.id", ondelete="CASCADE"), nullable=False)
    metric_name = Column(String(80), nullable=False)
    metric_value = Column(Integer, nullable=False)
    unit = Column(String(30))
    recorded_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
