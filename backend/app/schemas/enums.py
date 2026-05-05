import enum

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
