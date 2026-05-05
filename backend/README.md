# 🚀 PaaS Deployments Backend

Backend de la **Plataforma de Autoservicio para Despliegues Controlados sobre Kubernetes**.

## Arquitectura

```
Frontend (React+Vite)
        │
        ▼ REST
┌─────────────────┐     AMQP      ┌───────────────┐
│   FastAPI API   │──────────────▶│   RabbitMQ    │
│  (Prisma ORM)   │               │  (2 colas +   │
│    Supabase     │               │    DLQ)       │
└─────────────────┘               └───────┬───────┘
                                          │ consume
                                  ┌───────▼───────┐
                                  │    Worker     │
                                  │ (Kubernetes   │
                                  │  simulado)    │
                                  └───────────────┘
```

### Flujo de estados
```
POST /deployments → PENDING → [RabbitMQ] → QUEUED
                                                │
                                           Worker consume
                                                │
                                            RUNNING
                                           /        \
                                     SUCCESS    FAILED / ROLLED_BACK
```

---

## Estructura del proyecto

```
backend/
├── app/
│   ├── api/
│   │   ├── dependencies.py      # Extracción de user_id del header
│   │   └── routers/
│   │       ├── deployments.py   # Todos los endpoints de deployments
│   │       └── health.py        # GET /health (con DB check)
│   ├── core/
│   │   ├── config.py            # Settings desde .env
│   │   └── database.py          # Cliente Prisma singleton
│   ├── integrations/
│   │   ├── rabbitmq.py          # Productor + gestión de colas
│   │   └── kubernetes_client.py # Simulador K8s (reemplazable)
│   ├── schemas/
│   │   ├── deployment.py        # Pydantic models de entrada/salida
│   │   ├── event.py
│   │   └── enums.py
│   ├── services/
│   │   └── deployment_service.py # Lógica de negocio
│   ├── workers/
│   │   └── deployment_worker.py  # Consumidor RabbitMQ
│   └── main.py                   # App FastAPI + lifespan
├── schema.prisma                 # Schema de base de datos
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
└── .env.example
```

---

## Endpoints

| Método | URL | Descripción |
|--------|-----|-------------|
| GET | `/health` | Estado de API + BD |
| POST | `/deployments` | Crear deployment (→ RabbitMQ) |
| GET | `/deployments` | Listar (filtros: status, environment, service_name) |
| GET | `/deployments/{id}` | Detalle de un deployment |
| PATCH | `/deployments/{id}/status` | Actualizar estado manualmente |
| POST | `/deployments/{id}/events` | Registrar evento manual |
| GET | `/deployments/{id}/events` | Timeline de eventos |
| POST | `/deployments/{id}/promote` | Promover staging → producción |
| POST | `/deployments/{id}/rollback` | Rollback manual |

---

## Cómo correr localmente

### Requisitos
- Docker Desktop
- Cuenta en Supabase (base de datos ya creada con el script SQL)

### 1. Configurar variables de entorno
```bash
cp .env.example .env
# Editar .env con tu DATABASE_URL de Supabase
```

### 2. Levantar todos los servicios
```bash
docker-compose up --build
```

Esto levanta:
- **API** → http://localhost:8000 | Swagger: http://localhost:8000/docs
- **RabbitMQ** → http://localhost:15672 (user: guest / pass: guest)
- **Worker** → proceso en background consumiendo colas

### 3. Verificar que todo funciona
```bash
curl http://localhost:8000/health
# Esperado: {"status": "ok", "database": "connected"}
```

---

## Payloads de ejemplo

### POST /deployments
```json
{
  "service_name": "auth-service",
  "image": "registry.gitlab.com/mi-empresa/auth-service:v1.2.0",
  "environment": "staging",
  "policy": "replace",
  "k8s_namespace": "staging-ns",
  "k8s_resource_name": "deploy-auth"
}
```

### POST /deployments/{id}/rollback
```json
{
  "reason": "Aumento de errores 5xx detectado en monitoreo"
}
```

### PATCH /deployments/{id}/status
```json
{
  "status": "CANCELLED",
  "notes": "Cancelado por el operador antes de ejecutarse"
}
```

---

## Mensaje RabbitMQ (ejemplo)

Cola destino: `deployments.staging` o `deployments.production`

```json
{
  "deployment_id": "550e8400-e29b-41d4-a716-446655440000",
  "service_name": "auth-service",
  "image": "registry.gitlab.com/mi-empresa/auth-service:v1.2.0",
  "environment": "staging",
  "policy": "replace",
  "requested_by_user_id": "user-uuid-here",
  "k8s_namespace": "staging-ns",
  "k8s_resource_name": "deploy-auth",
  "requested_at": "2026-05-05T21:00:00+00:00"
}
```

---

## Conectar Kubernetes real

1. Cambiar en `.env`:
   ```
   SIMULATE_K8S=False
   ```
2. Instalar cliente: `pip install kubernetes`
3. Seguir los comentarios marcados con `# TODO: REAL K8S` en `app/integrations/kubernetes_client.py`
4. Configurar `KUBECONFIG` o usar in-cluster config si corres dentro del clúster

---

## Colas RabbitMQ

| Cola | Propósito |
|------|-----------|
| `deployments.staging` | Deployments de entorno staging |
| `deployments.production` | Deployments de entorno production |
| `deployments.dlq` | Dead Letter Queue — mensajes que fallaron 3+ veces |
