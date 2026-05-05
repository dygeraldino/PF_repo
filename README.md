# PaaS MVP (Kubernetes + CI/CD)

Prototipo minimo viable para el proyecto "Diseno, Implementacion y Evaluacion de una Plataforma PaaS de Autoservicio para Despliegues Controlados sobre Kubernetes".

## Componentes

- Frontend: React (Vite)
- API: FastAPI
- Mensajeria: RabbitMQ
- Persistencia: PostgreSQL
- Worker: consumidor RabbitMQ que ejecuta kubectl
- Observabilidad: Prometheus + Grafana
- Kubernetes local: kind

## Arranque rapido (MVP)

1. Crear cluster local
   - `kind create cluster --name paas-mvp --config infra/k8s/kind-config.yaml`
2. Aplicar deployment de prueba
   - `kubectl apply -f infra/k8s/sample-app/deployment.yaml`
   - `kubectl apply -f infra/k8s/sample-app/service.yaml`
3. Levantar stack local
   - `docker compose -f infra/docker/docker-compose.yml up --build`
4. UI
   - `http://localhost:5173` (si ejecutas frontend en dev)
   - API en `http://localhost:8000`
5. Observabilidad
   - Prometheus `http://localhost:9090`
   - Grafana `http://localhost:3000` (admin/admin)

## Endpoints API

- `POST /deployments`
- `GET /deployments/{id}`
- `GET /deployments`
- `GET /health`
- `GET /metrics`

## Cola y ejecucion

- `deploy.staging` con prefetch > 1
- `deploy.production` serial (prefetch = 1)
- DLQ: `deploy.dlq` para mensajes rechazados

## Notas

- El worker asume que `service_name` coincide con el `Deployment` y el container en Kubernetes.
- Para el ejemplo usa `service_name=sample-app` e `image=nginx:1.25`.
- En produccion se aplica una politica simple por allowlist via `POLICY_ALLOWLIST`.

# PF_repo
