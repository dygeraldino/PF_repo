import asyncio
import random
import logging
import os
import yaml
from typing import Optional
from jinja2 import Environment, FileSystemLoader

from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException

from app.core.config import settings

logger = logging.getLogger(__name__)

class KubernetesClient:
    """
    Orquestador de Kubernetes para la PaaS (Camino B: Plantillas Estándar).
    """

    def __init__(self, simulate: bool = True):
        self.simulate = simulate
        self.templates_path = os.path.join(os.path.dirname(__file__), "k8s_templates")
        self.jinja_env = Environment(loader=FileSystemLoader(self.templates_path))

        if not simulate:
            try:
                # Intenta cargar config interna (si el backend corre en K8s) 
                # o externa (vía ~/.kube/config mapeado)
                try:
                    config.load_incluster_config()
                    logger.info("✔ K8s: Usando configuración In-Cluster")
                except config.ConfigException:
                    config.load_kube_config()
                    logger.info("✔ K8s: Usando configuración KubeConfig local")
                
                # Overwrite server URL if specified (useful for Docker-to-Kind)
                server_override = os.getenv("K8S_SERVER_OVERRIDE")
                if server_override:
                    conf = client.Configuration.get_default_copy()
                    conf.host = server_override
                    conf.verify_ssl = False # Kind certs use internal IPs, skip verify to avoid SAN errors
                    client.Configuration.set_default(conf)
                    logger.info(f"⚠ K8s: Server URL sobreescrita a {server_override} (SSL Verify=False)")

                self.apps_v1 = client.AppsV1Api()
                self.core_v1 = client.CoreV1Api()
                self.networking_v1 = client.NetworkingV1Api()
                self.api_client = client.ApiClient()
            except Exception as e:
                logger.error(f"❌ Error al inicializar cliente real de K8s: {e}")
                self.simulate = True # Fallback a simulación si falla la conexión

    async def apply_deployment(
        self, namespace: str, resource_name: str, image: str, policy: str, service_name: str,
        health_path: str = "/health", port: int = 8000, env_vars: dict = None
    ) -> dict:
        """
        Genera y aplica los recursos (Deployment, Service, Ingress) usando plantillas.
        """
        if self.simulate:
            logger.info(f"[K8S SIMULATED] apply {resource_name} ({image})")
            await asyncio.sleep(2)
            return {"success": True, "revision": random.randint(1, 10), "message": "Simulated apply OK"}

        try:
            # 1. Asegurar que el namespace existe
            await self._ensure_namespace(namespace)

            # 2. Renderizar y aplicar cada componente
            template_vars = {
                "resource_name": resource_name,
                "service_name": service_name,
                "namespace": namespace,
                "image": image,
                "policy": policy,
                "port": port,
                "health_path": health_path,
                "env_vars": env_vars,
            }

            # Aplicar Deployment
            dep_yaml = self.jinja_env.get_template("deployment.yaml").render(template_vars)
            await self._apply_yaml(dep_yaml, namespace)

            # Aplicar Service
            svc_yaml = self.jinja_env.get_template("service.yaml").render(template_vars)
            await self._apply_yaml(svc_yaml, namespace)

            # Aplicar Ingress
            ing_yaml = self.jinja_env.get_template("ingress.yaml").render(template_vars)
            await self._apply_yaml(ing_yaml, namespace)

            return {
                "success": True,
                "revision": 1, # TODO: Obtener revisión real de la API
                "message": f"Servicio {service_name} desplegado en {namespace}",
            }

        except Exception as e:
            logger.error(f"❌ Error aplicando despliegue real: {e}")
            return {"success": False, "revision": None, "message": str(e)}

    async def check_rollout_status(self, namespace: str, resource_name: str) -> dict:
        """Verifica si los pods están listos en el clúster real."""
        if self.simulate:
            await asyncio.sleep(1)
            return {"ready": True, "available_replicas": 1, "message": "Simulated ready"}

        try:
            # 1. Obtener el deployment para saber cuántas réplicas queremos e imagen objetivo
            dep = self.apps_v1.read_namespaced_deployment(name=resource_name, namespace=namespace)
            target_image = dep.spec.template.spec.containers[0].image
            desired = dep.spec.replicas or 1

            # 2. Listar los pods actuales de este servicio
            # Usamos el label app={{ service_name }} que definimos en el template
            pods = self.core_v1.list_namespaced_pod(
                namespace=namespace, 
                label_selector=f"app={resource_name}"
            )

            # 3. Contar cuántos pods con la imagen NUEVA están realmente Ready
            ready_count = 0
            for pod in pods.items:
                # 3a. Ignorar pods que se están borrando
                if pod.metadata.deletion_timestamp:
                    continue

                # 3b. Verificar imagen (Comparación flexible para evitar temas de docker.io/ library/)
                pod_image = pod.spec.containers[0].image
                # Si el pod tiene la imagen vieja, no lo contamos para el éxito de la nueva versión
                if target_image not in pod_image and pod_image not in target_image:
                    continue 
                
                # 3c. Verificar si está Ready y Running
                phase_ok = pod.status.phase == "Running"
                containers_ready = all(c.ready for c in (pod.status.container_statuses or []))
                
                if phase_ok and containers_ready:
                    ready_count += 1

            # Éxito solo si los pods NUEVOS están listos
            ready = ready_count >= desired

            return {
                "ready": ready,
                "available_replicas": ready_count,
                "desired_replicas": desired,
                "message": "Rollout exitoso" if ready else f"Esperando pods con imagen {target_image} ({ready_count}/{desired} listos)",
            }
        except ApiException as e:
            return {"ready": False, "available_replicas": 0, "message": f"Error API: {e}"}


    # --- Helpers Internos ---

    async def _ensure_namespace(self, namespace: str):
        try:
            self.core_v1.read_namespace(name=namespace)
        except ApiException as e:
            if e.status == 404:
                self.core_v1.create_namespace(
                    body=client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
                )

    async def _apply_yaml(self, yaml_content: str, namespace: str):
        """Implementación básica de 'kubectl apply' vía Python client."""
        data = yaml.safe_load(yaml_content)
        kind = data["kind"]
        name = data["metadata"]["name"]

        try:
            if kind == "Deployment":
                try:
                    self.apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
                    self.apps_v1.patch_namespaced_deployment(name=name, namespace=namespace, body=data)
                except ApiException:
                    self.apps_v1.create_namespaced_deployment(namespace=namespace, body=data)
            
            elif kind == "Service":
                try:
                    self.core_v1.read_namespaced_service(name=name, namespace=namespace)
                    self.core_v1.patch_namespaced_service(name=name, namespace=namespace, body=data)
                except ApiException:
                    self.core_v1.create_namespaced_service(namespace=namespace, body=data)
            
            elif kind == "Ingress":
                try:
                    self.networking_v1.read_namespaced_ingress(name=name, namespace=namespace)
                    self.networking_v1.patch_namespaced_ingress(name=name, namespace=namespace, body=data)
                except ApiException:
                    self.networking_v1.create_namespaced_ingress(namespace=namespace, body=data)
                    
        except ApiException as e:
            logger.error(f"Error aplicando {kind}/{name}: {e}")
            raise
