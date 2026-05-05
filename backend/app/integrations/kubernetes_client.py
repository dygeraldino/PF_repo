import asyncio
import random
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)


class KubernetesClient:
    """
    Capa de integración con Kubernetes.

    En modo simulación (SIMULATE_K8S=True):
        Todos los métodos simulan operaciones con delays realistas.
        Ideal para demo universitaria sin necesitar un clúster real.

    Para conectar con Kubernetes real:
        1. Establecer SIMULATE_K8S=False en .env
        2. Instalar: pip install kubernetes
        3. Configurar KUBECONFIG o usar in-cluster config
        4. Los comentarios "# TODO: REAL K8S" marcan exactamente dónde agregar las llamadas reales
    """

    def __init__(self, simulate: bool = True):
        self.simulate = simulate

        if not simulate:
            # TODO: REAL K8S — Inicializar cliente de Kubernetes
            # from kubernetes import client, config
            # config.load_kube_config()        # usa ~/.kube/config local
            # # config.load_incluster_config() # usa token del pod en k8s
            # self.apps_v1 = client.AppsV1Api()
            # self.core_v1 = client.CoreV1Api()
            logger.warning("Kubernetes real activado pero cliente no inicializado. Ver kubernetes_client.py")

    async def apply_deployment(
        self, namespace: str, resource_name: str, image: str, policy: str
    ) -> dict:
        """
        Aplica un Deployment en Kubernetes.
        Retorna: {success, revision, message}
        """
        if self.simulate:
            logger.info(f"[K8S SIMULATED] apply {resource_name} ({image}) in {namespace}")
            await asyncio.sleep(2)  # simula latencia de kubectl apply
            return {
                "success": True,
                "revision": random.randint(1, 20),
                "message": f"deployment.apps/{resource_name} configured",
            }

        # TODO: REAL K8S — Aplicar manifiesto via cliente Python
        # manifest = self._build_deployment_manifest(namespace, resource_name, image, policy)
        # try:
        #     resp = self.apps_v1.patch_namespaced_deployment(
        #         name=resource_name, namespace=namespace, body=manifest
        #     )
        #     return {"success": True, "revision": resp.metadata.resource_version, "message": "applied"}
        # except Exception as e:
        #     return {"success": False, "revision": None, "message": str(e)}

    async def check_rollout_status(self, namespace: str, resource_name: str) -> dict:
        """
        Verifica si el rollout completó correctamente (todas las réplicas listas).
        Retorna: {ready, available_replicas, message}
        """
        if self.simulate:
            logger.info(f"[K8S SIMULATED] rollout status {resource_name} in {namespace}")
            await asyncio.sleep(1.5)
            # 85% de éxito para que la demo muestre ambos caminos ocasionalmente
            success = random.random() > 0.15
            if success:
                replicas = random.randint(1, 3)
                return {
                    "ready": True,
                    "available_replicas": replicas,
                    "message": f"deployment \"{resource_name}\" successfully rolled out",
                }
            else:
                return {
                    "ready": False,
                    "available_replicas": 0,
                    "message": f"Waiting for deployment \"{resource_name}\" rollout: 0 of 1 updated replicas are available",
                }

        # TODO: REAL K8S — Leer estado del deployment
        # dep = self.apps_v1.read_namespaced_deployment(name=resource_name, namespace=namespace)
        # desired = dep.spec.replicas or 1
        # ready = dep.status.ready_replicas or 0
        # return {
        #     "ready": ready >= desired,
        #     "available_replicas": ready,
        #     "message": "" if ready >= desired else f"{ready}/{desired} replicas ready",
        # }

    async def rollback_deployment(self, namespace: str, resource_name: str) -> dict:
        """
        Hace rollback al revision anterior del deployment.
        Retorna: {success, message}
        """
        if self.simulate:
            logger.info(f"[K8S SIMULATED] rollback {resource_name} in {namespace}")
            await asyncio.sleep(1)
            return {
                "success": True,
                "message": f"deployment.apps/{resource_name} rolled back",
            }

        # TODO: REAL K8S — kubectl rollout undo via Python client
        # import subprocess
        # result = subprocess.run(
        #     ["kubectl", "rollout", "undo", f"deployment/{resource_name}", "-n", namespace],
        #     capture_output=True, text=True
        # )
        # return {"success": result.returncode == 0, "message": result.stdout or result.stderr}
