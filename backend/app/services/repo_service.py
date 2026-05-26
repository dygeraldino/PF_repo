import os
import subprocess
import shutil
import logging
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)


def _detect_frontend_service(c_data: dict) -> tuple[str, int]:
    """
    Detecta el servicio 'frontend' dentro del docker-compose.
    Retorna (nombre_servicio, puerto_expuesto).
    Prioriza servicios cuyo nombre contiene 'front', 'web', 'ui' o 'nginx'.
    Si no encuentra ninguno, usa el primer servicio que tenga ports definidos.
    """
    FRONTEND_KEYWORDS = ("front", "web", "ui", "nginx", "app")
    services = c_data.get("services", {})
    candidates = []
    for name, svc in services.items():
        if not isinstance(svc, dict):
            continue
        ports_raw = svc.get("ports", [])
        for port_entry in ports_raw:
            # Puede ser "8080:80" o {target: 80, published: 8080}
            if isinstance(port_entry, str):
                parts = port_entry.split(":")
                # El último segmento es el puerto del contenedor (target)
                container_port = int(parts[-1].split("/")[0])
                host_port = int(parts[0]) if len(parts) > 1 else container_port
            elif isinstance(port_entry, dict):
                container_port = int(port_entry.get("target", 80))
                host_port = int(port_entry.get("published", container_port))
            else:
                continue
            # Kompose mapea el host_port como Service.port en K8s
            candidates.append((name, host_port, container_port))
            break  # solo el primer puerto de cada servicio

    # Buscar por keyword
    for keyword in FRONTEND_KEYWORDS:
        for name, host_port, container_port in candidates:
            if keyword in name.lower():
                return name, host_port

    # Fallback: servicio con puerto más bajo (más probable que sea el frontend)
    if candidates:
        candidates.sort(key=lambda x: x[1])
        return candidates[0][0], candidates[0][1]

    return None, 80

async def process_repo_deployment(
    deployment_id: str,
    repo_url: str,
    docker_context_path: str,
    service_name: str,
    cluster_name: str,
    image_name: str,
    image_version: str,
    env_file_content: str = None,
    is_compose: bool = False,
):
    """
    Clones a repository, creates a kind cluster, builds the image, and loads it.
    Replicates the functionality of deploy.sh.
    """
    from app.core.database import prisma_client
    from app.services.deployment_service import log_event, enqueue_deployment
    from app.schemas.event import DeploymentEventCreate
    from prisma.enums import DeploymentStatus as PrismaDeploymentStatus

    tmp_dir = f"/tmp/paas_repo_{cluster_name}_{image_name}"
    full_image_name = f"{image_name}:{image_version}"
    
    logger.info(f"Starting repo deployment process for {repo_url} (ID: {deployment_id})")
    
    try:
        # 1. Clean up tmp dir if exists
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            
        # 2. Clone repository
        logger.info(f"Cloning {repo_url} into {tmp_dir}...")
        await log_event(
            prisma_client, deployment_id,
            DeploymentEventCreate(
                event_type="STARTED",
                event_status="PENDING",
                source="API",
                message="[1/4] Clonando repositorio Git...",
            )
        )
        clone_proc = await asyncio.create_subprocess_shell(
            f"git clone {repo_url} {tmp_dir}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await clone_proc.communicate()
        if clone_proc.returncode != 0:
            err_msg = stderr.decode()
            logger.error(f"Git clone failed: {err_msg}")
            raise RuntimeError(f"Git clone falló: {err_msg}")

        # 3. Create Kind cluster (if not exists)
        await log_event(
            prisma_client, deployment_id,
            DeploymentEventCreate(
                event_type="STARTED",
                event_status="PENDING",
                source="API",
                message="[2/4] Verificando cluster de Kubernetes (Kind)...",
            )
        )
        check_proc = await asyncio.create_subprocess_shell(
            "kind get clusters",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await check_proc.communicate()
        existing_clusters = stdout.decode().strip().split('\n')

        if cluster_name not in existing_clusters:
            err_msg = (
                f"El clúster '{cluster_name}' no existe. "
                f"Los clústeres disponibles son: {existing_clusters}. "
                f"Por favor usa uno de los clústeres existentes o créalo manualmente desde el host con: "
                f"kind create cluster --name {cluster_name} --config kind-config.yaml"
            )
            logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            logger.info(f"Kind cluster '{cluster_name}' already exists. Using it.")
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="STARTED",
                    event_status="PENDING",
                    source="API",
                    message=f"Clúster '{cluster_name}' detectado. Reutilizando clúster existente.",
                )
            )

        # 5. Build Docker Image
        await log_event(
            prisma_client, deployment_id,
            DeploymentEventCreate(
                event_type="STARTED",
                event_status="PENDING",
                source="API",
                message="[3/4] Compilando imagen Docker (docker build)...",
            )
        )
        build_dir = os.path.join(tmp_dir, docker_context_path)
        
        # Write .env file if provided
        if env_file_content:
            env_path = os.path.join(build_dir, ".env")
            logger.info(f"Writing .env file to {env_path}")
            with open(env_path, "w") as f:
                f.write(env_file_content)

        if is_compose:
            # --- PREPROCESAMIENTO DE COMPOSE ---
            # Inyectar nombres de imágenes explícitos para que kompose y docker-compose coincidan
            import yaml
            compose_file = None
            for f in ["docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"]:
                p = os.path.join(build_dir, f)
                if os.path.exists(p):
                    compose_file = p
                    break
            
            if compose_file:
                try:
                    with open(compose_file, "r") as cf:
                        c_data = yaml.safe_load(cf)
                    
                    if c_data and "services" in c_data:
                        for s_name, s_def in c_data["services"].items():
                            if isinstance(s_def, dict) and "image" not in s_def:
                                s_def["image"] = f"deploy-{deployment_id[:8]}-{s_name}:latest".lower()
                    
                    with open(compose_file, "w") as cf:
                        yaml.dump(c_data, cf)
                    logger.info(f"Injected explicit image names into {compose_file}")
                except Exception as e:
                    logger.warning(f"Could not preprocess compose file: {e}")

            # --- FLUJO DOCKER COMPOSE ---
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="STARTED",
                    event_status="PENDING",
                    source="API",
                    message="[3/4] Compilando imágenes con docker-compose build...",
                )
            )
            build_proc = await asyncio.create_subprocess_shell(
                "docker-compose build",
                cwd=build_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await build_proc.communicate()
            if build_proc.returncode != 0:
                raise RuntimeError(f"docker-compose build falló: {stderr.decode()}")

            img_proc = await asyncio.create_subprocess_shell(
                "docker-compose config --images",
                cwd=build_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await img_proc.communicate()
            compose_images = [img for img in stdout.decode().strip().split('\n') if img]

            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="STARTED",
                    event_status="PENDING",
                    source="API",
                    message=f"[4/4] Inyectando imágenes ({len(compose_images)}) y aplicando kompose...",
                )
            )
            for img in compose_images:
                safe_img_name = img.replace(':', '_').replace('/', '_')
                tar_path = f"/tmp/{safe_img_name}.tar"
                save_proc = await asyncio.create_subprocess_shell(f"docker save {img} -o {tar_path}", stdout=asyncio.subprocess.PIPE)
                await save_proc.communicate()
                cp_proc = await asyncio.create_subprocess_shell(f"docker cp {tar_path} {cluster_name}-control-plane:/{safe_img_name}.tar", stdout=asyncio.subprocess.PIPE)
                await cp_proc.communicate()
                import_proc = await asyncio.create_subprocess_shell(f"docker exec {cluster_name}-control-plane ctr -n k8s.io images import /{safe_img_name}.tar", stdout=asyncio.subprocess.PIPE)
                await import_proc.communicate()
                if os.path.exists(tar_path):
                    os.remove(tar_path)
            
            # Convertir con kompose
            kompose_proc = await asyncio.create_subprocess_shell(
                "kompose convert -o k8s-manifests.yaml",
                cwd=build_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await kompose_proc.communicate()
            if kompose_proc.returncode != 0:
                raise RuntimeError(f"kompose convert falló: {stderr.decode()}")
            
            # Postprocesar k8s-manifests.yaml para inyectar imagePullPolicy: IfNotPresent
            manifests_path = os.path.join(build_dir, "k8s-manifests.yaml")
            if os.path.exists(manifests_path):
                try:
                    import re
                    with open(manifests_path, "r", encoding="utf-8") as mf:
                        m_content = mf.read()
                    
                    # Como kompose no añade imagePullPolicy por defecto, K8s usa Always para tags :latest
                    # Inyectamos imagePullPolicy: IfNotPresent debajo de cualquier declaración de image:
                    def inject_pull_policy(match):
                        prefix = match.group(1)
                        image_val = match.group(2)
                        next_prefix = prefix.replace('-', ' ')
                        return f"{prefix}image: {image_val}\n{next_prefix}imagePullPolicy: IfNotPresent"

                    pattern = r'^([ \t]*(?:-[ \t]+)?)image:[ \t]*(.+)$'
                    
                    # Contamos cuántas veces se encuentra
                    matches = len(re.findall(pattern, m_content, flags=re.MULTILINE))
                    m_content = re.sub(pattern, inject_pull_policy, m_content, flags=re.MULTILINE)
                    
                    with open(manifests_path, "w", encoding="utf-8") as mf:
                        mf.write(m_content)
                    
                    # Registrar un evento de despliegue para saber cuántas imágenes fueron parcheadas
                    await log_event(
                        prisma_client, deployment_id,
                        DeploymentEventCreate(
                            event_type="STARTED",
                            event_status="PENDING",
                            source="API",
                            message=f"Post-procesador: Se inyectó 'imagePullPolicy: IfNotPresent' en {matches} recursos de k8s-manifests.yaml.",
                        )
                    )
                    logger.info(f"Successfully injected {matches} imagePullPolicy occurrences to IfNotPresent in k8s-manifests.yaml")
                except Exception as patch_err:
                    logger.warning(f"Could not patch imagePullPolicy: {patch_err}")

            # Asegurar namespace
            ns_proc = await asyncio.create_subprocess_shell("kubectl create namespace staging-ns", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await ns_proc.communicate()
            
            # Aplicar en kubernetes directamente
            apply_proc = await asyncio.create_subprocess_shell(
                "kubectl apply -f k8s-manifests.yaml -n staging-ns --validate=false",
                cwd=build_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await apply_proc.communicate()
            if apply_proc.returncode != 0:
                raise RuntimeError(f"kubectl apply falló: {stderr.decode()}")

            # --- CREAR INGRESS AUTOMÁTICO PARA EL FRONTEND ---
            # Kompose no genera Ingress. Lo creamos nosotros apuntando al servicio frontend.
            try:
                from app.core.config import settings
                import yaml as _yaml

                # Reabrir el compose ya modificado para detectar el servicio frontend
                compose_data_for_ingress = None
                if compose_file and os.path.exists(compose_file):
                    with open(compose_file, "r") as _cf:
                        compose_data_for_ingress = _yaml.safe_load(_cf)

                if compose_data_for_ingress:
                    frontend_svc, frontend_port = _detect_frontend_service(compose_data_for_ingress)
                else:
                    frontend_svc, frontend_port = "frontend", 80

                if frontend_svc:
                    ingress_host = f"{service_name}-staging.{settings.INGRESS_BASE_DOMAIN}"
                    ingress_manifest = f"""apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {service_name}-compose-ingress
  namespace: staging-ns
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
  - host: {ingress_host}
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: {frontend_svc}
            port:
              number: {frontend_port}
"""
                    ingress_path = os.path.join(build_dir, "k8s-ingress.yaml")
                    with open(ingress_path, "w", encoding="utf-8") as _f:
                        _f.write(ingress_manifest)

                    ingress_proc = await asyncio.create_subprocess_shell(
                        f"kubectl apply -f k8s-ingress.yaml -n staging-ns",
                        cwd=build_dir,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    ing_out, ing_err = await ingress_proc.communicate()
                    if ingress_proc.returncode == 0:
                        await log_event(
                            prisma_client, deployment_id,
                            DeploymentEventCreate(
                                event_type="STARTED",
                                event_status="PENDING",
                                source="API",
                                message=f"Ingress creado: http://{ingress_host}:11000/ → {frontend_svc}:{frontend_port}",
                            )
                        )
                        logger.info(f"Compose Ingress created: {ingress_host} -> {frontend_svc}:{frontend_port}")
                    else:
                        logger.warning(f"No se pudo crear el Ingress compose: {ing_err.decode()}")
            except Exception as ing_ex:
                logger.warning(f"Fallo al crear Ingress automático para compose: {ing_ex}")

            # Marcar como completado directamente (sin worker)
            await prisma_client.deployment.update(
                where={"id": deployment_id},
                data={"status": PrismaDeploymentStatus.SUCCESS, "success": True, "notes": "Desplegado vía docker-compose/kompose"}
            )
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="FINISHED",
                    event_status="SUCCESS",
                    source="API",
                    message="Despliegue de Docker Compose finalizado correctamente.",
                )
            )
            logger.info("Compose deployment process finished successfully.")
            return True, "Process completed successfully."

        else:
            # --- FLUJO DOCKERFILE ORIGINAL ---
            logger.info(f"Building Docker image '{full_image_name}' from context '{build_dir}'...")
            build_proc = await asyncio.create_subprocess_shell(
                f"docker build -t {full_image_name} .",
                cwd=build_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await build_proc.communicate()
            if build_proc.returncode != 0:
                err_msg = stderr.decode()
                logger.error(f"Docker build failed: {err_msg}")
                raise RuntimeError(f"Docker build falló: {err_msg}")

            # 6. Load image into Kind cluster
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="STARTED",
                    event_status="PENDING",
                    source="API",
                    message="[4/4] Inyectando imagen compilada al clúster Kubernetes...",
                )
            )
            logger.info(f"Loading image '{full_image_name}' into cluster '{cluster_name}'...")
            tar_path = f"/tmp/{image_name}_{image_version}.tar"
            
            save_proc = await asyncio.create_subprocess_shell(
                f"docker save {full_image_name} -o {tar_path}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await save_proc.communicate()
            if save_proc.returncode != 0:
                raise RuntimeError("Docker save falló al exportar la imagen")
                
            cp_proc = await asyncio.create_subprocess_shell(
                f"docker cp {tar_path} {cluster_name}-control-plane:/{image_name}_{image_version}.tar",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await cp_proc.communicate()
            if cp_proc.returncode != 0:
                raise RuntimeError("Fallo al copiar la imagen tar al nodo control-plane")
                
            import_proc = await asyncio.create_subprocess_shell(
                f"docker exec {cluster_name}-control-plane ctr -n k8s.io images import /{image_name}_{image_version}.tar",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await import_proc.communicate()
            
            if os.path.exists(tar_path):
                os.remove(tar_path)
                
            if import_proc.returncode != 0:
                err_msg = stderr.decode()
                logger.error(f"Kind manual load image failed: {err_msg}")
                raise RuntimeError(f"Importación de imagen en el nodo falló: {err_msg}")

            # 7. Trigger the worker to perform the actual deploy
            logger.info(f"Image loaded. Enqueueing deployment for {service_name}...")
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="STARTED",
                    event_status="PENDING",
                    source="API",
                    message="Compilación e inyección exitosas. Encolando despliegue de Kubernetes...",
                )
            )
            
            await enqueue_deployment(prisma_client, deployment_id, user_id=None)

            logger.info("Repo deployment process finished successfully.")
            return True, "Process completed successfully."

    except Exception as e:
        logger.exception("Unexpected error in process_repo_deployment")
        # Actualizar base de datos con el fallo
        try:
            await prisma_client.deployment.update(
                where={"id": deployment_id},
                data={
                    "status": PrismaDeploymentStatus.FAILED,
                    "error_message": str(e),
                }
            )
            await log_event(
                prisma_client, deployment_id,
                DeploymentEventCreate(
                    event_type="ERROR",
                    event_status="FAILED",
                    source="API",
                    message=f"Fallo en compilación/clonado: {str(e)[:300]}",
                )
            )
        except Exception as db_err:
            logger.error(f"Fallo al registrar error en base de datos: {db_err}")
            
        return False, str(e)
    finally:
        # Cleanup
        if os.path.exists(tmp_dir):
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to clean up tmp dir {tmp_dir}: {e}")
