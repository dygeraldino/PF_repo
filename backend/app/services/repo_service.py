import os
import subprocess
import shutil
import logging
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)

async def process_repo_deployment(
    deployment_id: str,
    repo_url: str,
    docker_context_path: str,
    service_name: str,
    cluster_name: str,
    image_name: str,
    image_version: str,
    env_file_content: str = None,
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
        
        # Use manual save/cp/import to avoid containerd snapshotter detection errors inside DinD
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
        
        # Cleanup tarball
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
        
        # Enqueue the pre-created deployment
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
