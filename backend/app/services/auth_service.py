import logging
from typing import Optional

import httpx
from fastapi import HTTPException
from prisma import Prisma
from prisma.enums import UserRole as PrismaUserRole

from app.core.config import settings
from app.schemas.auth import RegisterRequest

logger = logging.getLogger(__name__)


def _normalize_supabase_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    base = raw_url.rstrip("/")
    if base.endswith("/rest/v1"):
        base = base[: -len("/rest/v1")]
    return base


def _auth_admin_url() -> str:
    base = _normalize_supabase_url(settings.SUPABASE_URL)
    if not base:
        return ""
    return f"{base}/auth/v1/admin/users"


async def create_auth_user(register: RegisterRequest) -> str:
    admin_url = _auth_admin_url()
    if not admin_url or not settings.SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Supabase admin no configurado")

    headers = {
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": settings.SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }

    payload = {
        "email": register.email,
        "password": register.password,
        "email_confirm": True,
        "user_metadata": {
            "full_name": register.full_name,
            "role": register.role.value,
            "team": register.team,
        },
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(admin_url, headers=headers, json=payload)

    try:
        data = response.json() if response.content else {}
    except ValueError:
        data = {}
    if response.status_code >= 400:
        message = data.get("msg") or data.get("message") or "Error creando usuario en Supabase"
        raise HTTPException(status_code=400, detail=message)

    user_id = data.get("id") or data.get("user", {}).get("id")
    if not user_id:
        logger.error("Respuesta inesperada de Supabase al crear usuario: %s", data)
        raise HTTPException(status_code=502, detail="Respuesta invalida de Supabase")

    return user_id


async def delete_auth_user(user_id: str) -> None:
    admin_url = _auth_admin_url()
    if not admin_url or not settings.SUPABASE_SERVICE_ROLE_KEY:
        return

    headers = {
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": settings.SUPABASE_SERVICE_ROLE_KEY,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        await client.delete(f"{admin_url}/{user_id}", headers=headers)


async def create_user_profile(prisma: Prisma, user_id: str, register: RegisterRequest):
    return await prisma.userprofile.create(
        data={
            "id": user_id,
            "email": register.email,
            "full_name": register.full_name,
            "role": PrismaUserRole(register.role.value),
            "team": register.team,
        }
    )
