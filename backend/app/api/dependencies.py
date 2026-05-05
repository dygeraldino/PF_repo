from fastapi import Header
from typing import Optional
from uuid import UUID

from prisma import Prisma
from app.core.database import get_prisma

async def get_current_user_id(authorization: Optional[str] = Header(None)) -> Optional[str]:
    # Para el MVP, si se provee un token que parezca un UUID válido, lo usamos como user_id.
    # En un entorno real con Supabase, aquí verificaríamos el JWT usando JWT_SECRET.
    if not authorization:
        return None
    token = authorization.replace("Bearer ", "")
    try:
        UUID(token)
        return token
    except ValueError:
        return None
