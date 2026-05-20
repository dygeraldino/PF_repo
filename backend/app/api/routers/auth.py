from fastapi import APIRouter, Depends, HTTPException
from prisma import Prisma

from app.core.database import get_prisma
from app.schemas.auth import RegisterRequest, UserProfileResponse
from app.services.auth_service import create_auth_user, create_user_profile, delete_auth_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserProfileResponse, status_code=201)
async def register_user(
    payload: RegisterRequest,
    prisma: Prisma = Depends(get_prisma),
):
    user_id = await create_auth_user(payload)

    try:
        profile = await create_user_profile(prisma, user_id, payload)
    except Exception as exc:
        await delete_auth_user(user_id)
        raise HTTPException(status_code=500, detail="No se pudo crear el perfil") from exc

    return UserProfileResponse.model_validate(profile)
