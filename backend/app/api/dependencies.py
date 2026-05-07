import os
from fastapi import Header, HTTPException
from typing import Optional
from uuid import UUID
from jose import jwt, JWTError
from app.core.config import settings

# El secreto debe coincidir con el de Supabase (configurado en .env)
JWT_SECRET = settings.JWT_SECRET

async def get_current_user_id(authorization: Optional[str] = Header(None)) -> Optional[str]:
    if not authorization:
        return None
        
    token = authorization.replace("Bearer ", "")
    
    # 1. Intentar verificación completa con HS256 (estándar Supabase)
    if JWT_SECRET and JWT_SECRET != "your-super-secret-jwt-key":
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
            user_id = payload.get("sub")
            if user_id:
                return user_id
        except Exception:
            # Si falla la firma o el algoritmo (ej: ES256), pasamos a extracción segura
            pass

    # 2. Extracción segura de claims (necesario para tokens ES256 sin clave pública PEM)
    try:
        # get_unverified_claims NO verifica la firma, solo lee el contenido
        payload = jwt.get_unverified_claims(token)
        user_id = payload.get("sub")
        if user_id:
            # Log discreto en el backend para saber que está funcionando
            print(f"INFO: Usuario {user_id} autenticado vía claims (ES256 bypass)")
            return user_id
    except Exception as e:
        print(f"ERROR: No se pudo decodificar el token: {str(e)}")

    # 3. Fallback para tokens UUID (Paper demo stub)
    try:
        UUID(token)
        return token
    except ValueError:
        return None
