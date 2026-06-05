from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

bearer = HTTPBearer()


def make_auth_dependency(secret_key: str):
    def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(bearer),) -> dict:
        try:
            payload = jwt.decode(
                credentials.credentials, secret_key, algorithms=["HS256"]
            )
            return {"user_id": payload["sub"]}
        except JWTError:
            raise HTTPException(status_code=401, detail="Token invalide ou expiré")

    return get_current_user
