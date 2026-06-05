from fastapi import APIRouter, HTTPException, Depends

from user.schemas import RegisterRequest, LoginRequest, UpdateRequest, RefreshRequest
from user.service import (
    register,
    login,
    get_user,
    update_user,
    refresh_access_token
)
from user.dependencies import make_auth_dependency


def create_auth_router(client, secret_key: str, expire_minutes: int =4*60):
    router = APIRouter(prefix="/auth", tags=["auth"])
    get_current_user = make_auth_dependency(secret_key)

    @router.post("/register", status_code=201)
    def register_route(body: RegisterRequest):
        try:
            register(client, body.email, body.username, body.password)
            return {"message": "Compte créé"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.post("/login")
    def login_route(body: LoginRequest):
        try:
            return login(
                client,
                body.email,
                body.password,
                secret_key,
                expire_minutes
            )
        except ValueError as e:
            raise HTTPException(status_code=401, detail=str(e))


    @router.post("/refresh_token")
    def refresh_route(body: RefreshRequest):
        try:
            return refresh_access_token(
                body.refresh_token,
                secret_key,
                expire_minutes
            )
        except ValueError as e:
            raise HTTPException(status_code=401, detail=str(e))


    @router.get("/infos")
    def me(current_user: dict = Depends(get_current_user)):
        try:
            return get_user(client, current_user["user_id"])
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))


    @router.patch("/update_info")
    def update_me(
        body: UpdateRequest,
        current_user: dict = Depends(get_current_user)
    ):
        data = body.model_dump(exclude_none=True)
        try:
            update_user(client, current_user["user_id"], data)
            return {"message": "Informations mises à jour"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    return router