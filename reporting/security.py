from fastapi import Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
import os

load_dotenv("app.env")

API_TOKEN = os.getenv("API_TOKEN")

bearer_scheme = HTTPBearer()

def verify_internal_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)
):
    if credentials.credentials !=API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid internal API token"
        )