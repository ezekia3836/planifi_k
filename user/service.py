from jose import jwt, JWTError
from datetime import datetime, timedelta
import bcrypt


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_token(user_id: str, secret_key: str, expire_minutes: int) -> str:
    payload = {
        "sub": user_id,
        "type": "access",
        "exp": datetime.utcnow() + timedelta(minutes=expire_minutes),
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def create_refresh_token(user_id: str, secret_key: str, expire_days: int = 7) -> str:
    payload = {
        "sub": user_id,
        "type": "refresh",
        "exp": datetime.utcnow() + timedelta(days=expire_days),
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def refresh_access_token(refresh_token: str, secret_key: str, expire_minutes: int) -> dict:
    try:
        payload = jwt.decode(refresh_token, secret_key, algorithms=["HS256"])

        if payload.get("type") != "refresh":
            raise ValueError("Token invalide")

        user_id = payload.get("sub")
        if not user_id:
            raise ValueError("Token invalide")

        new_access_token = create_token(
            user_id,
            secret_key,
            expire_minutes
        )

        return {
            "access_token": new_access_token,
            "token_type": "bearer"
        }

    except JWTError:
        raise ValueError("Refresh token invalide ou expiré")


def register(client, email: str, username: str, password: str):
    result = client.query(
        "SELECT 1 FROM users WHERE email = %(e)s LIMIT 1",
        parameters={"e": email}
    )

    if result.result_rows:
        raise ValueError("Email déjà utilisé")

    client.insert(
        "users",
        [[email, username, hash_password(password)]],
        column_names=["email", "username", "password"]
    )


def login(client, email: str, password: str, secret_key: str, expire_minutes: int) -> dict:
    result = client.query(
        "SELECT id, password, is_active FROM users WHERE email = %(e)s LIMIT 1",
        parameters={"e": email}
    )

    if not result.result_rows:
        raise ValueError("Identifiants invalides")

    user_id, pw_hash, is_active = result.result_rows[0]

    if not is_active or not verify_password(password, pw_hash):
        raise ValueError("Identifiants invalides")

    access_token = create_token(str(user_id), secret_key, expire_minutes)
    refresh_token = create_refresh_token(str(user_id), secret_key)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


def get_user(client, user_id: str) -> dict:
    result = client.query(
        "SELECT id, email, username, created_at FROM users WHERE id = %(id)s LIMIT 1",
        parameters={"id": user_id}
    )

    if not result.result_rows:
        raise ValueError("Utilisateur introuvable")

    uid, email, username, created_at = result.result_rows[0]

    return {
        "user_id": str(uid),
        "email": email,
        "username": username,
        "created_at": created_at,
    }


def update_user(client, user_id: str, data: dict):
    if not data:
        raise ValueError("Aucune donnée à mettre à jour")

    result = client.query(
        "SELECT email, username, password FROM users WHERE id = %(id)s LIMIT 1",
        parameters={"id": user_id}
    )

    if not result.result_rows:
        raise ValueError("Utilisateur introuvable")

    current_email, current_username, current_hash = result.result_rows[0]

    new_email = data.get("email", current_email)

    if new_email != current_email:
        exists = client.query(
            "SELECT 1 FROM users WHERE email = %(e)s LIMIT 1",
            parameters={"e": new_email}
        )
        if exists.result_rows:
            raise ValueError("Email déjà utilisé")

    new_username = data.get("username", current_username)
    new_hash = hash_password(data["password"]) if "password" in data else current_hash

    client.command(
        """
        ALTER TABLE users UPDATE
            email = %(email)s,
            username = %(username)s,
            password = %(password)s
        WHERE id = %(id)s
        """,
        parameters={
            "email": new_email,
            "username": new_username,
            "password": new_hash,
            "id": user_id,
        }
    )