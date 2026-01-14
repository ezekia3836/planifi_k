# app/routers/database_router.py
from fastapi import APIRouter,FastAPI
from models.Databases import Database


app = FastAPI()
router = APIRouter()
db = Database()


@router.post("/create")
def create_database(id: int, name: str, owner: str):
    return db.create(id, name, owner)

@router.get("/{id}")
def read_database(id: int):
    return db.read(id)

@router.get("/")
def read_all_database():
    return db.read_all()

@router.put("/{id}")
def update_database(id: int, name: str = None, owner: str = None, is_active: int = None):
    updates = {k: v for k, v in {"name": name, "owner": owner, "is_active": is_active}.items() if v is not None}
    return db.update(id, **updates)

@router.delete("/{id}")
def delete_database(id: int):
    return db.delete(id)