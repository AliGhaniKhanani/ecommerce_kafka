from typing import List
from passlib.hash import bcrypt # type: ignore
from sqlmodel import Session, select
from app.main import User
from .tokens import create_refresh_token, create_access_token, decode_token
from ..settings import ACCESS_EXPIRY_TIME, REFRESH_EXPIRY_TIME

def create_user(db: Session, user_data: User):
    hashed_password = bcrypt.hash(user_data.password)
    user = User(username=user_data.user_name, email=user_data.email, password=hashed_password)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def get_user(db: Session, user_id: int):
    user = db.get(User, user_id)
    return user

def get_users(db: Session, skip: int = 0, limit: int = 100):
    users = db.exec(select(User).offset(skip).limit(limit)).all()
    return users

def update_user(db: Session, user_id: int, user_data: dict):
    user = db.get(User, user_id)
    for key, value in user_data.items():
        setattr(user, key, value)
    db.commit()
    db.refresh(user)
    return user

def delete_user(db: Session, user_id: int):
    user = db.get(User, user_id)
    db.delete(user)
    db.commit()
    return {"message": "User deleted successfully"}

def signup(db: Session, user_data: User):
    user_exist = db.exec(select(User).where(User.email == user_data.email)).all()
    if len(user_exist)>0:
        print("User already exists: ", user_data)
        return
    user = create_user(db, user_data)
    to_encode = {
        "email": user.email
    }
    access_token = create_access_token(to_encode, ACCESS_EXPIRY_TIME)
    refresh_token = create_refresh_token(to_encode, REFRESH_EXPIRY_TIME)
    return {"user": user, "refresh_token": refresh_token, "access_token": access_token}

def login(db: Session, username: str, password: str):
    user = db.exec(select(User).where(User.user_name == username)).first()
    if user and bcrypt.verify(password, user.password):
        to_encode = {
            "email": user.email
        }
        access_token = create_access_token(to_encode, ACCESS_EXPIRY_TIME)
        refresh_token = create_refresh_token(to_encode, REFRESH_EXPIRY_TIME)
        return {"user": user, "refresh_token": refresh_token, "access_token": access_token}
    return None

def decode_access_token(token: str):
    return decode_token(token)