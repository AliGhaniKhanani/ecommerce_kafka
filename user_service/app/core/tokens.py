from datetime import datetime, timezone, timedelta
from jose import jwt # type:ignore
from app.settings import SECRET_KEY, ALGORITHM

def create_access_token(to_encode: dict, expires_delta: timedelta):
    to_encode.update(
        {
            "exp": datetime.now(timezone.utc) + expires_delta
        }
    )
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(to_encode: dict, expires_delta: timedelta):
    to_encode.update(
        {
            "exp": datetime.now(timezone.utc) + expires_delta
        }
    )
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        # Handle expired token
        return None
    except jwt.InvalidTokenError:
        # Handle invalid token
        return None