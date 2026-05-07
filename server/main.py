import uuid
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cassandra.cluster import Cluster

app = FastAPI(title="Distributed AI Gateway - Leader Node")

# Allow React app to communicate with FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to Cassandra
cluster = None
session = None

@app.on_event("startup")
def startup_event():
    global cluster, session
    print("Connecting to Cassandra...")
    for attempt in range(10):
        try:
            cluster = Cluster(["127.0.0.1"], port=9042)
            session = cluster.connect('web_app')
            print("Successfully connected to Cassandra keyspace 'web_app'")
            return
        except Exception as e:
            print(f"Waiting for Cassandra (attempt {attempt + 1}/10)...")
            time.sleep(2)
    print("WARNING: Could not connect to Cassandra on startup.")

@app.on_event("shutdown")
def shutdown_event():
    if cluster:
        cluster.shutdown()

class SignupRequest(BaseModel):
    name: str
    email: str
    password: str
    role: str

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/auth/signup")
async def signup(req: SignupRequest):
    if not session:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    # Check if user already exists
    query = "SELECT email FROM users WHERE email = %s"
    existing = session.execute(query, (req.email,)).one()
    
    if existing:
        raise HTTPException(status_code=400, detail="An account with that email already exists.")
    
    user_id = uuid.uuid4()
    
    insert_query = """
        INSERT INTO users (user_id, username, email, password_hash, role, created_at)
        VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
    """
    try:
        session.execute(insert_query, (user_id, req.name, req.email, req.password, req.role))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    return {
        "id": str(user_id),
        "name": req.name,
        "email": req.email,
        "role": req.role
    }

@app.post("/auth/login")
async def login(req: LoginRequest):
    if not session:
        raise HTTPException(status_code=500, detail="Database not connected")
        
    # Real app would check password hash, this uses plain text for now
    query = "SELECT user_id, username, email, password_hash, role FROM users WHERE email = %s"
    user = session.execute(query, (req.email,)).one()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    if user.password_hash != req.password:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    return {
        "id": str(user.user_id),
        "name": user.username,
        "email": user.email,
        "role": user.role
    }

@app.get("/health")
async def health():
    return {"status": "ok"}
