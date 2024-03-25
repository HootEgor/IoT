from typing import List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from starlette.websockets import WebSocket
from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from models import ProcessedAgentData, ProcessedAgentDataInDB, AgentData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import Sequence
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)

# Use Session for synchronous database operations
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

metadata = MetaData()

Base = declarative_base()
# Define the ProcessedAgentData table
class ProcessedAgentDataDB(Base):
    __tablename__ = 'processed_agent_data'

    id = Column(Integer, Sequence('processed_agent_data_id_seq'), primary_key=True, index=True)
    road_state = Column(String)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    timestamp = Column(DateTime)

# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(data)

# WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# CRUDL endpoints
@app.post("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
async def create_processed_agent_data(data: List[ProcessedAgentData], db: Session = Depends(get_db)):
    db_list = []
    for item in data:
        db_entry = ProcessedAgentDataDB(
            road_state=item.road_state,
            x=item.agent_data.accelerometer.x,
            y=item.agent_data.accelerometer.y,
            z=item.agent_data.accelerometer.z,
            latitude=item.agent_data.gps.latitude,
            longitude=item.agent_data.gps.longitude,
            timestamp=item.agent_data.timestamp,
        )
        db.add(db_entry)
        db.commit()
        db.refresh(db_entry)
        db_list.append(db_entry)
    return db_list

@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    db_entry = db.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
    if db_entry is None:
        raise HTTPException(status_code=404, detail="Processed Agent Data not found")
    return db_entry

@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data(db: Session = Depends(get_db)):
    return db.query(ProcessedAgentDataDB).all()

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData, db: Session = Depends(get_db)):
    db_entry = db.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
    if db_entry is None:
        raise HTTPException(status_code=404, detail="Processed Agent Data not found")
    db_entry.road_state = data.road_state
    db_entry.x = data.agent_data.accelerometer.x
    db_entry.y = data.agent_data.accelerometer.y
    db_entry.z = data.agent_data.accelerometer.z
    db_entry.latitude = data.agent_data.gps.latitude
    db_entry.longitude = data.agent_data.gps.longitude
    db_entry.timestamp = data.agent_data.timestamp
    db.commit()
    db.refresh(db_entry)
    return db_entry

@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    db_entry = db.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
    if db_entry is None:
        raise HTTPException(status_code=404, detail="Processed Agent Data not found")
    db.delete(db_entry)
    db.commit()
    return db_entry

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
