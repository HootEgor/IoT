from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, validator
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped

class AccelerometerData(BaseModel):
    x: Optional[float]
    y: Optional[float]
    z: Optional[float]

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @validator("timestamp")
    def parse_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError("Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime
