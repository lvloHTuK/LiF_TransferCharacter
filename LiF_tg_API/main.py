"""
Основной модуль приложения FastAPI.
Содержит определение endpoints и маршрутизацию.
"""

from fastapi import FastAPI, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from typing import List
import asyncio

from database import SessionLocal, engine
from models import Base

# Исправляем импорты - импортируем конкретные классы из schemas
from schemas import (
    EventCreate, EventResponse,
    ServerCreate, ServerResponse,
    GeneralCreate, GeneralResponse,
    TeamLordCreate, TeamLordResponse,
    ParticipantCreate, ParticipantResponse
)

import crud

# Создаем таблицы в базе данных
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Event Management API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Зависимость для получения сессии базы данных
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Events endpoints
@app.post("/events/", response_model=EventResponse)
async def create_event(event: EventCreate, db: Session = Depends(get_db)):
    return await run_in_threadpool(crud.create_event, db=db, event=event)

@app.get("/events/", response_model=List[EventResponse])
async def read_events(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return await run_in_threadpool(crud.get_events, db, skip=skip, limit=limit)

@app.get("/events/{event_id}", response_model=EventResponse)
async def read_event(event_id: int, db: Session = Depends(get_db)):
    db_event = await run_in_threadpool(crud.get_event, db, event_id=event_id)
    if db_event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return db_event

# Servers endpoints
@app.post("/servers/", response_model=ServerResponse)
async def create_server(server: ServerCreate, db: Session = Depends(get_db)):
    # Проверяем существование события
    db_event = await run_in_threadpool(crud.get_event, db, event_id=server.event_id)
    if db_event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return await run_in_threadpool(crud.create_server, db=db, server=server)

@app.get("/servers/{server_id}", response_model=ServerResponse)
async def read_server(server_id: int, db: Session = Depends(get_db)):
    db_server = await run_in_threadpool(crud.get_server, db, server_id=server_id)
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    return db_server

@app.get("/events/{event_id}/servers/", response_model=List[ServerResponse])
async def read_servers_by_event(event_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # Проверяем существование события
    db_event = await run_in_threadpool(crud.get_event, db, event_id=event_id)
    if db_event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return await run_in_threadpool(crud.get_servers_by_event, db, event_id=event_id, skip=skip, limit=limit)

# Generals endpoints
@app.post("/generals/", response_model=GeneralResponse)
async def create_general(general: GeneralCreate, db: Session = Depends(get_db)):
    # Проверяем существование сервера
    db_server = await run_in_threadpool(crud.get_server, db, server_id=general.server_id)
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    return await run_in_threadpool(crud.create_general, db=db, general=general)

@app.get("/generals/{general_id}", response_model=GeneralResponse)
async def read_general(general_id: int, db: Session = Depends(get_db)):
    db_general = await run_in_threadpool(crud.get_general, db, general_id=general_id)
    if db_general is None:
        raise HTTPException(status_code=404, detail="General not found")
    return db_general

@app.get("/servers/{server_id}/generals/", response_model=List[GeneralResponse])
async def read_generals_by_server(server_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # Проверяем существование сервера
    db_server = await run_in_threadpool(crud.get_server, db, server_id=server_id)
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    return await run_in_threadpool(crud.get_generals_by_server, db, server_id=server_id, skip=skip, limit=limit)

# TeamLords endpoints
@app.post("/lords/", response_model=TeamLordResponse)
async def create_team_lord(lord: TeamLordCreate, db: Session = Depends(get_db)):
    # Проверяем существование генерала
    db_general = await run_in_threadpool(crud.get_general, db, general_id=lord.general_id)
    if db_general is None:
        raise HTTPException(status_code=404, detail="General not found")
    return await run_in_threadpool(crud.create_team_lord, db=db, lord=lord)

@app.get("/lords/{lord_id}", response_model=TeamLordResponse)
async def read_team_lord(lord_id: int, db: Session = Depends(get_db)):
    db_lord = await run_in_threadpool(crud.get_team_lord, db, lord_id=lord_id)
    if db_lord is None:
        raise HTTPException(status_code=404, detail="Team lord not found")
    return db_lord

@app.get("/generals/{general_id}/lords/", response_model=List[TeamLordResponse])
async def read_lords_by_general(general_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # Проверяем существование генерала
    db_general = await run_in_threadpool(crud.get_general, db, general_id=general_id)
    if db_general is None:
        raise HTTPException(status_code=404, detail="General not found")
    return await run_in_threadpool(crud.get_lords_by_general, db, general_id=general_id, skip=skip, limit=limit)

# Participants endpoints
@app.post("/participants/", response_model=ParticipantResponse)
async def create_participant(participant: ParticipantCreate, db: Session = Depends(get_db)):
    # Проверяем существование лорда
    db_lord = await run_in_threadpool(crud.get_team_lord, db, lord_id=participant.lord_id)
    if db_lord is None:
        raise HTTPException(status_code=404, detail="Team lord not found")
    return await run_in_threadpool(crud.create_participant, db=db, participant=participant)

@app.get("/participants/{participant_id}", response_model=ParticipantResponse)
async def read_participant(participant_id: int, db: Session = Depends(get_db)):
    db_participant = await run_in_threadpool(crud.get_participant, db, participant_id=participant_id)
    if db_participant is None:
        raise HTTPException(status_code=404, detail="Participant not found")
    return db_participant

@app.get("/lords/{lord_id}/participants/", response_model=List[ParticipantResponse])
async def read_participants_by_lord(lord_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # Проверяем существование лорда
    db_lord = await run_in_threadpool(crud.get_team_lord, db, lord_id=lord_id)
    if db_lord is None:
        raise HTTPException(status_code=404, detail="Team lord not found")
    return await run_in_threadpool(crud.get_participants_by_lord, db, lord_id=lord_id, skip=skip, limit=limit)

# Убрали блок if __name__ == "__main__", так как он не совместим с workers