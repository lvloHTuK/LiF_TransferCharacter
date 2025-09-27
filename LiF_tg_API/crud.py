"""
Модуль с CRUD операциями (Create, Read, Update, Delete).
Содержит функции для взаимодействия с базой данных.
"""

# Исправленные импорты в crud.py
from sqlalchemy.orm import Session
from models import Event, Server, General, TeamLord, Participant
from schemas import EventCreate, ServerCreate, GeneralCreate, TeamLordCreate, ParticipantCreate

# Остальной код остается без изменений...

# CRUD для Event
def get_event(db: Session, event_id: int):
    return db.query(Event).filter(Event.id == event_id).first()

def get_events(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Event).offset(skip).limit(limit).all()

def create_event(db: Session, event: EventCreate):
    db_event = Event(name=event.name)
    db.add(db_event)
    db.commit()
    db.refresh(db_event)
    return db_event

# CRUD для Server
def get_server(db: Session, server_id: int):
    return db.query(Server).filter(Server.id == server_id).first()

def get_servers_by_event(db: Session, event_id: int, skip: int = 0, limit: int = 100):
    return db.query(Server).filter(Server.event_id == event_id).offset(skip).limit(limit).all()

def create_server(db: Session, server: ServerCreate):
    db_server = Server(name=server.name, event_id=server.event_id, server_name=server.server_name)
    db.add(db_server)
    db.commit()
    db.refresh(db_server)
    return db_server

# CRUD для General
def get_general(db: Session, general_id: int):
    return db.query(General).filter(General.id == general_id).first()

def get_generals_by_server(db: Session, server_id: int, skip: int = 0, limit: int = 100):
    return db.query(General).filter(General.server_id == server_id).offset(skip).limit(limit).all()

def create_general(db: Session, general: GeneralCreate):
    db_general = General(name=general.name, server_id=general.server_id)
    db.add(db_general)
    db.commit()
    db.refresh(db_general)
    return db_general

# CRUD для TeamLord
def get_team_lord(db: Session, lord_id: int):
    return db.query(TeamLord).filter(TeamLord.id == lord_id).first()

def get_lords_by_general(db: Session, general_id: int, skip: int = 0, limit: int = 100):
    return db.query(TeamLord).filter(TeamLord.general_id == general_id).offset(skip).limit(limit).all()

def create_team_lord(db: Session, lord: TeamLordCreate):
    db_lord = TeamLord(
        name=lord.name,
        general_id=lord.general_id,
        password=lord.password  # В реальном приложении нужно хэшировать пароль
    )
    db.add(db_lord)
    db.commit()
    db.refresh(db_lord)
    return db_lord

# CRUD для Participant
def get_participant(db: Session, participant_id: int):
    return db.query(Participant).filter(Participant.id == participant_id).first()

def get_participants_by_lord(db: Session, lord_id: int, skip: int = 0, limit: int = 100):
    return db.query(Participant).filter(Participant.lord_id == lord_id).offset(skip).limit(limit).all()

def create_participant(db: Session, participant: ParticipantCreate):
    db_participant = Participant(
        fio=participant.fio,
        lord_id=participant.lord_id,
        player_id=participant.player_id,
        server_name=participant.server_name  # Добавлено сохранение server_name
    )
    db.add(db_participant)
    db.commit()
    db.refresh(db_participant)
    return db_participant