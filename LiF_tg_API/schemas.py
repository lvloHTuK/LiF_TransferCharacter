"""
Модуль с Pydantic-схемами для валидации данных.
Схемы определяют структуру данных для запросов и ответов API.
"""

from pydantic import BaseModel, field_validator
from typing import Optional, List


# Базовые схемы
class EventBase(BaseModel):
    name: str


class ServerBase(BaseModel):
    name: str
    event_id: int
    server_name: Optional[str] = None


class GeneralBase(BaseModel):
    name: str
    server_id: int
    server_name: Optional[str] = None


class TeamLordBase(BaseModel):
    name: str
    general_id: int
    password: str


class ParticipantBase(BaseModel):
    fio: str
    lord_id: int
    player_id: Optional[int] = None
    server_name: Optional[str] = None  # Добавлено необязательное поле

# Схемы для создания (без ID)
class EventCreate(EventBase):
    pass


class ServerCreate(ServerBase):
    pass


class GeneralCreate(GeneralBase):
    pass


class TeamLordCreate(TeamLordBase):
    # Валидация пароля (минимальная длина 6 символов)
    @field_validator('password')
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError('Пароль должен содержать минимум 6 символов')
        return v


class ParticipantCreate(ParticipantBase):
    pass


# Схемы для ответов (с ID)
class EventResponse(EventBase):
    id: int

    class Config:
        from_attributes = True


class ServerResponse(ServerBase):
    id: int

    class Config:
        from_attributes = True


class GeneralResponse(GeneralBase):
    id: int

    class Config:
        from_attributes = True


class TeamLordResponse(TeamLordBase):
    id: int

    class Config:
        from_attributes = True


class ParticipantResponse(ParticipantBase):
    id: int

    class Config:
        from_attributes = True


# Схемы с отношениями (для отображения связанных данных)
class EventWithServers(EventResponse):
    servers: List[ServerResponse] = []


class ServerWithGenerals(ServerResponse):
    generals: List[GeneralResponse] = []


class GeneralWithLords(GeneralResponse):
    lords: List[TeamLordResponse] = []


class TeamLordWithParticipants(TeamLordResponse):
    participants: List[ParticipantResponse] = []