# Исправленные импорты в models.py
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from database import Base

# Остальной код остается без изменений...


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)  # Название события

    # Связь один-ко-многим с серверами (у одного события много серверов)
    servers = relationship("Server", back_populates="event")


class Server(Base):
    __tablename__ = "servers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)  # Название сервера

    # Внешний ключ для связи с событием
    event_id = Column(Integer, ForeignKey("events.id"))

    # Связь многие-к-одному с событием (много серверов у одного события)
    event = relationship("Event", back_populates="servers")

    # Связь один-ко-многим с генералами (у одного сервера много генералов)
    generals = relationship("General", back_populates="server")

    server_name = Column(String)


class General(Base):
    __tablename__ = "generals"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)  # Имя генерала

    # Внешний ключ для связи с сервером
    server_id = Column(Integer, ForeignKey("servers.id"))

    # Связь многие-к-одному с сервером (много генералов у одного сервера)
    server = relationship("Server", back_populates="generals")

    # Связь один-ко-многим с лордами команд (у одного генерала много лордов)
    lords = relationship("TeamLord", back_populates="general")


class TeamLord(Base):
    __tablename__ = "team_lords"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)  # Имя лорда команды

    # Внешний ключ для связи с генералом
    general_id = Column(Integer, ForeignKey("generals.id"))

    # Пароль для аутентификации (в реальном приложении должен храниться в хэшированном виде)
    password = Column(String)

    # Связь многие-к-одному с генералом (много лордов у одного генерала)
    general = relationship("General", back_populates="lords")

    # Связь один-ко-многим с участниками (у одного лорда много участников)
    participants = relationship("Participant", back_populates="lord")


class Participant(Base):
    __tablename__ = "participants"

    id = Column(Integer, primary_key=True, index=True)
    fio = Column(String, index=True)
    player_id = Column(Integer)
    server_name = Column(String, nullable=True)  # Добавлено необязательное поле
    lord_id = Column(Integer, ForeignKey("team_lords.id"))
    lord = relationship("TeamLord", back_populates="participants")