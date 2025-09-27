import pandas as pd
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Event, Server, General, TeamLord, Participant


def import_excel_to_db(excel_path: str):
    # Чтение Excel файла
    df = pd.read_excel(excel_path)

    # Создание сессии БД
    db = SessionLocal()

    try:
        # Создание или получение события
        event_name = df['Event'].iloc[0]  # Берем первое значение из колонки Event
        event = db.query(Event).filter(Event.name == event_name).first()
        if not event:
            event = Event(name=event_name)
            db.add(event)
            db.commit()
            db.refresh(event)

        # Создание или получение сервера
        server_name = df['Servers'].iloc[0]  # Берем первое значение из колонки Servers
        server = db.query(Server).filter(Server.name == server_name, Server.event_id == event.id).first()
        if not server:
            server = Server(name=server_name, event_id=event.id)
            db.add(server)
            db.commit()
            db.refresh(server)

        # Обработка генералов и их команд
        for _, row in df.iterrows():
            general_name = row['Generals']
            team_lord_name = row['Team_Lords']
            participant_fio = row['Participants']

            # Создание или получение генерала
            general = db.query(General).filter(
                General.name == general_name,
                General.server_id == server.id
            ).first()

            if not general:
                general = General(name=general_name, server_id=server.id)
                db.add(general)
                db.commit()
                db.refresh(general)

            # Создание или получение лорда команды
            team_lord = db.query(TeamLord).filter(
                TeamLord.name == team_lord_name,
                TeamLord.general_id == general.id
            ).first()

            if not team_lord:
                team_lord = TeamLord(
                    name=team_lord_name,
                    general_id=general.id,
                    password="default_password"  # Установите надежный пароль
                )
                db.add(team_lord)
                db.commit()
                db.refresh(team_lord)

            # Создание участника
            participant = db.query(Participant).filter(
                Participant.fio == participant_fio,
                Participant.lord_id == team_lord.id
            ).first()

            if not participant:
                participant = Participant(
                    fio=participant_fio,
                    lord_id=team_lord.id
                )
                db.add(participant)

        db.commit()
        print("Данные успешно импортированы!")

    except Exception as e:
        db.rollback()
        print(f"Произошла ошибка: {str(e)}")
    finally:
        db.close()


if __name__ == "__main__":
    import_excel_to_db("Распределение LIF.xlsx")