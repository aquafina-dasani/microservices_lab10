from db import ENGINE
from models import Base, Lap, Sector


def create_tables():
    Base.metadata.create_all(ENGINE)


if __name__ == "__main__":
    create_tables()