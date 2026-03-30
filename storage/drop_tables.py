import time
from db import ENGINE
from models import Base, Lap, Sector


def drop_tables():
    Base.metadata.drop_all(ENGINE)


if __name__ == "__main__":
    drop_tables()