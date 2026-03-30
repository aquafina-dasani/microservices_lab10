from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# <mysql>+<mysql_connector_driver>://<db_user>:<db_user_password>@<host>:<port>/<db_name>
ENGINE = create_engine("mysql://ncao5:1220@db:3306/race_db")

def make_session():
    return sessionmaker(bind=ENGINE)()


"""
SQLite may not be a good choice for this type of service because:
- there are many operations being sent
- but only one operation can be read at a time; Single Writer Lock
- when an a write operation is being done, 
    all of the other operations must wait for the current operation to finish before the next can write to the db
"""