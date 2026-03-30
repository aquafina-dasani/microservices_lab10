from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, Float, func

class Base(DeclarativeBase):
    pass


class Lap(Base):
    __tablename__ = "lap"
    id = mapped_column(Integer, primary_key=True)
    car_id = mapped_column(String(250), nullable=False)
    batch_id = mapped_column(String(250), nullable=False)
    stint_id = mapped_column(String(250), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    lap_number = mapped_column(Integer, nullable=False)

    lap_time_seconds = mapped_column(Float, nullable=False)
    fuel_consumed_kg = mapped_column(Float, nullable=False)
    max_speed_kph =  mapped_column(Float, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.utc_timestamp())

    trace_id = mapped_column(String(250), nullable=False)

    def to_dict(self):
        """ Dictionary representation of a lap """
        dict = {}
        dict['car_id'] = self.car_id
        dict['batch_id'] = self.batch_id
        dict['stint_id'] = self.stint_id
        dict['batch_timestamp'] = self.batch_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        dict['lap_number'] = self.lap_number
        dict['lap_time_seconds'] = self.lap_time_seconds
        dict['fuel_consumed_kg'] = self.fuel_consumed_kg
        dict['max_speed_kph'] = self.max_speed_kph
        dict['trace_id'] = self.trace_id
        dict['date_created'] = self.date_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return dict

class Sector(Base):
    __tablename__ = "sector"
    id = mapped_column(Integer, primary_key=True)
    car_id = mapped_column(String(250), nullable=False)
    batch_id = mapped_column(String(250), nullable=False)
    stint_id = mapped_column(String(250), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    lap_number = mapped_column(Integer, nullable=False)

    sector_id = mapped_column(String(250), nullable=False)
    sector_time_seconds = mapped_column(Float, nullable=False)
    entry_speed_kph = mapped_column(Float, nullable=False)
    exit_speed_kph = mapped_column(Float, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.utc_timestamp())

    trace_id = mapped_column(String(250), nullable=False)

    def to_dict(self):
        """ Dictionary representation of a sector """
        dict = {}
        dict['car_id'] = self.car_id
        dict['batch_id'] = self.batch_id
        dict['stint_id'] = self.stint_id
        dict['batch_timestamp'] = self.batch_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        dict['lap_number'] = self.lap_number
        dict['sector_id'] = self.sector_id
        dict['sector_time_seconds'] = self.sector_time_seconds
        dict['entry_speed_kph'] = self.entry_speed_kph
        dict['exit_speed_kph'] = self.exit_speed_kph
        dict['trace_id'] = self.trace_id
        dict['date_created'] = self.date_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return dict