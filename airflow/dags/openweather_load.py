from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from openweather_transform import transform_main
import yaml, logging
import plugins.logger_setup

# Load configuration
with open("/opt/airflow/dags/secrets.yaml", "r") as f:
    config = yaml.safe_load(f)

log_file_path = config['log_file_path']
load_logger = plugins.logger_setup.Logger(name='openweather_etl', log_file=log_file_path).get_logger()

Base = declarative_base()

# ORM model for weather data
class WeatherData(Base):
    __tablename__ = config['postgres_table_name']

    id = Column(Integer, primary_key=True, autoincrement=True)
    coord_lon = Column(Float, nullable=True)
    coord_lat = Column(Float, nullable=True)
    weather_0_id = Column(Integer, nullable=True)
    weather_0_main = Column(String, nullable=True)
    weather_0_description = Column(String, nullable=True)
    weather_0_icon = Column(String, nullable=True)
    base = Column(String, nullable=True)
    main_temp = Column(Float, nullable=True)
    main_feels_like = Column(Float, nullable=True)
    main_temp_min = Column(Float, nullable=True)
    main_temp_max = Column(Float, nullable=True)
    main_pressure = Column(Integer, nullable=True)
    main_humidity = Column(Integer, nullable=True)
    visibility = Column(Integer, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_deg = Column(Integer, nullable=True)
    clouds_all = Column(Integer, nullable=True)
    dt = Column(DateTime, nullable=True)
    sys_type = Column(Integer, nullable=True)
    sys_id = Column(Integer, nullable=True)
    sys_country = Column(String, nullable=True)
    sys_sunrise = Column(DateTime, nullable=True)
    sys_sunset = Column(DateTime, nullable=True)
    timezone = Column(Integer, nullable=True)
    city_id = Column(Integer, nullable=True)  
    city_name = Column(String, nullable=True)  
    cod = Column(Integer, nullable=True)

    def __repr__(self):
        return f"<WeatherData(id={self.id}, coord_lon={self.coord_lon}, coord_lat={self.coord_lat}, weather_0_id={self.weather_0_id}, weather_0_main={self.weather_0_main}, weather_0_description={self.weather_0_description}, weather_0_icon={self.weather_0_icon}, base={self.base}, main_temp={self.main_temp}, main_feels_like={self.main_feels_like}, main_temp_min={self.main_temp_min}, main_temp_max={self.main_temp_max}, main_pressure={self.main_pressure}, main_humidity={self.main_humidity}, visibility={self.visibility}, wind_speed={self.wind_speed}, wind_deg={self.wind_deg}, clouds_all={self.clouds_all}, dt={self.dt}, sys_type={self.sys_type}, sys_id={self.sys_id}, sys_country={self.sys_country}, sys_sunrise={self.sys_sunrise}, sys_sunset={self.sys_sunset}, timezone={self.timezone}, city_id={self.city_id}, city_name={self.city_name}, cod={self.cod})>"

# Function to load data into the database
def load_main(postgres_uri, dataframe=None):
    # Create engine and session
    try:
        engine = create_engine(postgres_uri)
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        load_logger.error(f"Error connecting to the database: {e}")
        raise
    
    # Create table
    # Base.metadata.drop_all(engine)
    try:
        Base.metadata.create_all(engine)
    except Exception as e:
        load_logger.error(f"Error creating table: {e}")
        raise

    try:
        if dataframe is None:
            dataframe = transform_main(config['mongo_uri'], config['db_name'], config['collection_name'])

        load_logger.info(f"---> DF --->: {dataframe.columns}\n{dataframe.head(2)}")

        # Insert data into the database
        for index, row in dataframe.iterrows():
            record = WeatherData(**row.to_dict())
            session.add(record)
        session.commit()
        load_logger.info("Data loaded successfully.")

        
    except Exception as e:
        load_logger.error(f"Error loading data: {e}")
        session.rollback()
    finally:
        session.close()
        engine.dispose()
        load_logger.info("Database connection closed.")
