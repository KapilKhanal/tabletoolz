from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base


engine = create_engine("sqlite:///sql/databases/Car_Database.db")

Base = automap_base()
Base.prepare(engine, reflect=True)

# Tables from Car database
brands = Base.classes.Brands
car_options = Base.classes.Car_Options
car_vins = Base.classes.Car_Vins
customer_ownership = Base.classes.Customer_Ownership
dealers = Base.classes.Dealers
manufacture_plant = Base.classes.Manufacture_Plant
models = Base.classes.Models