import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import pandas as pd
import datetime as dt

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

# Creates a session per request. Otherwise exception is thrown:
# "SQLite objects created in a thread can only be used in that same thread"
session = scoped_session(sessionmaker(bind=engine))

@app.teardown_request
def remove_session(ex=None):
    session.remove()

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/'start'<br/>"
        f"/api/v1.0/'start'/'end'<br/>"
    )

def row2dict(row):
    d = {}
    for column in row.__table__.columns:
        d[column.name] = str(getattr(row, column.name))
    return d

def parse_date(str):
    return dt.datetime.strptime(str, '%Y-%m-%d')

@app.route("/api/v1.0/precipitation")
def precipitation():
    """
    Convert the query results to a Dictionary using date as the key and tobs as the value.
    Return the JSON representation of your dictionary.
    """
    # Query 
    results = session.query(Measurement.date, Measurement.tobs).all()
    dict = {}
    for result in results:
        dict[result.date] = result.tobs

    return jsonify(dict)

@app.route("/api/v1.0/stations")
def stations():
    """
    Return a JSON list of stations from the dataset.
    """
    # Query 
    results = session.query(Measurement).all()
    list = []
    for result in results:
        list.append(row2dict(result))
    return jsonify(list)


@app.route("/api/v1.0/tobs")
def tobs():
    """
    query for the dates and temperature observations from a year from the last data point.
    Return a JSON list of Temperature Observations (tobs) for the previous year.
    """
    measurement_df = pd.DataFrame(engine.execute('SELECT * FROM measurement').fetchall()).drop(columns = [0])
    end_date = measurement_df.iloc[-1][2]
    date = parse_date(end_date)
    start_date = date - dt.timedelta(days=365)
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    list = []
    for result in results:
        list.append({'date':result.date, 'tobs': result.tobs})
    return jsonify(list)

@app.route("/api/v1.0/<start>", defaults={'end': None})
@app.route("/api/v1.0/<start>/<end>")
def start(start, end):
    """
    Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
    """
    query = session.query(func.min(Measurement.tobs).label("min"), \
                          func.avg(Measurement.tobs).label("avg"), \
                          func.max(Measurement.tobs).label("max"))\
                    .filter(Measurement.date >= parse_date(start))
    if end is not None:
        query = query.filter(Measurement.date <= parse_date(end))

    results = query.all()
    list = []
    for result in results:
        list.append({"TMIN": result.min, "TAVG": result.avg, "TMAX": result.max})
    return jsonify(list)
