#!/usr/bin/python
"""
Script to help create DBSessions to be able to run queries against DB.
"""
from sqlalchemy import Column, Float, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

session = ''
def get_db_session():
    global session
    engine = create_engine('mysql+mysqldb://root:root@localhost/zest_la_test', echo=True)
    session = Session(engine)
