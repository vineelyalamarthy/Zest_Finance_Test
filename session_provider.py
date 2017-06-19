#!/usr/bin/python
"""
Script to help create DBSessions to be able to run queries against DB.
"""
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

session = ''


def get_db_session():
    # Switch to better way of achieving singletons.
    global session
    # TODO(vyalamarthy): Read these from config object and use .format API to get the formatted strings.
    engine = create_engine('mysql+mysqldb://root:root@localhost/zest_la_test', echo=True)
    session = Session(engine)
