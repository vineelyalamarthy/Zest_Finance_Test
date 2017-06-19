#!/usr/bin/python
# coding: utf-8
from sqlalchemy import Column, Float, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import session_provider as sess

Base = declarative_base()
metadata = Base.metadata
sess.get_db_session()


class FactorWeight(Base):
    __tablename__ = 'factor_weights'

    id = Column(Integer, primary_key=True)
    chloroform_weight = Column(Float)
    bromoform_weight = Column(Float)
    bromodichloromethane_weight = Column(Float)
    dibromichloromethane_weight = Column(Float)


t_schema_migrations = Table(
    'schema_migrations', metadata,
    Column('version', String(255), nullable=False, unique=True)
)


class WaterSample(Base):
    __tablename__ = 'water_samples'

    id = Column(Integer, primary_key=True)
    site = Column(String(255))
    chloroform = Column(Float)
    bromoform = Column(Float)
    bromodichloromethane = Column(Float)
    dibromichloromethane = Column(Float)

    def find(self, target):
        db_session = sess.session
        result = db_session.query(WaterSample).filter_by(id=target).one()
        self.id = result.id
        self.site = result.site
        self.chloroform = result.chloroform
        self.bromoform = result.bromoform
        self.bromodichloromethane = result.bromodichloromethane
        self.dibromichloromethane = result.dibromichloromethane

    def factors(self, factor):
        db_session = sess.session
        weights = db_session.query(FactorWeight).filter_by(id=factor).one()
        weights_data = [weights.chloroform_weight, weights.bromoform_weight, weights.bromodichloromethane_weight,
                        weights.dibromichloromethane_weight]
        pros = [self.chloroform, self.bromoform, self.bromodichloromethane, self.dibromichloromethane]
        sol = sum([x * y for x, y in zip(weights_data, pros)])
        print sol,
        return sol

    def to_hash(self, include_factors=False):
        db_session = sess.session
        # For both true and false, we need to add water sample's id and components.
        results = dict()
        for k, v in self.__dict__.iteritems():
            if not k.startswith('_'):
              results[k] = v

        if not include_factors:
            return results

        # if incude_factors is False, just return. Else get the list of factors and add them to the dict object.
        for row in db_session.query(FactorWeight):
            key = 'factor_' + str(row.id)
            results[key] = self.factors(row.id)
        return results


if __name__ == '__main__':
    # engine, suppose it has two tables 'user' and 'address' set up

    abc = WaterSample()
    abc.find(3)
    abc.factors(2)

