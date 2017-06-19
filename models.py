#!/usr/bin/python
# coding: utf-8
from sqlalchemy import Column, Float, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc
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
        try:
            result = db_session.query(WaterSample).filter_by(id=target).one()
            self.id = result.id
            self.site = result.site
            self.chloroform = result.chloroform
            self.bromoform = result.bromoform
            self.bromodichloromethane = result.bromodichloromethane
            self.dibromichloromethane = result.dibromichloromethane
        except exc.SQLAlchemyError:
            # TODO : Use Logging
            print 'DB Error'


    def factors(self, factor):
        db_session = sess.session
        try:
            weights = db_session.query(FactorWeight).filter_by(id=factor).one()
            weights_data = [weights.chloroform_weight, weights.bromoform_weight, weights.bromodichloromethane_weight,
                            weights.dibromichloromethane_weight]
            components = [self.chloroform, self.bromoform, self.bromodichloromethane, self.dibromichloromethane]

            # Calculating linear combination of weights and components.
            sol = sum([x * y for x, y in zip(weights_data, components)])
            return sol
        except exc.SQLAlchemyError:
            print 'DB Error'
            return None

    def to_hash(self, include_factors=False):
        db_session = sess.session
        # For both true and false, we need to add water sample's id and components.
        results = dict()
        for k, v in self.__dict__.iteritems():
            if not k.startswith('_'):
                results[k] = v

        if not include_factors:
            return results

        # if include_factors is False, just return. Else get the list of factors and add them to the dict object.
        for row in db_session.query(FactorWeight):
            key = 'factor_' + str(row.id)
            results[key] = self.factors(row.id)
        return results


if __name__ == '__main__':

    # TODO (vyalamarthy): Use pyUnitest framework and Mock API to simulate DB data in test environment.

    water_sample = WaterSample()
    water_sample.find(2)
    assert str(water_sample.id) == '2'
    assert water_sample.site == 'North Hollywood Pump Station (well blend)'
    assert water_sample.chloroform == 0.00291
    assert water_sample.bromoform == 0.00487
    assert water_sample.dibromichloromethane == 0.0109
    assert water_sample.bromodichloromethane == 0.00547

    # Linear combination of factor weights
    assert water_sample.factors(2) == 0.02415

    results = water_sample.to_hash(include_factors=True)

    assert len(results) == 10

    # There are only four factor weight ids in DB.
    assert 'factor_1' in results.keys()
    assert 'factor_2' in results.keys()
    assert 'factor_3' in results.keys()
    assert 'factor_4' in results.keys()

    assert 'factor_5' not in results.keys()
    assert 'factor_6' not in results.keys()

    results = water_sample.to_hash(include_factors=False)

    # Since include_factors is False, none of the factors should NOT  be included in it.

    assert 'factor_1' not in results.keys()
    assert 'factor_2' not in results.keys()
    assert 'factor_3' not in results.keys()
    assert 'factor_4' not in results.keys()

    assert results['chloroform'] == 0.00291


