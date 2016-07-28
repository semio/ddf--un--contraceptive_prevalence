# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file

# configuration of file paths
source = '../source/UNPD_WCU2015_CP_Country Data Survey-Based.xlsx'
out_dir = '../../'


def _fix_column_names(data):
    """There are 3 columns contains the column names.
    This function combine those columns and change the columns of dataframe.
    """
    for i in range(len(data.columns)):

        if not data.ix[0].isnull().ix[i]:
            name1 = data.iloc[0, i]

        else:
            name1 = ''

        if not data.ix[1].isnull().ix[i]:
            name = data.iloc[1, i] + ' ' + name1

        else:
            name = name1

        if name:
            data = data.rename(columns={data.columns[i]: name})

    data = data.rename(columns={'Male ': 'Male Sterilization'})

    return data.drop([0, 1])


def extract_entities_iso_code(data):
    country = data[['Country', 'ISO Code']]
    country = country.drop_duplicates().dropna().copy()
    country.columns = ['name', 'iso_code']
    country.iso_code = country.iso_code.map(int)

    return country


def extract_entities_age(data):
    age = data[['Age']].drop_duplicates().copy()
    age['age'] = age['Age'].map(to_concept_id)
    age = age.dropna()
    age.columns = ['name', 'age']

    return age


def extract_entities_method(data):
    # modern methods.
    modern = data.columns[6:15]
    # traditional methods
    trad = data.columns[16:]

    # combine into one dataframe
    methods = pd.DataFrame(np.r_[modern, trad], columns=['name'])
    methods['method'] = methods['name'].map(to_concept_id)

    methods = methods.set_index('name')

    methods['is--modern_method'] = False
    methods['is--traditional_method'] = False

    methods.loc[modern, 'is--modern_method'] = True
    methods.loc[trad, 'is--traditional_method'] = True

    methods = methods.reset_index()

    return methods


def extract_concepts():
    """manually create the concept dataframe"""
    cdf = pd.DataFrame([['name', 'Name', 'string'],
                        ['iso_code', 'ISO Code', 'entity_domain'],
                        ['method', 'Methods', 'entity_domain'],
                        ['modern_method', 'Modern methods', 'entity_set'],
                        ['traditional_method', 'Traditional methods', 'entity_set'],
                        ['age', 'Age', 'entity_domain'],
                        ['year', 'Year(s)', 'time'],
                        ['domain', 'Domain', 'string'],
                        ['contraceptive_prevalence', 'Contraceptive Prevalence', 'measure']
                        ], columns=['concept', 'name', 'concept_type'])

    # adding domain column
    cdf['domain'] = np.nan
    cdf.loc[[3, 4], 'domain'] = 'method'

    return cdf


def extract_datapoints(data):
    dps = data.drop(['Any method', 'Any modern method',
                     'Any traditional method', 'Country'], axis=1).copy()

    # change column name to alphanumeric
    dps.columns = list(map(to_concept_id, dps.columns))
    dps = dps.rename(columns={'year_s': 'year'})

    # combine all method info into 'method' column
    dps.iso_code = dps.iso_code.map(int)
    dps = dps.set_index(['iso_code', 'year', 'age'])
    dps = dps.stack().reset_index()
    dps.columns = ['iso_code', 'year', 'age', 'method', 'contraceptive_prevalence']

    return dps


if __name__ == '__main__':
    data = pd.read_excel(source, skiprows=3, na_values='..')
    data = _fix_column_names(data)

    # only keep columns needed
    data = data.ix[:, :19]

    print('creating concept files...')
    cdf = extract_concepts()
    path = os.path.join(out_dir, 'ddf--concepts.csv')
    cdf.to_csv(path, index=False)

    print('creating entities files...')
    # country
    iso_code = extract_entities_iso_code(data)
    path = os.path.join(out_dir, 'ddf--entities--iso_code.csv')
    iso_code.to_csv(path, index=False)

    # methods
    methods = extract_entities_method(data)
    path = os.path.join(out_dir, 'ddf--entities--method.csv')
    methods.to_csv(path, index=False)

    # ages
    age = extract_entities_age(data)
    path = os.path.join(out_dir, 'ddf--entities--age.csv')
    age.to_csv(path, index=False)

    print('creating datapoints files...')
    dps = extract_datapoints(data)
    path = os.path.join(out_dir,
                        'ddf--datapoints--contraceptive_prevalence--by--iso_code--age--year.csv')
    dps.to_csv(path, index=False)

    print('creating index file...')
    create_index_file(out_dir)
