import pandas as pd
import ast
from pandas.errors import EmptyDataError
import json
from glob import glob

import os

import shutil

from json_flatten import flatten

import zipfile

import pathlib

def parse_path(path):

    PATH = pathlib.Path(path)

    return PATH.parent, PATH.name, PATH.stem, PATH.suffix #par, name, stem, suffix = parse_path(path)

def pattern_merge(pattern,drop_duplicates=True, move_to_folder='output'):

    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')

    merged = pd.DataFrame()

    paths = glob(pattern)

    paths.sort()
    oneFileSameOutput = False
    if len(paths) == 1:
        path = paths[0]
        if os.path.realpath(path) == os.path.realpath(output):
            oneFileSameOutput = True
            print('there is only one match with the same name as output, nothing else to merge.')
    for path in paths:
        try:
            ## Reading each file
            parent, fname, stem, suffix = parse_path(path)
            # print(f'Identified:¥t {path}')
            # Couldn't fetch employee and review info in one go. So this was my attempt to fetch them separately and merge them
            print(path)
            if suffix == '.txt':
                with open(path, 'r') as datafile:
                    file = datafile.read()
                    file = ast.literal_eval(file)
                    file = json.dumps(file)
                    data = json.loads(file)
                employee = pd.json_normalize(data, ["employees"])
                review = pd.json_normalize(data)
                df = pd.concat([review,employee], axis = 1)
                df.drop(['employees'], axis = 1, inplace = True)
            else:
                print(f'suffix {suffix} not supported')
                print(path, 'read')
        except EmptyDataError:
            print(f'empty data @ {path}')

        #df = pd.DataFrame()
        merged = merged.append(df)

    if len(merged) == 0:

        print(f'pattern merge returned no records. len(merged) == {len(merged)}')

    return merged

all_sales = pattern_merge('reviews/all_pages_scarped?????.txt', drop_duplicates=True, move_to_folder='output')

all_sales.to_csv('output/output.csv', index=False)