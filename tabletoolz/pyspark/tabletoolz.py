# -*- coding: utf-8 -*-

"""Main module."""

def select(df,cols):
    '''Input: Dataframe as list of dictionary, each dictionary as a row and list of cols to select
       Output:list of dictionary, each dictionary as a row with selected column from cols '''
    for row in df:
        for k,v in row.items():
            if k in cols:
                yield({k:v})
