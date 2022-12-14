import numpy as np
import pandas as pd
import dask.dataframe as dd
import sys
import os


def load_NEMSIS_dask(key:str, data_dir: os.PathLike, load_targets: dict):
    
    data_dict = {}

    for f in load_targets.keys():
        if f == "FACTPCRMEDICATION":
            data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=85e6, sample=25000000, dtype={"'eMedications_10'": 'object', "'eMedications_03'": 'object'})

        if f == "FACTPCRPROCEDURE":
            data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=85e6, sample=25000000, dtype={"'eProcedures_10'": 'object', "'eProcedures_03'": 'object'})

        if f == "FACTPCRPROTOCOL":
            data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=85e6, sample=25000000, dtype={"'eProtocol_01'": 'object'})

        if f == "Pub_PCRevents":
            data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=85e6, sample=25000000, dtype={"'eDisposition_23'": 'object', "'eResponse_07'":'object', "'eArrest_11'":'object'})

        else:
            data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=85e6, sample=25000000)
            
        # Remove quotes from headers
        data.columns = [col.strip("'") for col in data.columns]

        if load_targets[f] is not None:
            data = data[load_targets[f]]

        data.set_index(key)

        data_dict[f] = data

    return data_dict

def load_NEMSIS_pandas(data_dir:os.PathLike, load_targets:dict, nrows:int=500000):

    data_dict = {}

    for f in load_targets.keys():
        data = pd.read_csv(os.path.join(data_dir, f + ".txt"), sep="~\|~", engine="python", nrows=nrows)

        # Remove quotes from headers
        data.columns = [col.strip("'") for col in data.columns]

        if load_targets[f] is not None:
            data = data[load_targets[f]]

        data = data.replace(to_replace='^\. $', value=pd.NA, regex=True)

        data_dict[f] = data

    return data_dict


def sort_NEMSIS (data_dir:os.PathLike, load_targets:dict):
    for f in load_targets.keys():
        pass


def chain_join(datadict:dict, key:str, start_table:str, type:str='left'):
    
    for t in datadict.keys():

        datadict[t] = datadict[t].set_index(key, append=True)

    main = datadict[start_table]

    to_join = [datadict[t] for t in datadict.keys() if t != start_table]

    joined_tables = main.join(to_join, how=type)

    return joined_tables