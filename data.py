import numpy as np
import pandas as pd
import dask.dataframe as dd
import sys
import os


def load_NEMSIS(key:str, data_dir: os.PathLike, load_targets: dict):
    
    data_dict = {}
    for f in load_targets.keys():
        data = dd.read_csv(os.path.join(data_dir, f + ".txt"), delimiter="~\|~", engine="python", blocksize=25e6)

        # Remove quotes from headers
        data.columns = [col.strip("'") for col in data.columns]

        if load_targets[f] is not None:
            data = data[load_targets[f]]

        data.set_index(key)

        data_dict[f] = data

    return data_dict
