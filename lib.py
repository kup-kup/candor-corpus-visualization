import os
import json
import pandas as pd
import sys
from time import perf_counter

class time_perf:
    def __init__(self, name=None):
        self.name = name
        self.s = 0
    
    def __enter__(self):
        self.s = perf_counter()
    
    def __exit__(self, *_):
        if self.name:
            print(self.name, end=': ')
        print(perf_counter()-self.s)

class Gatherer:
    def __init__(self):
        self.root = os.getcwd()

        # check for root/data directory
        self.data_path = os.path.join(self.root, 'data')
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"'data' directory not found in {self.root}")
    
    def _iterate(self):
        """main method to iterate through all conversations folders"""
        for entry in os.scandir(self.data_path):
            if not entry.is_dir() or len(entry.name) != 36:
                continue
            yield entry

    def get_metadata_df(self, verbose=False) -> pd.DataFrame:
        """get metadata, return pd.DataFrame"""
        res = pd.DataFrame()
        for entry in self._iterate():
            metadata_path = os.path.join(entry.path, 'metadata.json')

            # skip if no metadata
            if not os.path.exists(metadata_path):
                if verbose:
                    print(f"Metadata not found for: {entry.name}")
                continue

            # read metadata
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            entry_df = pd.DataFrame([metadata])
            res = pd.concat([res, entry_df], ignore_index=True)

            if verbose:
                print(f"Processed: {entry.name}")
    
        return res

if __name__ == "__main__":
    gatherer = Gatherer()
    with time_perf("Metadata Gathering"): # ~ 1.5s
        data = gatherer.get_metadata_df()
    print(data.info())
    '''dfsdfsfdsfdsfdsfs'''
