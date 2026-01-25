import os
import json
import pandas as pd
import sys

class Gatherer:
    def __init__(self):
        self.root = os.getcwd()

        # check for root/data directory
        self.data_path = os.path.join(self.root, 'temp')
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"'data' directory not found in {self.root}")
    
    def _iterate(self, action, verbose = False) -> pd.DataFrame:
        res = pd.DataFrame()
        for entry in os.scandir(self.data_path):
            if (
                not entry.is_dir()
                or len(entry.name) != 36
            ):
                continue

            if verbose:            
                print(f"Processing directory: {entry.name}")

            try:
                if action:
                    res = pd.concat([res, action(entry)], ignore_index=True)
            except Exception as e:
                print(f"Error processing {entry.name}: {e}")
                
        return res

    def get_metadata_df(self, verbose) -> pd.DataFrame:

        def _action(entry):
            metadata_path = os.path.join(entry.path, 'metadata.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                return pd.DataFrame([metadata])
            return pd.DataFrame()
    
        return self._iterate(_action, verbose)
        

if __name__ == "__main__":
    gatherer = Gatherer()
    data = gatherer.get_metadata_df(verbose=True)
    print(data.info())