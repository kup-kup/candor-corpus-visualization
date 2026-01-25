import os
import json
import pandas as pd

class Gatherer:
    def __init__(self):
        self.root = os.getcwd()

        # check for root/data directory
        self.data_path = os.path.join(self.root, 'data')
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

            if action:
                res = pd.concat([res, pd.read_csv(os.path.join(entry.path, 'data.csv'))], ignore_index=True)
                
        return res

    def get_metadata(self) -> list[dict]:
        pass
        # metadata_file = os.path.join(self.data_path, 'metadata.json')
        # if not os.path.exists(metadata_file):
        #     raise FileNotFoundError(f"'metadata.json' not found in {self.data_path}")

        # with open(metadata_file, 'r') as f:
        #     metadata = json.load(f)
        
        # return metadata


if __name__ == "__main__":
    gatherer = Gatherer()
    print(gatherer._iterate(len))