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
    def __init__(self, data_path='data'):
        self.root = os.getcwd()

        # check for root/data directory
        self.data_path = os.path.join(self.root, data_path)
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

    def get_survey_df(self, verbose=False) -> pd.DataFrame:
        """get survey results, return pd.DataFrame"""
        res = pd.DataFrame()
        for entry in self._iterate():
            survey_path = os.path.join(entry.path, 'survey.csv')
    
            # skip if no survey
            if not os.path.exists(survey_path):
                if verbose:
                    print(f"Survey not found for: {entry.name}")
                continue

            # read survey
            entry_df = pd.read_csv(survey_path)
            res = pd.concat([res, entry_df], ignore_index=True)

            if verbose:
                print(f"Processed: {entry.name}")
                
        return res
    
    def _get_transcription(self, entry) -> tuple:
        """return audiophile, backbiter, cliffhanger as pd.DataFrame"""
        transcription_path = os.path.join(entry.path, 'transcription')

        # skip if no transcription
        if not os.path.exists(transcription_path) or not os.path.isdir(transcription_path):
            return None, None, None

        # read transcription
        audiophile_df = pd.read_csv(os.path.join(transcription_path, 'transcript_audiophile.csv'))
        backbiter_df = pd.read_csv(os.path.join(transcription_path, 'transcript_backbiter.csv'))
        cliffhanger_df = pd.read_csv(os.path.join(transcription_path, 'transcript_cliffhanger.csv'))
        return audiophile_df, backbiter_df, cliffhanger_df

    def get_transcriptions_info(self, verbose=False) -> pd.DataFrame:
        """get number of turns, average gaps between turns, average length of turns, 
        average words per turn, number of questions asked, number of turns ending with a question,
        number of overlaps, grouped by user"""

        cols = [
            'conversation_id', 'type', 'user_id', 'num_turns', 'avg_gap_between_turns', 
            'avg_length_of_turns', 'avg_words_per_turn', 'num_questions_asked', 
            'num_turns_ending_with_question', 'num_overlaps'
        ]
        res = pd.DataFrame(columns=cols)
        for entry in self._iterate():
            zipper = zip(['audiophile', 'backbiter', 'cliffhanger'], self._get_transcription(entry))
            for t, df in zipper:
                if df is None:
                    if verbose:
                        print(f"Transcription not found for: {entry.name} - {t}")
                    continue

                # group by user_id
                grouped = df.groupby('speaker')
                for user_id, group in grouped:
                    num_turns = len(group)
                    avg_gap_between_turns = group['interval'].mean()
                    avg_length_of_turns = group['delta'].mean()
                    avg_words_per_turn = group['n_words'].mean()
                    num_questions_asked = group['questions'].sum()
                    num_turns_ending_with_question = group['end_question'].sum()
                    num_overlaps = group['overlap'].sum()

                    row = {
                        'conversation_id': entry.name,
                        'type': t,
                        'user_id': user_id,
                        'num_turns': num_turns,
                        'avg_gap_between_turns': avg_gap_between_turns,
                        'avg_length_of_turns': avg_length_of_turns,
                        'avg_words_per_turn': avg_words_per_turn,
                        'num_questions_asked': num_questions_asked,
                        'num_turns_ending_with_question': num_turns_ending_with_question,
                        'num_overlaps': num_overlaps
                    }

                    res = pd.concat([res, pd.DataFrame([row])], ignore_index=True)
    
        return res

if __name__ == "__main__":
    gatherer = Gatherer('temp')
    with time_perf("Metadata Gathering"): # ~ 1.5s
        data = gatherer.get_metadata_df()
    print(data.info())
