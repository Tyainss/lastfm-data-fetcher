import pandas as pd
import os

class DataStorage:
    def __init__(self):
        pass

    def output_csv(self, path, df, append=False):
        if append:
            print('Adding to existing CSV to: ', path)
            df.to_csv(path, mode='a', index=False, header=False)
        else:
            print('New CSV to: ', path)
            df.to_csv(path, index=False)

    def read_excel(self, path, schema=None):
        print('Reading Excel from: ', path)
        df = pd.read_excel(path)
        if schema:
            # Convert DataFrame columns to the specified data types
            for column, dtype in schema.items():
                print('Column :', column, 'dtype :', dtype)
                df[column] = df[column].astype(dtype)
        
        return df

    def output_excel(self, path, df, schema=None, append=False):
        print('Outputting Excel to: ', path)
        if schema:
            # Convert DataFrame columns to the specified data types
            for column, dtype in schema.items():
                print('Column :', column, 'dtype :', dtype)
                df[column] = df[column].astype(dtype)
        
        if os.path.exists(path) and append:
            existing_df = self.read_excel(path=path, schema=schema)
            df = pd.concat([existing_df, df], ignore_index=True)
        
        df.to_excel(path, index=False)