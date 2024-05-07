import pandas as pd

class DataStorage:
    def __init__(self, path):
        self.path = path

    def save_data(self, data, schema):
        df = pd.DataFrame(data)
        df.to_excel(self.path, index=False)

    def load_data(self, schema):
        return pd.read_excel(self.path)
