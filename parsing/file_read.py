import pandas as pd
import os
from parsing import setting


class Read(object):
    def __init__(self):
        self.file_url = setting.file_url

    def read_table(self, file_name, sheet_name=None, skip_rows=7):
        if '.xlsx' in file_name:
            df = pd.read_excel(os.path.join(self.file_url + file_name))
        elif'.csv' in file_name:
            try:
                df = pd.read_csv(os.path.join(self.file_url + file_name), skiprows=skip_rows, low_memory=False)
            except:
                df = pd.read_csv(os.path.join(self.file_url + file_name), skiprows=skip_rows,  encoding="gbk", low_memory=False)
        elif '.txt' in file_name:
            df = pd.read_csv(os.path.join(self.file_url + file_name), skiprows=skip_rows)
        else:
            df = pd.DataFrame(columns=setting.columns)
        return df

    def data_frame(self, data):
        df = pd.DataFrame(data)
        return df



if __name__ == '__main__':
    read = Read()
    print(read.read_table('PUS-ACHK-51W.csv', skip_rows=7))