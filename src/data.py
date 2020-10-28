import os
import json
from tqdm import tqdm
import pandas as pd
import datetime
import requests
import db_util
from download import download
import zipfile


class TickerCikMap():
    base_url = 'https://www.sec.gov/files/company_tickers.json'

    def __init__(self):
        self.conn = db_util.DBUtil.getConnect()

    def reqest(self):
        res = requests.get(TickerCikMap.base_url)
        return res

    def str_to_dict(self, json_string):
        return json.loads(json_string)

    def dict_to_df(self, dict_):
        return pd.DataFrame.from_dict(dict_, orient='index')

    def new_ticker_df(self):
        res = self.reqest()
        dict_ = self.str_to_dict(res.text)
        df = self.dict_to_df(dict_)
        df = df.rename(columns={'cik_str': 'cik'})
        df['cik'] = df['cik'].astype(str)
        return df

    def existing_ticker_df(self):
        sql = 'select * from tickers'
        return pd.read_sql_query(sql, self.conn)

    def df_diff(self, df1, df2):
        return df1[~df1.isin(df2.to_dict(orient='list')).all(1)]


class Instance():
    def __init__(self, year, quater):
        self.year = year
        self.quater = quater
        self.path = f'../donwload/{self.year}q{self.quater}.zip'

    def download_path(self):
        url1 = f"https://www.sec.gov/files/node/add/data_distribution/{self.year}q{self.quater}.zip"
        url2 = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{self.year}q{self.quater}.zip"
        return [url1, url2]

    def download(self):
        url = self.download_path()
        try:
            download(url[0], self.path)
        except RuntimeError:
            download(url[1], self.path)

    def remove_download(self):
        os.remove(self.path)

    def download_to_df(self):
        self.download()
        zip_file = zipfile.ZipFile(self.path)
        df = pd.read_csv(zip_file.open('sub.txt'), sep='\t')
        df['cik'] = df['cik'].astype(str)
        df = df[df['form'].isin(
            ['10-Q', '10-Q', 'S-1', '10-Q/A', '10-Q/A', 'S-1/A'])]
        return df

    def main(self):
        df = self.download_to_df()
        db_util.DBUtil.insertDf(df, 'info')
        self.remove_download()


if __name__ == '__main__':
    for year, quater in tqdm([(year, quater) for year in range(2014, 2021) for quater in range(2, 5)]):
        Instance(year, quater).main()
        print('done!')
