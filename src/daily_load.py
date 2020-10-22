import os
import re
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
import urllib.request
import requests
import db_util


class Idx():
    EST = timezone(timedelta(hours=-4), 'EST')

    def __init__(self):
        self.dt_now = datetime.now(Idx.EST)
        self.base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
        self._idx_file()

    def year(self):
        """現在の年をyyyyの形式で取得する

        Returns:
            str: year
        """
        return str(self.dt_now.strftime('%Y'))

    def month(self):
        """現在の月をmmの形式で取得する

        Returns:
            str: month
        """
        return str(self.dt_now.strftime('%m'))

    def day(self):
        """現在の1日前をddの形式で取得する

        Returns:
            str: day
        """
        return str(self.dt_now.day - 1)

    def _current_quater(self):
        """現在の四半期を取得する

        Returns:
            str: quater
        """
        month = int(self.month())
        if 1 <= month and month <= 3:
            return '1'
        elif 4 <= month and month <= 6:
            return '2'
        elif 7 <= month and month <= 9:
            return '3'
        elif 10 <= month and month <= 12:
            return '4'

    def _target_url(self):
        """idxの置かれているパスを作成する

        Returns:
            str: path
        """
        return self.base_url + self.year() + '/QTR' + self._current_quater() + '/' + 'company.' + self.year() + self.month() + self.day() + '.idx'

    def _idx_file(self):
        """"idxファイルを取得する
        """
        url = self._target_url()
        self.download_path = './' + self.day() + '.txt'
        try:
            urllib.request.urlretrieve(url, self.download_path)
        except:
            sys.exit()

    def _preprocess_idx(self, line):
        """idxの行をトリミングする

        Args:
            line array: idxの一行

        Returns:
            array: 2文字以上の空白と、改行文字を削除したidxの一行
        """
        return [re.sub(r'^ ', '', i) for i in line.split('  ') if not (i == '' or '\n' in i)]

    def _trim_idx(self):
        """idxファイルをトリミングする
        """
        with open(self.download_path, 'r', encoding='utf-8', newline='') as f:
            return f.readlines()

    def _row_df(self):
        """dfを作成する

        Returns:
            df: 'company_name', 'form_type','cik', 'date_filed', 'base_url'を含むdf
        """
        df = pd.DataFrame()
        for line in self._trim_idx():
            row = self._preprocess_idx(line)
            if len(row) == 5:
                df = df.append(pd.Series(row), ignore_index=True)
        df.columns = ['company_name', 'form_type',
                      'cik', 'date_filed', 'base_url']
        return df

    def df(self):
        """DBに格納するように整形する

        Returns:
            df: df
        """
        df = self._row_df()
        df['base_url'] = '/Archives/' + \
            df['base_url'].str.replace('-', '').replace('.txt', '')
        df['year'] = int(self.year())
        df['QT'] = 'QTR' + self._current_quater()
        df[df['form_type'].isin(['10-Q', '10-Q', '8-K', 'S-1'])]
        df.reindex(columns=['cik', 'company_name',
                            'form_type', 'date_filed', 'base_url', 'year', 'QT'])
        return df


if __name__ == '__main__':
    i = Idx()
    df = i.df()
    db_util.DBUtil.insertDf(df, 'base_info')
    os.remove(i.download_path)
