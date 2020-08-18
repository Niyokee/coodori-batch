import re
import logging
import pandas as pd
from db_util import *
from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2 as pg
from logging import getLogger, StreamHandler, DEBUG
from gensim.models import Word2Vec
from IPython import embed




def get_category(statement_name, cik, year, quater):
  sql = f'''SELECT category FROM {statement_name}
             WHERE cik = '{cik}' and
                   year = {year} and
                   quater = {quater}
          '''
  category_df = psql.read_sql(sql, DBUtil.getConnect())
  return category_df

def get_cik(statement_name):
  sql = f'''SELECT cik from {statement_name}
             GROUP BY cik
         '''
  cik_df = psql.read_sql(sql, DBUtil.getConnect())
  return cik_df

def format_df_to_list(df):
    text = df['category'].to_string(index=False)
    split_text = re.sub('\n {2,}', '==', text.lower())
    split_text = split_text.replace('\n', '').replace(' ', '_').replace('==', ' ')
    split_text = re.sub('_{2,}', '' , split_text)
    categories = split_text.split(' ')

    return categories


if __name__ == '__main__':
  statement_name = 'balance_sheets'
  print(f'{statement_name}')
  cik_df = get_cik(statement_name)
  category_list = []
  for _, row in cik_df.iloc[0:30].iterrows():
    cik = str(row['cik'])
    print(f'{cik}')
    for year in range(2019, 2020):
      for quater in range(1,5):
          print(f'FY:{year}/ quater: {quater}')
          category_df = get_category(statement_name, cik, year, quater)
          # if category_df:
          #   continue
          categories = format_df_to_list(category_df)
          category_list.append(categories)

  model = Word2Vec(category_list, sg=1, size=100, window=5, min_count=1)
  embed()
