import re
import time
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pandas.io.sql as psql
import db_util
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2 as pg
import datetime


class FinancialStatement:
    """
      財務諸表の各属性値や、各属性値を取得するためのメソッドを持つ

      Attributes
      ----------
      url : str
          該当の財務諸表が表示されるHTMLのURL
      soup : beautifulsoup
          該当の財務諸表のHTMLのbeautifulsoup
      contextref : stg
         財務諸表の期間を保持するID
      conn : psycopg2
         DBの接続情報を保持する
    """

    def __init__(self, base_url, cik):
        self.base_url = base_url
        self.cik = cik
        self.url = self.path_to_xml()
        self.soup = self.get_soup()
        self.year = self.fisical_year()
        self.quater = self.fisical_quater()
        self.document_period_end_date = self.document_period_end_date()

    def path_to_xml(self):
        """
        財務諸表のあるxmlのパスを取得するメソッド

        Paramters
        ---------
        None

        Returns
        -------
        path_to_xml : str
          base_url + ba-20101231.xml
          FIXME: 後半部分は Ticker + end period + .xmlになっているので、tickerをDBにもてたら修正したい
        """
        base_url = self.base_url
        url = 'https://www.sec.gov' + base_url + '/index.json'
        url = url.replace('.txt', '')
        res = requests.get(url).json()
        time.sleep(2)
        items = [item['name'] for item in res['directory']
                 ['item'] if re.search(r'.*.xml', item['name'])]
        item = [item for item in items if not re.search(
            r'R\d.*.xml|defnref.xml|.*_cal.xml|.*_def.xml|.*_lab.xml|.*_pre.xml|FilingSummary.xml', item)]
        if len(item) == 1:
            path_to_xml = 'https://sec.gov' + base_url + '/' + item[0]
        else:
            path_to_xml = 'https://sec.gov' + base_url + '/' + item[0]
            print(item)
        return path_to_xml

    def get_soup(self):
        """
        urlからリクエストしたページのhtmlをBeautifulSoupオブジェクトに変換する

        Parameters
        ----------
        None

        Returns
        -------
        soup : beautifulsoup
            レスポンスのHTMLから変換されたBeautifulSoupオブジェクト
        """
        res = requests.get(self.url)
        soup = BeautifulSoup(res.text, 'lxml')
        return soup

    def fisical_year(self):
        """
        財務諸表の対象の期間を取得する

        Parameters
        ----------
        None

        Returns
        -------
        str : year
        """
        tag = 'dei:' + 'DocumentFiscalYearFocus'.lower()
        year = self.soup.find(tag).get_text()
        return year

    def fisical_quater(self):
        """
        財務諸表の対象の期間を取得する

        Parameters
        ----------
        None

        Returns
        -------
        str : quater
            10-K : FY, 10-Q : 1, 2, 3, 4
        """
        tag = 'dei:' + 'DocumentFiscalPeriodFocus'.lower()
        self.contextref = self.soup.find(tag)['contextref']
        quater = self.soup.find(tag).get_text()
        return quater

    def start_date(self, contextref):
        """
          財務諸表の対象期間の開始日を取得する

          Parameters
          ----------
          contextref : str
              財務諸表の対象期間を表すcontextref

          Returns
          -------
          return: start_date : str
              財務諸表の対象期間の開始日
        """
        start_date = self.soup.find(id=contextref).find('startdate').get_text()
        return start_date

    def end_date(self, contextref):
        """
          財務諸表の対象期間の終了日を取得する

          Parameters
          ----------
          contextref : str
              財務諸表の対象期間を表すcontextref

          Returns
          -------
          return: end_date : str
              財務諸表の対象期間の終了日
        """
        try:
            end_date = self.soup.find(id=contextref).find(
                re.compile('enddate')).get_text()
        except AttributeError:
            end_date = self.soup.find(id=contextref).find(
                re.compile('instant')).get_text()
        finally:
            pass
        return end_date

    def document_period_end_date(self):
        tag = 'dei:' + 'DocumentPeriodEndDate'.lower()
        end_date = self.soup.find(tag).get_text()
        return end_date

    def get_value(self, tag_name):
        tags = self.soup.find_all(tag_name.lower())
        value_list = [tag.get_text()
                      for tag in tags if self.contextref == tag['contextref']]
        value = 0.0 if len(value_list) == 0 else value_list[0]
        return value

    def make_df(self):
        tmp_dict = self.__dict__
        try:
            del tmp_dict['soup'], tmp_dict['contextref'], tmp_dict['item_table'], tmp_dict['url']
        except KeyError:
            pass
        df = pd.DataFrame.from_dict(tmp_dict, orient='index').T
        df = df.replace({'quater': {'FY': 'Q4'}})
        try:
            # https://www.sec.gov/Archives/edgar/data/937098/000093709820000145/tnet-033120x10q_htm.xml
            df['year'] = pd.to_datetime(df['year'], format='%Y')
        except ValueError:
            df['year'] = None
        df['document_period_end_date'] = pd.to_datetime(
            df['document_period_end_date'], format='%Y-%m-%d')
        df['created_at'] = datetime.datetime.now()
        return df


class CashflowStatement(FinancialStatement):
    def __init__(self, base_url, cik):
        super().__init__(base_url, cik)
        self.item_table = 'cash_flow_item'
        self.cash_from_operating_activities = self.get_cash_from_operating_activities()
        self.cash_from_investing_activities = self.get_cash_from_investing_activities()
        self.cash_from_financial_activities = self.get_cash_from_financial_activities()
        self.cash = self.get_cash()

    def get_cash_from_operating_activities(self):
        tag = self.get_tag(1)
        cash_from_operating_activities = int(float(self.get_value(tag)))
        if cash_from_operating_activities == 0.0:
            tag = self.get_tag(4)
            cash_from_operating_activities = int(float(self.get_value(tag)))
        return cash_from_operating_activities

    def get_cash_from_investing_activities(self):
        tag = self.get_tag(2)
        cash_from_investing_activities = int(float(self.get_value(tag)))
        if cash_from_investing_activities == 0.0:
            tag = self.get_tag(5)
            cash_from_investing_activities = int(float(self.get_value(tag)))
        return cash_from_investing_activities

    def get_cash_from_financial_activities(self):
        tag = self.get_tag(3)
        cash_from_financial_activities = int(float(self.get_value(tag)))
        if cash_from_financial_activities == 0.0:
            tag = self.get_tag(6)
            cash_from_financial_activities = int(float(self.get_value(tag)))
        return cash_from_financial_activities

    def get_cash(self):
        tag = self.get_tag(8)
        cash = int(float(self.get_value(tag)))
        if cash == 0.0:
            tag = self.get_tag(9)
            cash = int(float(self.get_value(tag)))
        return cash

