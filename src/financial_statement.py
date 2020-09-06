import re
import sys
import traceback
from datetime import datetime
import requests
import logging
import pandas as pd
import numpy as np
from db_util import *
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2 as pg


class FinancialStatement:
    BASE_URL = "https://www.sec.gov"

    def __init__(self, path):
        self.path = path

    def _xml_summary(self, path):
        path_to_xml_summary = FinancialStatement.BASE_URL + path
        content = requests.get(path_to_xml_summary).json()
        item_name = [item['name'] for item in content['directory']
                     ['item'] if item['name'] == 'FilingSummary.xml']
        # Grab the filing summary and create a new url leading to the file so we can download it
        xml_summary = FinancialStatement.BASE_URL + \
            content['directory']['name'] + "/" + item_name[0]

        return xml_summary

    def report_list(self):
        xml_summary = self._xml_summary(self.path)
        base_url = xml_summary.replace('FilingSummary.xml', '')
        content = requests.get(xml_summary).content
        soup = BeautifulSoup(content, 'lxml')
        # find the 'myreports' tag because this contains all the individual reports submitted.
        reports = soup.find('myreports')
        # a list to store all the individual components of the report
        report_list = []
        # loop through each report in the 'myreports' tag but avoid the last one as this will cause an error.
        for report in reports.find_all('report')[:-1]:
            # let's create a dictionary to store all the different parts we need.
            report_dict = {'name_short': report.shortname.text,
                           'name_long': report.longname.text}
            try:
                report_dict['url'] = base_url + report.htmlfilename.text
            except AttributeError:
                report_dict['url'] = base_url + report.xmlfilename.text
            # append the dictionary to the master list.
            report_list.append(report_dict)

        return report_list

    def statements_dict(self, report_list):
        # create the list to hold the statement urls
        statements_dict = []

        for report_dict in report_list:
            if re.match('paren', report_dict['name_short'], re.IGNORECASE):
                continue
            # バランスシート
            balance_sheet_patterns = [
                '.*balance.*', '.*financial ?position.*', '.*financial ?condition.*']
            # 損益計算書
            income_statement_patterns = [
                '.*of ?income.*', '.*of ?operation.*', '.*of ?earnings']
            # キャッシュフロー計算書
            cash_flow_patterns = ['.*cash ?flow.*']
            # report_list = [balontinue
            statements_dict = {}
            for index, patterns in enumerate([balance_sheet_patterns, income_statement_patterns, cash_flow_patterns]):
                for pattern in patterns:
                    tmp_title_dict = {}
                    for report_dict in report_list:
                        if re.match(pattern, report_dict['name_short'], re.IGNORECASE):
                            key = f"({index}){report_dict['name_short']}"
                            tmp_title_dict[key] = report_dict['url']
                    if len(tmp_title_dict) == 1:
                        statements_dict.update(tmp_title_dict)
                    elif len(tmp_title_dict) > 1:
                        tmp_dict = {min(tmp_title_dict): tmp_title_dict[min(tmp_title_dict)]}
                        statements_dict.update(tmp_dict)
                    elif len(tmp_title_dict) == 0:
                        continue

        return statements_dict

    def statements_data(self, statement_name, statement_url):
        # let's assume we want all the statements in a single data set.
        statements_data = []
        # define a dictionary that will store the different parts of the statement.
        statement_data = {'statement_name': statement_name,
                          'headers': [], 'sections': [], 'data': []}

        # request the statement file content
        logging.info(
            f'statement_name is {statement_name} statement_url is {statement_url}')
        content = requests.get(statement_url).content
        report_soup = BeautifulSoup(content, 'html')

        first_row = report_soup.table.find_all('tr')[0].get_text()
        # find all the rows, figure out what type of row it is, parse the elements, and store in the statement file list.
        for index, row in enumerate(report_soup.table.find_all('tr')):

            # first let's get all the elements.
            cols = row.find_all('td')

            # if it's a regular row and not a section or a table header
            reg_row = []
            if (len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0):
                try:
                    for ele in cols:
                        if ele.get_text():
                            reg_row += [ele.find('td').text]
                        else:
                            reg_row += ['Nan']
                except AttributeError:
                    reg_row = [ele.text.strip() for ele in cols]
                statement_data['data'].append(reg_row)

            # if it's a regular row and a section but not a table header
            elif (len(row.find_all('th')) == 0 and len(row.find_all('strong')) != 0):
                sec_row = cols[0].text.strip()
                statement_data['sections'].append(sec_row)

            # finally if it's not any of those it must be a header
            elif (len(row.find_all('th')) != 0):
                hed_row = [ele.text.strip() for ele in row.find_all('th')]
                statement_data['headers'].append(hed_row)

            else:
                logging.info('We encountered an error.')

        # append it to the master list.
        statements_data.append(statement_data)

        return statements_data

    def denomination(self):
        if re.search(f'.*thousand*', self.statements_data[0]['headers'][0][0], re.IGNORECASE):
            return 1000
        elif re.search(f'.*million*', self.statements_data[0]['headers'][0][0], re.IGNORECASE):
            return 1000000
        else:
            return 1

    def income_header(self):
        try:
            income_header = self.statements_data[0]['headers'][1]
        except IndexError:
            income_header = self.statements_data[0]['headers'][0]
            income_header = [
                element for element in income_header if not '$' in element]
        return income_header
        # income_data = statements_data[0]['data']

    def trim_value(self):
        income_data = self.statements_data[0]['data']
        income_df = pd.DataFrame(income_data)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index = income_df[0]
        income_df.index.name = 'category'
        income_df = income_df.drop(0, axis=1)
        # Get rid of the '$', '(', ')', and convert the '' to NaNs.
        income_df = income_df.replace('[\$,)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('', 'NaN', regex=True)
        # everything is a string, so let's convert all the data to a float.
        try:
            income_df = income_df.astype(float)
        except:
            print(self.statement_url)
        # Change the column headers
        income_df.columns = self.header
        column_list = []
        for column in self.header:
            column_list += [datetime.strptime(
                column.replace('.', ''), "%b %d, %Y")]
        column_index = column_list.index(max(column_list))
        latest_column_name = self.header[column_index]
        values = income_df[latest_column_name]
        if type(income_df[latest_column_name]) != type(pd.Series(1)):
            values = income_df[latest_column_name].iloc[:, 0]

        return values

    def is_same_element(self, value_list):
        """
        取得したlistが全て同じ要素で構成されているか確かめる
        """
        return value_list == [value_list[0]] * len(value_list) if value_list else False

    def trim_index(self, index_list):
        """正規表現 + 内包for文で取得したindexのリストを整形するメソッド
        　　　　同一の値で構成される index_list が len(index_list) > 1 の時 len(index_list) = 1 にする
        """
        if len(set(index_list)) == 1:
            return list(set(index_list))[0]
        else:
            # FIXME index_listが同一の値で構成されていないときの処理
            return index_list[0]

    def find_category_with_regex(self, pattern):
        """正規表現に一致するリストを返す
        """
        return [category for category in self.values.index.values if re.search(pattern, category, re.IGNORECASE)]


class BalanceSheet(FinancialStatement):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.statements_data = self.statements_data(
            self.statement_name, self.statement_url)
        self.header = self.income_header()
        self.value = self.trim_value


class ProfitLoss(FinancialStatement):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.statements_data = self.statements_data(
            self.statement_name, self.statement_url)
        self.header = self.income_header()
        self.value = self.trim_value

    def get_number_of_stocks(self):
        pass

    def get_diluted_eps(self):
        pass

    def get_sales(self):
        pass


class CashFlow(FinancialStatement):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.statements_data = self.statements_data(
            self.statement_name, self.statement_url)
        self.header = self.income_header()
        self.values = self.trim_value()

    def get_operating_activities_value(self):
        """item名=operating activitiesのindexを取得して、その値を返すメソッド
        """
        indices_income_from_operation = self.find_category_with_regex(
            'operating activities')
        index_income_from_operation = self.trim_index(
            indices_income_from_operation)
        try:
            cash_from_operating_activities = self.values[index_income_from_operation].unique()[
                0] * self.denomination()
        except AttributeError:
            cash_from_operating_activities = self.values[index_income_from_operation] * self.denomination(
            )

        return cash_from_operating_activities

    def get_financing_activities_value(self):
        """item名=financing activitiesのindexを取得して、その値を返すメソッド
        """
        indices_income_from_financing = self.find_category_with_regex(
            'financing activities')
        index_income_from_financing = self.trim_index(
            indices_income_from_financing)
        try:
            cash_from_financing_activities = self.values[index_income_from_financing].unique()[
                0] * self.denomination()
        except AttributeError:
            cash_from_financing_activities = self.values[index_income_from_financing] * self.denomination(
            )
        return cash_from_financing_activities

    def get_cash_beginning(self):
        """item名=cash beginging of periodのindexを取得して、その値を返すメソッド
        """
        indices_cash_beginning = self.find_category_with_regex('beginning of')
        index_income_cash_beginning = self.trim_index(indices_cash_beginning)
        try:
            cash_beginning = self.values[index_income_cash_beginning].unique()[
                0] * self.denomination()
        except AttributeError:
            cash_beginning = self.values[index_income_cash_beginning] * \
                self.denomination()
        return cash_beginning

    def get_cash_end(self):
        """item名=item名=cash end of periodのindexを取得して、その値を返すメソッド
        """
        indices_cash_end = self.find_category_with_regex('end of')
        index_income_cash_end = self.trim_index(indices_cash_end)
        try:
            cash_end = self.values[index_income_cash_end].unique()[
                0] * self.denomination()
        except AttributeError:
            cash_end = self.values[index_income_cash_end] * self.denomination()
        return cash_end

    def get_investing_activities_value(self):
        """item名=investing activitiesのindexを取得して、その値を返すメソッド
        """
        indices_income_from_investing = self.find_category_with_regex(
            'investing activities')
        index_income_from_investing = self.trim_index(
            indices_income_from_investing)
        try:
            cash_from_investing_activities = self.values[index_income_from_investing].unique()[
                0] * self.denomination()
        except AttributeError:
            cash_from_investing_activities = self.values[index_income_from_investing] * self.denomination(
            )
        return cash_from_investing_activities

    def _make_df(self):
        return pd.DataFrame({'id': None,
                             'operating_activities': [self.get_operating_activities_value()],
                             'financing_activities': [self.get_financing_activities_value()],
                             'investing_activities': [self.get_investing_activities_value()],
                             'cash_beginning_of_period': [self.get_cash_beginning()],
                             'cash_end_of_period': [self.get_cash_end()],
                             'cik': [self.cik],
                             'year': [self.year],
                             'quater': [self.quater],
                             'form_type': [self.form_type],
                             'created_at': [datetime.now().strftime('%Y-%m-%d  %H:%M:%S')],
                             'source': [self.statement_url]
                             })

    def insert_df(self):
        DBUtil.insertDf(self._make_df(), 'cash_flow',
                        if_exists="append", index=False)


if __name__ == '__main__':
    start_year = 2018
    end_year = 2019
    start_quarter = 1
    end_quarter = 2
    form_type = '10-K'
    # start_year = int(os.environ['start_year'])
    # end_year = int(os.environ['end_year'])
    # start_quarter = int(os.environ['start_quarter'])
    # end_quarter = int(os.environ['end_quarter'])
    # form_type = os.environ['form_type']

    for year in range(start_year, end_year):
        for quater in range(start_quarter, end_quarter):
            source_df = pd.read_csv(f'../data/{year}_QTR{quater}.csv')
            source_df = source_df[source_df['Form_Type'] == form_type]
            for _, row in source_df.iterrows():
                try:
                    logging.info(row)
                    FinancialStatement.cik = str(row['CIK'])
                    FinancialStatement.year = year
                    FinancialStatement.quater = quater
                    FinancialStatement.form_type = form_type
                    financial_statement = FinancialStatement(str(row['url']))
                    report_list = financial_statement.report_list()
                    statements_dict = financial_statement.statements_dict(
                        report_list)

                    for statement_name, statement_url in statements_dict.items():
                        if '(0)' in statement_name:
                            balance_sheet = BalanceSheet(
                                statement_name=statement_name, statement_url=statement_url)
                        elif '(1)' in statement_name:
                            profit_loss = ProfitLoss(
                                statement_name=statement_name, statement_url=statement_url)
                        elif '(2)' in statement_name:
                            cash_flow = CashFlow(
                                statement_name=statement_name, statement_url=statement_url)
                            cash_flow.insert_df()
                except BaseException as e:
                    logging.error(e)
                    logging.error(row)
