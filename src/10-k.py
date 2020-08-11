import re
import os
from datetime import datetime
import requests
import logging
import pandas as pd
from db_util import *
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2 as pg
from logging import getLogger, StreamHandler, DEBUG


def setup_logger(name, logfile='log.txt'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(name)s - %(funcName)s - %(message)s')
    fh.setFormatter(fh_formatter)

    # create console handler with a INFO log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
    ch.setFormatter(ch_formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = setup_logger(__name__)


def __get_xml_summary(path_to_file):
    base_url = "https://www.sec.gov"
    # convert a normal url to a document url
    normal_url = base_url + '/Archives/' + path_to_file
    normal_url = normal_url.replace('-', '').replace('.txt', '/index.json')
    logger.info(f'{normal_url}の情報を取得します')
    # request the url and decode it.
    content = requests.get(normal_url).json()

    for file in content['directory']['item']:

        # Grab the filing summary and create a new url leading to the file so we can download it.
        if file['name'] == 'FilingSummary.xml':

            xml_summary = base_url + \
                content['directory']['name'] + "/" + file['name']
            logger.info('File Path: ' + xml_summary)

    return xml_summary


def __get_report_dict(xml_summary):
    # define a new base url that represents the filing folder.
    base_url = xml_summary.replace('FilingSummary.xml', '')
    # request and parse the content
    content = requests.get(xml_summary).content
    soup = BeautifulSoup(content, 'lxml')
    # find the 'myreports' tag because this contains all the individual reports submitted.
    reports = soup.find('myreports')
    # a list to store all the individual components of the report
    master_reports = []
    # loop through each report in the 'myreports' tag but avoid the last one as this will cause an error.
    for report in reports.find_all('report')[:-1]:
        # let's create a dictionary to store all the different parts we need.
        report_dict = {}
        report_dict['name_short'] = report.shortname.text
        report_dict['name_long'] = report.longname.text
        try:
            report_dict['url'] = base_url + report.htmlfilename.text
        except AttributeError:
            report_dict['url'] = base_url + report.xmlfilename.text

        # append the dictionary to the master list.
        master_reports.append(report_dict)

    return master_reports


def __get_statements_dict(master_reports):
    # create the list to hold the statement urls
    statements_dict = []

    for report_dict in master_reports:
        if re.match('paren', report_dict['name_short'], re.IGNORECASE):
            logger.info(f'{report_dict} contains "paren"')
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
                for report_dict in master_reports:
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


def __get_statements_data(statements_dict):
    # let's assume we want all the statements in a single data set.
    statements_data = []

    # loop through each statement url
    for statement_name, statement_url in statements_dict.items():

        # define a dictionary that will store the different parts of the statement.
        statement_data = {}
        statement_data['statement_name'] = statement_name
        statement_data['headers'] = []
        statement_data['sections'] = []
        statement_data['data'] = []

        # request the statement file content
        logger.info(statement_url)
        content = requests.get(statement_url).content
        report_soup = BeautifulSoup(content, 'html')

        # find all the rows, figure out what type of row it is, parse the elements, and store in the statement file list.
        for index, row in enumerate(report_soup.table.find_all('tr')):

            # first let's get all the elements.
            cols = row.find_all('td')

            # if it's a regular row and not a section or a table header
            reg_row = []
            if (len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0):
                try:
                    for ele in cols:
                        # if ele.find('td'):
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
                logger.info('We encountered an error.')

        # append it to the master list.
        statements_data.append(statement_data)

    return statements_data


def __convert_data_to_df(statements_data, cik, year, q, form_type):
    for i in range(len(statements_data)):
        try:
            income_header = statements_data[i]['headers'][1]
        except IndexError:
            income_header = statements_data[i]['headers'][0]
            income_header = [
                element for element in income_header if not '$' in element]
        income_data = statements_data[i]['data']

        # Put the data in a DataFrame
        income_df = pd.DataFrame(income_data)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index = income_df[0]
        income_df.index.name = 'Category'
        income_df = income_df.drop(0, axis=1)
        # Get rid of the '$', '(', ')', and convert the '' to NaNs.
        income_df = income_df.replace('[\$,)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('', 'NaN', regex=True)
        # everything is a string, so let's convert all the data to a float.
        income_df = income_df.astype(float)
        # Change the column headers
        income_df.columns = income_header
        column_list = []
        for column in income_header:
            dateFormatter = "%b. %d, %Y"
            column_list += [datetime.strptime(column, dateFormatter)]
        column_index = column_list.index(max(column_list))
        latest_column_name = income_header[column_index]

        category_df = pd.DataFrame()
        created_at = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
        category_df['category'] = income_df.reset_index()['Category']
        category_df['value'] = income_df[latest_column_name].values.tolist()
        category_df['cik'] = cik
        category_df['year'] = year
        category_df['quater'] = q
        category_df['form_type'] = form_type
        category_df['statement_name'] = statements_data[i]['statement_name']
        category_df['created_at'] = created_at

        if category_df['statement_name'].str.contains('(0)').all():
            category_df = category_df.replace('^\(\d\)', '', regex=True)
            DBUtil.insertDf(category_df, 'balance_sheets',
                            if_exists="append", index=False)
        elif category_df['statement_name'].str.contains('(1)').all():
            category_df = category_df.replace('^\(\d\)', '', regex=True)
            DBUtil.insertDf(category_df, 'profit_loss',
                            if_exists="append", index=False)
        elif category_df['statement_name'].str.contains('(2)').all():
            category_df = category_df.replace('^\(\d\)', '', regex=True)
            DBUtil.insertDf(category_df, 'cash_flow',
                            if_exists="append", index=False)


if __name__ == "__main__":
    start_year = int(os.environ['start_year'])
    end_year = int(os.environ['end_year'])
    start_quarter = int(os.environ['start_quarter'])
    end_quarter = int(os.environ['end_quarter'])
    form_type = os.environ['form_type']

    for year in range(start_year, end_year):
        for q in range(start_quarter, end_quarter):
            source_df = pd.read_csv(f'./data/{year}_QTR{q}.csv')
            source_df = source_df[source_df['Form_Type'] == form_type]
            for _, row in source_df.iterrows():
                try:
                    cik = str(row['CIK'])
                    logger.info(row)
                    logger.info(f"year: {year}, quater: {q}, cik: {cik}")
                    path_to_file = str(row['Filename'])
                    xml_summary = __get_xml_summary(path_to_file)
                    master_reports = __get_report_dict(xml_summary)
                    statements_dict = __get_statements_dict(master_reports)
                    statements_data = __get_statements_data(statements_dict)
                    __convert_data_to_df(
                        statements_data, cik, year, q, form_type)
                except BaseException as e:
                    logger.error(f'{e}')
