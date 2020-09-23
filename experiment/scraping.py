import csv
import re
import time
import logging
import pandas as pd
from db_util import *
from enum import Enum
from urllib.request import urlopen
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementNotInteractableException, NoSuchElementException

logging.basicConfig(filename='logs/development.log',level=logging.INFO)
logger = logging.getLogger(__name__)

# Chrome のオプションを設定する
options = webdriver.ChromeOptions()
options.add_argument('--headless')

# Selenium Server に接続する
driver = webdriver.Remote(
    command_executor='http://localhost:4444/wd/hub',
    desired_capabilities=options.to_capabilities(),
    options=options,
)


def get_table_id() -> int:
    table = driver.find_element_by_class_name('report')
    table_id = table.get_attribute('id')
    logger.info(f'table_id: {table_id}')

    return table_id


def get_table_contents(table_id: int) -> webdriver:
    table_contents = driver.find_element_by_xpath(
        f"//*[@id='{table_id}']/tbody")
    return table_contents


def get_tr_tag_num(table_contents: webdriver) -> int:
    '''
    tableの行数を取得するメソッド
    '''
    tr_tag_num = len(table_contents.find_elements_by_tag_name('tr'))
    return tr_tag_num


def get_th_tag_num(table_contents: webdriver) -> int:
    '''
    table headerの行数を取得するメソッド
    '''
    th_tag_num = len(table_contents.find_elements_by_tag_name('th'))
    return th_tag_num


def get_td_tag_num(tr_tag_num: int) -> int:
    '''
    tableの列数を取得するメソッド。
    先頭行はheaderになっていて正しく列数を取得できないので、最終行から取得するようにする
    '''
    table_contents = driver.find_element_by_xpath(
        f"//*[@id='{table_id}']/tbody/tr[{tr_tag_num}]")
    td_tag_num = len(table_contents.find_elements_by_tag_name('td'))

    return td_tag_num


def format_columns_to_df(columns: list):
    '''
    取得したlistからをdatagrameにして'$','()'など不要な文字列を削除する
    '''
    df = pd.DataFrame(columns).T
    df = df.replace({'\$': '', '\(': '', '\)': '', '\,': ''}, regex=True)
    return df


def get_statements_title_list():
    '''
    Financial Statementのタブをクリックして表示されるstatementの一覧を取得するメソッド
    '''
    statements_title_list = []
    for i in range(1, 8):
        try:
            statement = driver.find_element_by_id(f"r{i}")
            if statement.text:
                statement.click()
                statements_title_list += [statement.text]
        except NoSuchElementException as e:
            logger.warning(f'statement: {statement} ERROR: {e}')
            continue
    return statements_title_list


def get_statement_title():
    '''
    Financial Statementのタブをクリックして表示されるstatementから
    balance sheet, income statement, cash flowを表示させる<a>のテキストを取得する
    '''
    statements_title_list = get_statements_title_list()
    flag_dict = {'balance_sheet': False, 'income_statement': False, 'cash_flow': False}

    statements_url = []
    # バランスシート、貸借対照表
    balance_sheet_patterns = ['.*balance.*','.*financial ?position.*', '.*financial ?condition.*']
    # 損益計算書
    income_statement_patterns = ['.*of ?income.*', '.*of ?operation.*', '.*of ?earnings']
    # キャッシュフロー計算書
    cash_flow_patterns = ['.*cash ?flow.*']

    for patterns in [balance_sheet_patterns, income_statement_patterns, cash_flow_patterns]:
        for pattern in patterns:
            tmp_title_list = []
            for title in statements_title_list:
                if re.match(pattern, title, re.IGNORECASE):
                    tmp_title_list += [title]
                    logger.info(f'{title} matched the pattern: {pattern}')
            if len(tmp_title_list) == 1:
                statements_url += tmp_title_list
            elif len(tmp_title_list) > 1:
                title = min(tmp_title_list)
                statements_url += [title]
            elif len(tmp_title_list) == 0:
                continue
    if len(statements_url) != 3:
        logger.warning(f'{url}')
        logger.warning(f'The length of statements_url is {len(statements_url)}')
        for title in statements_title_list:
            logger.warning(title + '')
    logger.info(f'THE RESULT IS... {statements_url}')

    return statements_url


if __name__ == '__main__':
    source_df = pd.read_csv('./data/2019_QTR4.csv')
    source_df = source_df[source_df['Form_Type'] == '10-Q']

    for _, row in source_df.iterrows():
        cik = str(row['CIK'])
        accession_number = row['accession_number']

        # Financial Statementsタブを開いて財務三表を表示させる
        url = f'https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession_number}&xbrl_type=v#'
        logger.info(f'{url} + の情報を取得します ({_} / {len(source_df)})')
        driver.get(url)
        try:
            driver.find_element_by_xpath(
                "//*[text()='Financial Statements']").click()
        except NoSuchElementException:
            sleep(5)
            driver.find_element_by_xpath(
                "//*[text()='Financial Statements']").click()


        statements_title_list = get_statement_title()

        for title in statements_title_list:
            try:
                driver.find_element_by_xpath(f"//*[text()='{title}']").click()
                table_id = get_table_id()
                table_contents = get_table_contents(table_id)
                tr_tag_num = get_tr_tag_num(table_contents)
                td_tag_num = get_td_tag_num(tr_tag_num)

                columns_list = [[] for i in range(td_tag_num)]

                # headerの数を取得する
                th_tag_num = get_th_tag_num(table_contents)
                for tr in range(th_tag_num, tr_tag_num + 1):
                    for td in range(1, td_tag_num + 1):
                        try:
                            value = driver.find_element_by_xpath(
                                f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]").text
                        except:
                            value = driver.find_element_by_xpath(
                                f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]/a").text
                        columns_list[td-1] += [value]

                table_df = format_columns_to_df(columns_list)
                row_df = table_df.iloc[:, 0:2].to_csv(
                    f"./{cik}_{title.replace(' ', '_')}")

            except Exception as e:
                logger.warning(f'title: {title} ERROR: {e}')


                sql = f'''insert into blance_sheets {'(' + ','.join(row_df.iloc[0,:].astype(str).to_list()) + ')'}
                          values {'(' + ','.join(row_df.iloc[1,:].astype(str).to_list()) + ')'}'''
        logger.info('-'*140)
    # ブラウザを終了する
    driver.quit()
