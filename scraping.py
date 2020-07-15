import csv
import re
import time
import pandas as pd
from enum import Enum
from urllib.request import urlopen
from selenium import webdriver
from selenium.webdriver.common.by import By
# Chrome のオプションを設定する
options = webdriver.ChromeOptions()
options.add_argument('--headless')

# Selenium Server に接続する
driver = webdriver.Remote(
    command_executor='http://localhost:4444/wd/hub',
    desired_capabilities=options.to_capabilities(),
    options=options,
)


def _get_table_id() -> int:
    table = driver.find_element_by_class_name('report')
    table_id = table.get_attribute('id')

    return table_id


def _get_table_contents(table_id: int) -> webdriver:
    table_contents = driver.find_element_by_xpath(
        f"//*[@id='{table_id}']/tbody")
    return table_contents


def get_tr_tag_num(table_contents: webdriver) -> int:
    '''
    tableの行数を取得するメソッド
    '''
    tr_tag_num = len(table_contents.find_elements_by_tag_name('tr'))
    return tr_tag_num


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


if __name__ == '__main__':
    source_df = pd.read_csv('./2019_QTR4.csv')
    source_df = source_df[source_df['Form_Type'] == '10-Q']

    for _, row in source_df.iterrows():
        if _ > 5:
            break
        cik = str(row['CIK'])
        accession_number = row['accession_number']

        # Financial Statementsタブを開いて財務三表を表示させる
        print(f'https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession_number}&xbrl_type=v#')
        driver.get(
            f'https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession_number}&xbrl_type=v#')
        driver.find_element_by_xpath("//*[@id='menu_cat2']").click()

        # {2:Balance Sheets}, {4: stetement of operations}, {6: statement of cash flow} テーブルを開くためのクリック
        for i in [2, 4, 6]:
            try:
                driver.find_element_by_id(f"r{i}").click()
            except:
                time.sleep(2)
                driver.find_element_by_id(f"r{i}").click()
            table_id = _get_table_id()
            table_contents = _get_table_contents(table_id)
            tr_tag_num = get_tr_tag_num(table_contents)
            td_tag_num = get_td_tag_num(tr_tag_num)

            columns_list = [[] for i in range(td_tag_num)]

            for tr in range(3, tr_tag_num + 1):
                for td in range(1, td_tag_num + 1):
                    try:
                        # 2 = BS 4 = Inconmeの並びじゃない場合もある
                        # balace sheet
                        if i == 2:
                            try:
                                value = driver.find_element_by_xpath(
                                    f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]").text
                            except:
                                value = driver.find_element_by_xpath(
                                    f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]/a").text
                        # operation
                        elif i == 4:
                            value = driver.find_element_by_xpath(
                                f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]").text
                        # cash flow
                        elif i == 6:
                            value = driver.find_element_by_xpath(
                                f"//*[@id='{table_id}']/tbody/tr[{tr}]/td[{td}]").text
                        columns_list[td-1] += [value]
                    except:
                        pass

            df = format_columns_to_df(columns_list)

            if i == 2:
                df.to_csv(f'./{cik}_balance_sheet.csv')
            elif i == 4:
                df.to_csv(f'./{cik}_operations.csv')
            elif i == 6:
                df.to_csv(f'./{cik}_cash_flow.csv')

    # ブラウザを終了する
    driver.quit()

