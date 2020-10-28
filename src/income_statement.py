from financial_statement import FinancialStatement
import os
import re
import time
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pandas.io.sql as psql
import db_util
from bs4 import BeautifulSoup
import pandas.io.sql as psql
import psycopg2 as pg
import datetime


class IncomeStatement(FinancialStatement):
    def __init__(self, base_url, cik):
        super().__init__(base_url, cik)
        self.revenues = self.get_revenues()
        self.cost_of_revenues = self.get_cost_of_revenues()
        self.cost_of_goods_sold = self.get_cost_of_goods_sold()
        self.gross_profit = self.get_gross_profit()
        self.operating_expences = self.get_operating_expences()
        self.operating_income = self.get_operating_income()
        self.nonoperating_income_expense = self.get_nonoperating_income_expense()
        self.income_before_tax = self.get_income_before_tax()
        self.depreciation_and_amortization = self.get_depreciation_and_amortization()
        self.interest = self.get_interest()
        self.net_income = self.get_net_income()
        self.dividend = self.get_dividend()
        self.eps = self.get_eps()
        self.shares_outstanding = self.get_shares_outstanding()

    def get_fact(self, df, is_float=False):
        try:
            facts = [float(self.get_value('us-gaap:' + tag)) for tag in df['tag']
                     if not float(self.get_value('us-gaap:' + tag)) == 0.0]
            if not is_float:
                facts = list(map(lambda x: int(x), facts))
        except BaseException:
            return 0.0
        if len(facts) == 0:
            return 0.0
        return facts[0]

    def get_tag_df(self, id):
        sql = f'select tag from labels where parent_id = {id}'
        return psql.read_sql(sql, db_util.DBUtil.getConnect())

    def get_revenues(self):
        """売上: Revenue

        Tags:
           - Revenues
           - RevenueFromContractWithCustomerExcludingAssessedTax
           - RevenueFromContractWithCustomerIncludingAssessedTax

        Returns:
            int: 売上
        """
        tag_df = self.get_tag_df(1)
        revenues = self.get_fact(tag_df)
        if revenues == 0.0:
            tag_df = self.get_tag_df(2)
            revenues = self.get_fact(tag_df)
        return revenues

    def get_cost_of_revenues(self):
        """売上コスト: Cost of Revenue

        Formula:
            Cost of Goods and Searvice + Other Costs

        Tags:
            CostOfRevenue

        Returns:
            int: 売上コスト
        """
        tag_df = self.get_tag_df(6)
        cost_of_revenues = self.get_fact(tag_df)
        return cost_of_revenues

    def get_gross_profit(self):
        """売上総利益: Gross Profit

        Formula:
            Revenue - Cost of Revenue

        Tags:
           GrossProfit

        Returns:
            int: 売上総利益
        """
        tag_df = self.get_tag_df(4)
        gross_profit = self.get_fact(tag_df)
        return gross_profit

    def get_cost_of_goods_sold(self):
        """売上原価: Cost of Goods Sold

        Returns:
            int: 売上原価
        """
        tag_df = self.get_tag_df(3)
        cost_of_goods_sold = self.get_fact(tag_df)
        return cost_of_goods_sold

    def get_operating_expences(self):
        """営業経費: Operating Expences

        Tags:
            Operating Expences

        Returns:
            int: 営業経費
        """
        tag_df = self.get_tag_df(7)
        operating_expences = self.get_fact(tag_df)
        return operating_expences

    def get_operating_income(self):
        """営業損益: Operating Income(Loss)

        Formula:
            Gross Profit - Operating Expences + Other Operating Income (Expense), Net

        Tags:
            OperatingIncomeLoss

        Returns:
            int: 営業利益
        """
        tag_df = self.get_tag_df(8)
        operating_income = self.get_fact(tag_df)
        return operating_income

    def get_nonoperating_income_expense(self):
        """営業外損益: Non Operating Income(Loss)

        Tags:
            NonoperatingIncomeExpense

        Returns:
            int: 営業外損益
        """
        tag_df = self.get_tag_df(9)
        nonoperating_income_expense = self.get_fact(tag_df)
        return nonoperating_income_expense

    def get_income_before_tax(self):
        """税金等調整前当期純利益: Income Loss Before Income Taxes

        Tags:
            IncomeLossFromContinuingOperationsBeforeIncomeTaxes

    Returns:
            int: 税金等調整前当期純利益
        """
        tag_df = self.get_tag_df(10)
        income_before_tax = self.get_fact(tag_df)
        return income_before_tax

    def get_net_income(self):
        """純利益: Net Income

        Tags:
            Net Income

        Returns:
            int: 純利益
        """
        tag_df = self.get_tag_df(11)
        net_income_loss = self.get_fact(tag_df)
        return net_income_loss

    def get_depreciation_and_amortization(self):
        """減価償却費: depreciation and amortization

        Returns:
            int: 減価償却費
        """
        tag_df = self.get_tag_df(15)
        interest = self.get_fact(tag_df)
        return interest

    def get_interest(self):
        """支払金利: Interest Expence

        Tags:
            InterestExpence

        Returns:
            int: 支払金利
        """
        tag_df = self.get_tag_df(16)
        interest = self.get_fact(tag_df)
        return interest

    def get_dividend(self):
        """一株当たり配当: Devedend

        Tags:
            CommonStockDividendsPerShareDeclared

        Returns:
            float: 配当
        """
        tag_df = self.get_tag_df(12)
        dividend = self.get_fact(tag_df, is_float=True)
        return dividend

    def get_eps(self):
        """一株あたり収益: Earngings Per Share

        Returns:
            float: 一株あたり収益
        """
        tag_df = self.get_tag_df(13)
        eps = self.get_fact(tag_df, is_float=True)
        return eps

    def get_shares_outstanding(self):
        """発行株式: Shares Outstanding

        Returns:
            int: shares outstanding
        """
        tag_df = self.get_tag_df(14)
        shares_outstanding = self.get_fact(tag_df)
        return shares_outstanding

    def main(self):
        df = self.make_df()
        db_util.DBUtil.insertDf(df, 'income_statements_2020_test')


if __name__ == "__main__":
    def ciks(year, quater):
        sql = f"""SELECT t.cik, b.name, b."instance"
                    FROM info as b
                    JOIN (SELECT cik FROM tickers) as t
                      ON t.cik = b.cik
                   WHERE b.fy = {year} and (b.form like '10-Q' or b.form = '10-K') and fp = 'Q{quater}'
                   GROUP BY t.cik, b.name, b."instance"
                   ORDER by t.cik DESC"""
        return pd.read_sql_query(sql, db_util.DBUtil.getConnect())

    for year, quater in [(year, quater) for year in [os.environ['year']] for quater in range(1, 5)]:
        for _, cik in ciks(year, quater).iterrows():
            try:
                print(cik['cik'])
                IncomeStatement(cik['base_url'], cik['cik']).main()
            except KeyError as e:
                print(e)
