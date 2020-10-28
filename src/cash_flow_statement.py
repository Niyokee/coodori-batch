from financial_statement import FinancialStatement
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
