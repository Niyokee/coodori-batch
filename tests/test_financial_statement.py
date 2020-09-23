import sys
import os
sys.path.append('../')
from src.xbrl import FinancialStatement


def test_init():
  url = 'https://www.sec.gov/Archives/edgar/data/320193/000032019319000119/a10-k20199282019_htm.xml'
  financial_statement = FinancialStatement(url)