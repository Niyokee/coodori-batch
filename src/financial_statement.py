import re
import requests
from db_util import DBUtil as db
from bs4 import BeautifulSoup


class FinancialStatement:
    """
      財務諸表の各属性値や、各属性値を取得するためのメソッドを持つ

      Attributes
      ----------
      base_url : str
          該当の財務諸表が表示されるXMLのURL
      soup : beautifulsoup
          該当の財務諸表のHTMLのbeautifulsoup
    """

    def __init__(self, base_url, cik):
        self.base_url = base_url
        self.cik = cik
        self.soup = self._soup()

    def _soup(self):
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
        path_to_xml = self._path_to_xml()
        res = requests.get(path_to_xml)
        soup = BeautifulSoup(res.text, 'lxml')
        return soup

    def _path_to_xml(self):
        """
        財務諸表のあるxmlのパスを取得するメソッド

        Paramters
        ---------
        None

        Returns
        -------
        path_to_xml : str xmlファイルのパス
        """
        url = 'https://www.sec.gov' + self.base_url + '/index.json'
        res = requests.get(url).json()
        xml = self._xml(res)
        if len(xml) == 1:
            path_to_xml = 'https://sec.gov' + self.base_url + '/' + xml[0]
        else:
            path_to_xml = 'https://sec.gov' + self.base_url + '/' + xml[0]
        return path_to_xml

    def _xml(self, json):
        """引数で渡されるjson
           ex) https://www.sec.gov/Archives/edgar/data/320193/000032019319000119/index.json
           には複数のファイルがある。このファイル群から目的のxmlファイルを取得する

        Args:
            json (str): 該当期間に提出されたファイル群のあるパス
        """
        items = [item['name'] for item in json['directory']
                 ['item'] if re.search(r'.*.xml', item['name'])]
        item = [item for item in items if not re.search(
            r'R\d.*.xml|defnref.xml|.*_cal.xml|.*_def.xml|.*_lab.xml|.*_pre.xml|FilingSummary.xml', item)]
        return item

    def _context_ref(self):
        """temporaryなcontextRef = context#idを取得する
           ここで取得したcontextrefを元に、start_date, end_dateを取得する

        Returns:
            str: temporaryなcontextRef = context#id
        """
        tag = 'dei:' + 'EntityCentralIndexKey'.lower()
        return self.soup.find(tag)['contextref']

    def context_refs(self):
        """該当期間のcontext_refを全て取得する

        Returns:
            array: contextRef = context#idの配列
        """
        start = self.start_date()
        end = self.end_date()
        return [ref['id'] for ref in self.soup('context') if all(el is not None for el in [ref.find('startdate', text=start), ref.find('enddate', text=end)])]

    def axis_name(self, ref):
        """contextのid = conrextRefに紐づくaxisを取得する

        Args:
            id (str): context id

        Returns:
            str: axis
        """
        try:
            return self.soup.find(id=ref).find('xbrldi:explicitmember')['dimension']
        except TypeError:
            return None

    def axis_id(self, ref):
        """axisの名前からidを取得するメソッド

        Args:
            axis (str): axisの名前
            example: 'us-gaap:StatementClassOfStockAxis'

        Returns:
            int: axis id
        """
        axis_name = self.axis_name(ref)
        if type(axis_name) != type('string'):
            return 0
        else:
            name = self._subtract_tag_prefex(axis_name)
            query = f"""SELECT id FROM axis
                         WHERE name = '{name}'; """
            return db.fetch_one(query)[0]

    def category_ids(self, tag_name):
        """tag_nameからprimary_idとsecondary_idを取得するメソッド

        Args:
            axis (str): tag_name
            example: 'us-gaap:revenuefromcontractwithcustomerexcludingassessedtax'

        Returns:
            dict: {prime_category_id: x, secondary_category_id: x}
        """
        name = self._subtract_tag_prefex(tag_name)
        print(name)
        query = f"""SELECT id, prime_category_id
                      FROM secondary_categories
                     WHERE name ilike '{name}'; """
        result = db.fetch_one(query)
        return {'prime_category_id': result[1], 'secondary_category_id': result[0]}

    def table(self, prime_category_id):
        query = f"""SELECT s.name FROM prime_categories p
                     INNER JOIN statements s
                        ON s.id = p.statement_id
                     WHERE p.id = '{prime_category_id}'
                 """
        return db.fetch_one(query)[0]

    def _subtract_tag_prefex(self, axis):
        return re.sub(r'^.*:', '', axis)

    def member(self, id):
        """contextのidに紐づくmemberを取得する

        Args:
            id (str): context id

        Returns:
            str: member
        """
        try:
            return self.soup.find(id=id).find('xbrldi:explicitmember').get_text()
        except:
            return ''

    def fiscal_year(self):
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

    def fiscal_quater(self):
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
        quater = self.soup.find(tag).get_text()
        return quater

    def start_date(self):
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
        context_ref = self._context_ref()
        return self.soup.find(id=context_ref).find('startdate').get_text()

    def end_date(self):
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
        context_ref = self._context_ref()
        return self.soup.find(id=context_ref).find('enddate').get_text()

    def tags(self, ref):
        """引数のcontextRef = context#id を持つタグを全て取得する

        Args:
            ref str: contextRef = context#id
            example:'FD2019Q4YTD_srt_ProductOrServiceAxis_us-gaap_ProductMember'

        Returns:
            Array: tagの配列
        """
        return self.soup.find_all(contextref=ref)

    def _is_int(self, tag):
        """insertする値がintegerかfloatか判別するメソッド

        Args:
            tag str: 判別するtag

        Returns:
            bool
        """
        try:
            decimals = int(tag['decimals'])
        except KeyError:
            return True
        if decimals < 0:
            return True
        else:
            return False

    def _parse_int(self, tag):
        return int(tag.get_text())

    def _parse_float(self, tag):
        return float(tag.get_text())

    def fact(self, tag):
        return self._parse_int(tag) if self._is_int(tag) else self._parse_float(tag)

    def insert_query_params(self, **kwargs):
        pc_id = category_ids['prime_category_id']
        sc_id = category_ids['secondary_category_id']
        ref = kwargs.get('ref')
        tag = kwargs.get('tag')
        params = {
            'table': self.table(pc_id),
            'cik': self.cik if self.cik else '',
            'pc_id': pc_id,
            'sc_id': sc_id,
            'axis_id': self.axis_id(ref) if self.axis_id(ref) else 0,
            'member': self.member(ref) if self.member(ref) else '',
            'fact': self.fact(tag) if self.fact(tag) else 0,
            'start_day': self.start_date(),
            'end_day': self.end_date(),
            'fy': self.fiscal_year() if self.fiscal_year() else '',
            'fq': self.fiscal_quater().replace('FY', 'Q4') if self.fiscal_quater else '',
            'base_url': f'https://www.sec.gov{self.base_url}'
        }
        return params

    def insert_query(self, params):
        return f"""INSERT INTO {params['table']}
                   (cik,
                    prime_category_id,
                    secondary_category_id,
                    axis_id,
                    member,
                    fact,
                    start_day,
                    end_day,
                    fy,
                    fq,
                    base_url,
                    created_at,
                    updated_at)
            VALUES ('{params['cik']}',
                    '{params['pc_id']}',
                    {params['sc_id']},
                    {params['axis_id']},
                    '{params['member']}',
                    {params['fact']},
                    '{params['start_day']}'::date,
                    '{params['end_day']}'::date,
                    {params['fy']},
                    '{params['fq']}',
                    '{params['base_url']}',
                    now(),
                    now()); """


if __name__ == '__main__':
    cik = '789019'
    conn = db.conn()
    cur = db.cursor(conn)
    query = f"select base_url from base_info where cik = '{cik}' and form_type <> '8-K'"
    urls = db.fetch_many(query)
    for base_url in urls:
        f = FinancialStatement(base_url[0], cik)
        for ref in f.context_refs():
            for tag in [tag for tag in f.tags(ref) if not tag.name.startswith('dei:')]:
                try:
                    category_ids = f.category_ids(tag.name)
                    if f.table(category_ids['prime_category_id']) == 'income_statements':
                        params = f.insert_query_params(ref=ref, cik=cik, tag=tag, category_ids=category_ids)
                        db.execute(cur, f.insert_query(params))
                except BaseException as e:
                    continue
        db.commit(conn)
    db.cur_close(cur)
