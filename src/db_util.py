import os
import psycopg2 as pg
import logging
from sqlalchemy import create_engine

class DBUtil():
    @staticmethod
    def getConnect():
        conn = pg.connect(
            user=os.environ["DATABASE_USERNAME"],
            host=os.environ["DATABASE_HOST"],
            password=os.environ["DATABASE_PASSWORD"],
            port=os.environ["DATABASE_PORT"],
            dbname=os.environ["DATABASE_NAME"]
        )
        conn.set_client_encoding('utf-8')
        logging.info(conn)
        return conn

    @staticmethod
    def getEngine():
        engine = create_engine(
            "postgresql://" +
            os.environ["DATABASE_USERNAME"] +
            ":" +
            os.environ["DATABASE_PASSWORD"] +
            "@" +
            os.environ["DATABASE_HOST"] +
            ":" +
            os.environ["DATABASE_PORT"] +
            "/" +
            os.environ["DATABASE_NAME"] + "")
        return engine

    @staticmethod
    def insertDf(df, table_name, if_exists="append", index=False):
        """
        dfをinsertするメソッド

        Arguments:
        ----------
        df: dataframe
            DBにインサートしたいdataframe
        table_name: string
         dataframeを格納したいテーブル名
        if_exists: string
            データベースにデータが存在しているとき、appendするかreplaceするか選ぶ
            defaultはappend
        index: boolean
            dataframeのindexを格納するかどうか。
            defaultはFalse
        """
        try:
            engine = DBUtil.getEngine()
        except:
            sleep(5)
            engine = DBUtil.getEngine()
        df.to_sql(table_name, engine, if_exists=if_exists, index=index)
