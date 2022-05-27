from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from parsing import setting


class Mysql(object):
    def __init__(self, database):
        self.result = None
        self.engine = create_engine(setting.database_urls + database)
        db_session = sessionmaker(bind=self.engine)
        self.session = db_session()

    def execute(self, sql):
        # 提交SQL语句
        self.result = self.session.execute(sql)
        return self.result

    def data_frame(self):
        df = pd.DataFrame(self.result.fetchall())
        return df

    def commit(self):
        # 提交数据
        self.session.commit()

    def close(self):
        self.session.close()


# if __name__ == '__main__':
#     mysql = Mysql(basic_crawler_data)
#     print(mysql.execute('SELECT * FROM bsr_correlation WHERE date_time="2022-05-06" LIMIT 10').fetchall())
#     mysql.close()