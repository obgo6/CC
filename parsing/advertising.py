import os
import pandas as pd
from parsing import setting
from parsing.mysql import Mysql
from parsing.file_read import Read
import re
import inspect


class Advertising(object):
    def __init__(self, log):
        self.mysql = Mysql(setting.PRODUCT_BASE)
        self.read = Read()
        self.log = log
        self.columns = setting.columns
        self.sum_columns = setting.sun_columns
        self.file_url = setting.file_url
        self.mysql.execute(setting.PRODUCT_SQL)
        self.product_code_info = self.mysql.data_frame()
        self.log.info("Advertising---初始化完成")

    def str_dispose(self, txt):
        re_txt = r'[;,；=，]|[+_ ]?[\-VIDEvideSB]{3,4}|[ ][A-Z\-]'
        txt = txt.replace(' new(1)', '').replace('-EAN', '').replace('AMS_Ads_', '').replace('-ASIN', '')
        txt = re.sub(r'[Brand \-]{6}|Video[ \-]|-201217', "", txt)
        txt = re.split(re_txt, txt)[0]
        if " " in txt and ' new' not in txt:
            start_txt = re.findall(r'[A-Z\-]{2,10}', txt)
            end_txt = re.findall('[0-9]{3}', txt)
            if start_txt:
                txt = '+'.join([start_txt[0] + i for i in end_txt])
        return txt

    def read_append(self, type_table='DAVH', report_column_mapping=None):
        df = pd.DataFrame(columns=["SKU"])
        file_list = [file_name for file_name in os.listdir(self.file_url) if file_name[0] in type_table]
        self.log.info(f'开始读取广告表，广告表文件数{len(file_list)}')
        for file_name in file_list:
            dfs = self.read.read_table(file_name, skip_rows=0)
            if dfs.shape[0] > 0:
                if file_name[0] in 'DA':
                    df_columns = {"Advertised SKU": 'SKU', 'Spend': 'spend'}
                else:
                    df_columns = {"Campaign Name": 'SKU', 'Spend': 'spend'}
                dfs.rename(columns=df_columns, inplace=True)
                dfs['account'] = file_name.split("-")[1]
                dfs['country_region'] = file_name[1:3]
                dfs.rename(columns=report_column_mapping, inplace=True)
                df = pd.concat([df, dfs])
                self.log.info(f'{file_name}读取完成, shape{dfs.shape}')
            else:
                self.log.warning(f'空文件{file_name}, shape{dfs.shape}')
        self.log.info(f'广告数据读取完成, 总数据{df.shape}')
        return df

    def split(self, df):
        self.log.info("----处理异常字符---")
        df['spend_c'] = df['spend']
        df['SKU'] = df['SKU'].apply(self.str_dispose)
        df.reset_index(drop=True, inplace=True)
        df['SKU'] = df['SKU'].map(lambda x: x.split('+'))
        df = df.explode('SKU')
        return df

    def treating(self, df, columns=None):
        df = self.split(df)
        if not columns:
            columns = 'spend'
        self.log.info("----均摊到遍体---")
        for index in df.index:
            if type(df.loc[index, 'SKU']) != str:
                df.loc[index, columns] = df.loc[index, columns] / df.loc[index, 'SKU'].count()
            else:
                df.loc[index, columns] = df.loc[index, columns]
        df.reset_index(drop=True, inplace=True)
        return df

    def bool_shape(self, shape, df):
        f_name = inspect.getframeinfo(inspect.currentframe().f_back)[2]
        if shape == df.shape[0] and df[df['model_no'].notnull()].shape[0] == shape:
            self.log.info(f"数据完整--{df.shape}--{f_name}")
        else:
            self.log.error(f"数据不完整--{df.shape}--{f_name}")

    def merge_product(self, df):
        self.product_code_info.drop_duplicates('SKU', inplace=True)
        df = pd.merge(df, self.product_code_info[['SKU', 'model_no']], left_on=['SKU'], right_on=["SKU"], how='left')
        dfs = df[df[['model_no']].isnull().T.any()].drop(columns='model_no')
        product_code_info = self.product_code_info[['model_no']]
        dfs = pd.merge(dfs, product_code_info.drop_duplicates('model_no'), left_on=['SKU'], right_on=["model_no"], how='left')
        df.loc[df[df[['model_no']].isnull().T.any()].index] = dfs.values
        for index, value in df[df[['model_no']].isnull().T.any()].iterrows():
            sku = value['SKU']
            if sku[0] in "ABUF" or 'SW' == sku[:2]:
                like = '"{0}%{1}%"'.format(sku[0], re.findall(r'\d+', sku)[0])
            elif 'SW' != sku[:2] and sku[0] == "S":
                like = '"%{0}{1}%"'.format('S', re.findall(r'\d+', sku)[0])
            elif '-' in sku:
                like = f"'%{sku.split('-')[-1]}%'"
            else:
                like = f'"%{sku}%"'
            sql = 'SELECT model_no FROM product_code_info where SKU like {0} limit 1'.format(like)
            txt = self.mysql.execute(sql).fetchall()
            df.loc[index, 'model_no'] = txt[0][0] if txt else ''
        return df

    def group_by(self, df):
        """
        合并到关系表
        :param df:
        :return: df
        """
        shape = df.shape[0]
        df = self.merge_product(df)
        self.bool_shape(shape, df)
        df['model_no'] = df['model_no'].apply(lambda x: x.replace("	", '').replace("-New", ''))
        df = df.groupby(['account', 'country_region', 'model_no']).agg({'spend': 'sum'})
        df.reset_index(inplace=True)
        return df

    def run(self):
        df = self.read_append()
        df = self.treating(df[['SKU', 'spend', 'account', 'country_region']])
        df = self.group_by(df)
        return df


class AdvertisingSingle(Advertising):
    def __init__(self, log):
        super().__init__(log)
        self._df = None
        self.advert_sum_columns = setting.ADVERT_SUM_COLUMNS

    def sum_agg(self, columns):
        agg_sum = dict()
        for i in columns:
            agg_sum[i] = "sum"
        return agg_sum

    def ad_advert(self, processing=None, type_table=None):
        report_column_mapping = processing.report_column_mapping
        df = self.read_append(type_table, report_column_mapping)
        df.columns = df.columns.map(lambda x: x.replace(" ", '_'))
        self.product_code_info.drop_duplicates('SKU', inplace=True)
        self._df = pd.merge(df, self.product_code_info[["SKU", "model_no"]], left_on=['SKU'], right_on=['SKU'], how='left')
        column = self.advert_sum_columns[type_table] if type_table == "A" else self.advert_sum_columns['other']
        column_group_by = ['country_region', 'account', 'model_no', 'asin']
        df = self.func(self.sum_agg(column), column_group_by)
        return df

    def func(self, sum_agg, column_group_by):
        self._df = self._df[self._df["spend"] != 0]
        df_gr = self._df.groupby(column_group_by).agg(sum_agg)
        df_gr.reset_index(inplace=True)
        df_gr['cr'] = df_gr['units'] / df_gr['clicks']
        df_gr['ctr'] = df_gr['clicks'] / df_gr['impressions']
        df_gr['cpc'] = df_gr['spend'] / df_gr['clicks']
        return df_gr

    def hv_advert(self, processing=None, type_table=None):
        report_column_mapping = processing.report_column_mapping
        df = self.read_append(type_table, report_column_mapping)
        column = self.advert_sum_columns[type_table] if type_table == "A" else self.advert_sum_columns['other']
        df = self.treating(df, column)
        self._df = self.merge_product(df)
        self._df.columns = self._df.columns.map(lambda x: x.replace(" ", '_'))
        column_group_by = ['country_region', 'account', 'model_no']
        df = self.func(self.sum_agg(column), column_group_by)
        return df
