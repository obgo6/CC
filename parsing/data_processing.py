import os
import pandas as pd
from parsing import setting
from parsing.mysql import Mysql
from parsing.file_read import Read


class Processing(object):
    def __init__(self, log, product_code_info):
        self.mysql = Mysql(setting.BASIC_CRAWLER_DATA)
        self.read = Read()
        self.log = log
        self.columns = setting.columns
        self.sum_columns = setting.sun_columns
        self.agg_sum = self.sum_agg()
        self.file_url = setting.file_url
        self.mysql.execute(setting.SQl_TYPE_CORRELATION)
        self.type = self.mysql.data_frame()
        self.desc_list = self.type[self.type['description'] != "1"]['description']
        self.mysql.execute(setting.SQl_COLUMN_MAP)
        self.dic_df = self.mysql.data_frame()
        self.product_code_info = product_code_info
        self.log.info(self.__class__.__name__ + "---初始化---")
        self.report_column_mapping = self.report_column()

    def report_column(self):
        report_column_mapping = dict()
        for index, v in self.dic_df.iterrows():
            report_column_mapping[v['org_column_name']] = v['new_column_name']
        return report_column_mapping

    def sum_agg(self):
        agg_sum = dict()
        for i in self.sum_columns:
            agg_sum[i] = "sum"
        return agg_sum

    def append(self):
        df = pd.DataFrame(columns=self.columns)
        report_column = self.report_column()
        file_list = [file_name for file_name in os.listdir(self.file_url) if file_name[0] == 'P']
        self.log.info(f'开始读取P表，P表文件数{len(file_list)}')
        for file_name in file_list:
            dfs = self.read.read_table(file_name)
            try:
                dfs.rename(columns=report_column, inplace=True)
                if 't' not in dfs.columns[0]:
                    dfs = self.read.read_table(file_name, skip_rows=6)
                    dfs.rename(columns=report_column, inplace=True)
                if file_name[1:3] == "JP":
                    dfs['type'] = dfs['type'].apply(
                        lambda x: str(x).encode("gbk").decode("Shift_JIS") if type(x) == str else x)
                    dfs['description'] = dfs['description'].apply(lambda x: x.encode("gbk").decode("Shift_JIS"))
                dfs['Country'] = file_name[1:3]
                dfs['account'] = file_name.split("-")[1]
                self.log.info(f'{file_name}读取完成, shape{dfs.shape}')
                df = pd.concat([df, dfs])
            except AttributeError:
                self.log.error(f'异常文件{file_name}')
                continue
        df[self.sum_columns] = df[self.sum_columns].apply(format_float)
        df['description'] = df['description'].apply(lambda x: x.rstrip() if type(x) == str else x)
        self.log.info(f'文件读取完成，总数{df.shape}')
        return df[self.columns]

    def merge(self, df):
        self.log.info(f'合并财务关系表')
        df['description_copy'] = df['description'].apply(lambda x: x if x in self.desc_list else "1")
        df = pd.merge(df, self.type, left_on=['Country', 'type', 'description_copy'], right_on=['country', 'type', 'description'])
        return df

    def slice(self, df, df_copy):
        """
        统计二个表的差值
        :param df: pd.DataFrame
        :param df_copy: pd.DataFrame
        :return:
        """
        slice_lable = (
            df[['Country', 'type', 'description']].apply(tuple, axis=1).isin(
                df_copy[['Country', 'type', 'description']].apply(tuple, axis=1).to_list())
        )
        if df[~slice_lable].shape[0] > 0:
            self.log.warning(f'合并表异常{df[~slice_lable].shape}')
        return df[~slice_lable]

    def analytical(self, df) -> pd.DataFrame:
        self.log.info(f'汇总订单总额')
        dfs = df[df['country'] != "JP"]
        df.loc[dfs.index, '订单总额'] = dfs['product_sales'] + dfs['shipping_credits'] + dfs['gift_wrap_credits']
        dfs = df[df['country'] == "JP"]
        df.loc[dfs.index, '订单总额'] = dfs['product_sales'] + dfs['shipping_credits'] + dfs['gift_wrap_credits'] + dfs['points_fees'] + dfs['other']
        df['销售金额'] = df['订单总额'] + df['promotional_rebates']
        return df

    def refund_adjust(self, df, df_order):
        """
        退款
        :param df: pd.DataFrame
        :param df_order: pd.DataFrame
        :return:
        """
        # df_group_by = df[(df['transaction_type'].isin(["退款", '调整'])) & df['SKU'].notnull()].groupby(['SKU', 'transaction_type']).agg(self.agg_sum)
        df_group_by = df[(df['transaction_type'].isin(["退款", '调整'])) & df['SKU'].notnull()].groupby(['model_no', 'transaction_type']).agg(self.agg_sum)
        for index, df in df_group_by.iterrows():
            if "退款" in index[1]:
                df_order.loc[index[0], "退款"] = df[['product_sales', 'shipping_credits', 'points_fees', 'gift_wrap_credits', 'promotional_rebates']].sum()
                df_order.loc[index[0], 'marketplace_withheld_tax'] = df_order.loc[index[0], 'marketplace_withheld_tax'] + df['marketplace_withheld_tax']
                df_order.loc[index[0], 'product_sales_tax'] = df_order.loc[index[0], 'product_sales_tax'] + df['product_sales_tax']
            else:
                df_order.loc[index[0], index[1]] = df['total'] + df['other']
        return df_order

    def share_equally(self, df, df_order):
        """
        BU均摊计算
        :param df: pd.DataFrame
        :param df_order: pd.DataFrame
        :return: pd.DataFrame
        """
        str_list = []
        df_account = df[df[['SKU']].isnull().T.any()].groupby(['transaction_type']).agg(self.agg_sum)
        for index_columns in df_account.index:
            if index_columns == "Order" or index_columns == '调整':
                columns = "FBA运费"
            else:
                columns = index_columns
            str_list.append(
                f'{columns.replace("-", "_")}=销售金额*{float(df_account.loc[index_columns, "total"])}/销售金额.sum()')
        if str_list:
            df_order.eval('\n'.join(str_list), inplace=True)
        return df_order

    def run(self):
        df = self.append()
        df = self.merge(df)
        self.agg_sum['订单总额'] = 'sum'
        self.agg_sum['销售金额'] = 'sum'
        df = self.analytical(df)
        # 合并SKU的型号数据
        product_code_info_sku = self.product_code_info.drop_duplicates('SKU')
        df = pd.merge(df, product_code_info_sku[['SKU', 'model_no']], left_on='SKU', right_on="SKU", how='left')
        # 合并asin的数据
        dfs = df[df[['model_no']].isnull().T.any()].drop(columns='model_no')
        product_code_info_asin = self.product_code_info.drop_duplicates('asin')
        dfs = pd.merge(dfs, product_code_info_asin[['asin', 'model_no']], left_on='SKU', right_on="asin", how='left')
        dfs.drop(columns='asin', inplace=True)
        df.loc[df[df[['model_no']].isnull().T.any()].index] = dfs.values
        df.loc[df[df[['model_no']].isnull().T.any()].index, 'model_no'] = df.loc[df[df[['model_no']].isnull().T.any()].index, 'SKU']
        df_group_by = df.groupby(['country', 'account'])
        df_all = pd.DataFrame(columns=['country'])
        for index, df in df_group_by:
            df_order = df[(df['transaction_type'].isin(['Order', '其他-税费']) ) & df['SKU'].notnull()].groupby(['model_no']).agg(self.agg_sum)
            df_order = self.share_equally(df, df_order)
            df_order = self.refund_adjust(df, df_order)
            df_order[['country', 'account']] = index
            df_all = pd.concat([df_all, df_order])
        df_all.rename(columns=setting.relation, inplace=True)
        df_all.fillna(0, inplace=True)
        print(df_all.to_excel("汇总.xlsx"))
        # return df_all


def format_float(data):
    a_list = []
    for i in data:
        bb = str(i).replace("−", "-").replace("\u202f", "").replace("\xa0", "")
        a = float(bb.replace(",", "")) if "," in str(i) and "." in str(i) else float(bb.replace(",", "."))
        a_list.append(a)
    return a_list


