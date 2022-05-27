database_urls = 'mysql+pymysql://bison_operator:GV3!FrJB@rm-wz9mnt1nqmk6967p6yo.mysql.rds.aliyuncs.com:3306/'


# 关系表数据库
PRODUCT_BASE = "product"
PRODUCT_SQL = 'SELECT * FROM product_code_info'

# basic_crawler_data
BASIC_CRAWLER_DATA = 'basic_report'
# tpye 关系表
SQl_TYPE_CORRELATION = 'SELECT * FROM report_type_correlation'
SQl_COLUMN_MAP = 'SELECT * FROM report_column_map'

print('ADVERT_SUM_columns'.upper())


file_url = r'Z:\M\2022\M-04M\BOAHVDTGP\\'
# file_url = r'D:\18W\BOAHVDTGP\\'
columns = ['date_time', 'Country', 'account', 'description', 'fba_fees', 'fulfillment', 'gift_wrap_credits', 'giftwrap_credits_tax',
           'marketplace', 'marketplace_withheld_tax', 'order_city', 'order_id', 'order_postal', 'order_state',
           'other', 'other_transaction_fees', 'product_sales', 'product_sales_tax', 'promotional_rebates',
           'promotional_rebates_tax', 'quantity', 'selling_fees', 'settlement_id', 'shipping_credits',
           'shipping_credits_tax', 'SKU', 'tax_collection_model', 'total', 'type', 'points_fees']

agg = {'product_sales': "sum", 'fba_fees': "sum"}
a = 'sales tax collected'
sun_columns = ["fba_fees", 'quantity', "gift_wrap_credits", "giftwrap_credits_tax", "marketplace_withheld_tax", "other",
               "other_transaction_fees", "product_sales", "product_sales_tax", "promotional_rebates",
               "promotional_rebates_tax", "selling_fees", "shipping_credits", "shipping_credits_tax", "total", 'points_fees']

relation = {
    'Country': '国家C', 'type': '事务类型', 'description': '描述', 'tax_collection_model': '税收征管模式',
    'marketplace': '网站', 'quantity': '数量', 'product_sales': '产品收入', 'product_sales_tax': '代收销售税',
    'shipping_credits': '运费收入', 'shipping_credits_tax': '代收运费税', 'gift_wrap_credits': '包装收入',
    'giftwrap_credits_tax': '代收包装税', 'promotional_rebates': '销售折扣', 'promotional_rebates_tax': '代收销售折扣税',
    'sales tax collected': '代收营业税', 'marketplace_withheld_tax': '市场预扣税', 'selling fees': '佣金',
    'fba_fees': 'FBA配送费', 'points_fees': '积分费', 'other_transaction_fees': '其他交易费用', 'other': '其他',
    'total': '合计'
}

# a_columns = [
#     "record_time_type", "record_time", "sku", "asin", "model_no", "country", "currency", "account", "units", "sales",
#     "sku_units", "sku_sales", "other_sku_units", "other_sku_sales", "clicks", "impressions", "spend", "orders", "cr",
#     "ctr", "cpc", "batch_operation_date"
# ]
ADVERT_SUM_COLUMNS = {
    'A': [
        'sales', 'units', 'sku_sales', 'sku_units', 'other_sku_sales', 'other_sku_units', 'clicks', 'impressions','spend', 'orders'
    ],
    'other': [
            'sales', 'units', 'clicks', 'impressions', 'spend', 'orders'
    ]
}

