from parsing.advertising import Advertising, AdvertisingSingle
from parsing.data_processing import Processing
from parsing.log import Logs
import time


def ad_advert():
    log_file = time.time()
    logs = Logs(log_file)
    advertising = AdvertisingSingle(logs)
    product_code_info = advertising.product_code_info
    processing = Processing(logs, product_code_info)
    df = advertising.ad_advert(processing, 'A')
    print(df)


def all_advert():
    log_file = time.time()
    logs = Logs(log_file)
    advertising = Advertising(logs)
    df = advertising.run()
    print(df)


def run():
    log_file = time.time()
    logs = Logs(log_file)
    advertising = AdvertisingSingle(logs)
    product_code_info = advertising.product_code_info
    processing = Processing(logs, product_code_info)
    df = advertising.hv_advert(processing, 'V')
    print(df)


if __name__ == '__main__':
    ad_advert()
