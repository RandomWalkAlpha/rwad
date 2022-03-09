"""
MIT License

Copyright (c) 2022 RandomWalkAlpha

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

"""
import time
import json
import pandas as pd
import grequests
import requests
import re

from typing import List, Dict, Optional
from pandas import DataFrame, Series


class Collector:

    def get_historical_data(self, code: str, start_date: str, end_date: str, freq: str):
        raise NotImplementedError

    def get_real_time_data(self, code: str):
        raise NotImplementedError

    def clean(self, record: List):
        raise NotImplementedError


class DataCollector(Collector):
    """
    This interface is designed to get the raw stock data by published http(s) request.
    Now, it already collects data from Tencent, Sina, Netease and Hexun.
    Returned data differs from providers, users need to distinguish by themselves.
    """
    def __init__(self, provider: str = 'T'):
        """
        This init function is to indicate a data provider.
        :param provider: data provider: ['Tencent', 'Netease', 'Sina'], default: 'T' (for Tencent Stock)
        """
        assert provider in ['T', 'N', 'S'], f"{provider} is not in the provider list."
        self.provider = provider
        self.url = None
        self.requests = None
        self.grequests = None
        self.session = requests.Session()
        self.raw_columns = ["交易所x", "股票名称x", "股票代码x", "现价", "昨收", "今开", "成交量", "外盘", "内盘",
                            "买一", "买一量", "买二", "买二量", "买三", "买三量", "买四", "买四量", "买五", "买五量",
                            "卖一", "卖一量", "卖二", "卖二量", "卖三", "卖三量", "卖四", "卖四量", "卖五", "卖五量",
                            "时间戳", "涨跌", "涨跌幅(%)", "最高", "最低", "现价/成交量（手）/成交额（元）x", "成交量x",
                            "成交额（万元）x", "换手率", "TTM市盈率", "最高x", "最低x", "振幅(%)", "流通市值", "总市值",
                            "LF市盈率", "涨停价", "跌停价", "量比", "A", "均价", "动态市盈率", "静态市盈率", "B", "成交额",
                            "nonex", "nonex", "nonex", "GP-Ax", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "x",
                            "x", "M", "N", "x", "买盘大单", "买盘小单", "卖盘大单", "卖盘小单"]

    def get_historical_data(self, code: str, start_date: str, end_date: str, freq: str = 'day') -> Optional[DataFrame]:
        """
        Return historical stock data, excluding the latest opening day.
        :param code: stock code
        :param start_date: data starts with this date, form: YYYY-mm-dd
        :param end_date: data ends with this date, form: YYYY-mm-dd
        :param freq: frequency: ['day', 'month'], default: 'day' (for day)
        :return: a dataframe, including 6 basic stock factors (Date, Open, Close, High, Low, Volume)
        """
        code = code.lower()
        assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90|sh00\d{4})|(sz\d{6})").match(code) is not None,\
            f"Stock Code {code} is illegal."
        assert time.strptime(start_date, '%Y-%m-%d') or time.strptime(end_date, '%Y-%m-%d')
        assert freq in ['day', 'month'], f"{freq} doesn't belong to ['day', 'month']"
        assert self.provider in ['T', 'N', 'S'], f"{self.provider} is not in the provider list."

        columns = ["日期", "开盘价", "收盘价", "最高价", "最低价", "成交量（手）"]
        clean_data = []

        if self.provider == 'T':
            self.url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
            days = int(time.mktime(time.strptime(end_date, '%Y-%m-%d'))
                       - time.mktime(time.strptime(start_date, '%Y-%m-%d'))) // 86400
            params = f"param={code},{freq},{start_date},{end_date},{days},qfq"
            self.requests = self.session.get(f"{self.url}{params}")
            data = json.loads(self.requests.text)
            try:
                data = data["data"][code][f"qfq{freq}"]
            except KeyError:
                data = data["data"][code][f"{freq}"]

            for _data in data:
                if len(_data) > 6:
                    _data = _data[: 6]
                clean_data.append(_data)
            return pd.DataFrame(clean_data, columns=columns)

        else:
            assert freq == 'day', f"Netease only supports freq: 'day'."
            self.url = "https://quotes.money.163.com/service/chddata.html?"
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '')
            if re.compile(r"sh68|sh60|sh90|sh00\d{4}").match(code):
                code = '0' + code[2:]
            else:
                code = '1' + code[2:]
            params = f'code={code}&start={start_date}&end={end_date}' \
                     f'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER'
            self.requests = self.session.get(f"{self.url}{params}")
            raw_data = self.requests.text.split('\r\n')
            columns = raw_data[0].split(',')
            raw_data = raw_data[1: -1]
            df = pd.DataFrame(columns=columns)

            for line in raw_data:
                record = line.split(',')
                df = df.append(pd.Series(record, index=columns), ignore_index=True)
            return df

    def get_batch_historical_data(self, code_list: List, start_date: str, end_date: str, freq: str = 'day') -> Dict:
        """
        Return a batch of historical trading data with the stock code in list.
        :param code_list: stock code list
        :param start_date: data starts with this date, form: YYYY-mm-dd
        :param end_date: data ends with this date, form: YYYY-mm-dd
        :param freq: frequency: ['day', 'month'], default: 'day' (for day)
        :return: a dict of Dataframe
        """
        assert time.strptime(start_date, '%Y-%m-%d') or time.strptime(end_date, '%Y-%m-%d')
        assert freq in ['day', 'month'], f"{freq} doesn't belong to ['day', 'month']"
        assert self.provider in ['T', 'N', 'S'], f"{self.provider} is not in the provider list."

        if self.provider == 'T':
            self.url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
            df_dict = {}
            request_list = []
            columns = ["日期", "开盘价", "收盘价", "最高价", "最低价", "成交量（手）"]

            for code in code_list:
                code = code.lower()
                assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90|sh00\d{4})|(sz\d{6})").match(code) is not None, \
                    f"Stock Code {code} is illegal."
                days = int(time.mktime(time.strptime(end_date, '%Y-%m-%d'))
                           - time.mktime(time.strptime(start_date, '%Y-%m-%d'))) // 86400
                params = f"param={code},{freq},{start_date},{end_date},{days},qfq"
                request_list.append(grequests.get(f"{self.url}{params}", session=self.session))
            self.requests = grequests.map(request_list)

            for req, code in zip(self.requests, code_list):
                clean_data = []
                try:
                    data = json.loads(req.text)["data"][code.lower()][f"qfq{freq}"]
                except KeyError:
                    data = json.loads(req.text)["data"][code.lower()][f"{freq}"]

                for _data in data:
                    if len(_data) > 6:
                        _data = _data[: 6]
                    clean_data.append(_data)
                df_dict[code] = pd.DataFrame(clean_data, columns=columns)
            return df_dict

        else:
            assert freq == 'day', f"Netease only supports freq: 'day'."
            self.url = "https://quotes.money.163.com/service/chddata.html?"
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '')
            df_dict = {}
            request_list = []

            for code in code_list:
                code = code.lower()
                assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90|sh00\d{4})|(sz\d{6})").match(code) is not None, \
                    f"Stock Code {code} is illegal."
                if re.compile(r"sh68|sh60|sh90|sh00\d{4}").match(code):
                    code = '0' + code[2:]
                else:
                    code = '1' + code[2:]
                params = f'code={code}&start={start_date}&end={end_date}' \
                         f'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER'
                request_list.append(grequests.get(f"{self.url}{params}", session=self.session))
                print(f"{self.url}{params}")
            self.requests = grequests.map(request_list)

            for req, code in zip(self.requests, code_list):
                raw_data = req.text.split('\r\n')
                columns = raw_data[0].split(',')
                raw_data = raw_data[1: -1]
                df = pd.DataFrame(columns=columns)

                for line in raw_data:
                    record = line.split(',')
                    df = df.append(pd.Series(record, index=columns), ignore_index=True)
                df_dict[code] = df
            return df_dict

    def get_real_time_data(self, code: str) -> Optional[Series]:
        """
        Return real time trading data with specific stock, this interface can be called every 5 seconds
        at the fastest. If now time is not opening period, this would return latest valid data.
        If you want to get more than one stock data at one time, please use get_batch_real_time_data() instead.
        :param code: stock code, eg: SH600519, SZ399001
        :return: latest stock data string
        """
        assert self.provider == 'T', "Only Tencent Stock interface is supported!"
        code = code.lower()
        assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90\d{4})|(sz\d{6})").match(code) is not None, \
            f"Stock Code {code} is illegal."

        self.url = "https://qt.gtimg.cn/q="
        self.requests = self.session.get(f"{self.url}{code},s_pk{code}")
        return self.clean(self.requests.text[: -2]) if 'v_pv_none_match' not in self.requests.text else None

    def get_batch_real_time_data(self, code_list: List) -> Optional[Dict]:
        """
        Return a batch of real time trading data with the stock code in list.
        :param code_list: stock code list
        :return: latest stock data dict
        """
        assert self.provider == 'T', "Only Tencent Stock interface is supported!"

        self.url = "https://qt.gtimg.cn/q="
        request_list = []
        data_dict = {}

        for code in code_list:
            code = code.lower()
            assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90\d{4})|(sz\d{6})").match(code) is not None, \
                f"Stock Code {code} is illegal."
            request_list.append(grequests.get(f"{self.url}{code},s_pk{code}", session=self.session))
        self.requests = grequests.map(request_list)

        for code, req in zip(code_list, self.requests):
            data_dict[code] = self.clean(req.text[: -2]) if "v_pv_none_match" not in req.text else None
        return data_dict

    def get_minute_data(self, code) -> Optional[DataFrame]:
        """
        Get brief minute-level stock data of last opening day
        :param code: stock code
        :return: dataframe
        """
        assert self.provider == 'T', "Only Tencent Stock interface is supported!"
        code = code.lower()
        assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90|sh00\d{4})|(sz\d{6})").match(code) is not None, \
            f"Stock Code {code} is illegal."
        self.url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={code}"
        self.requests = self.session.get(self.url)
        data = json.loads(self.requests.text)
        columns = ["时间戳", "现价", "累计成交量", "现成交量"]
        df = pd.DataFrame(columns=columns)
        if data["code"] == -1:
            return df
        try:
            data = data["data"][f"{code}"]["data"]["data"]
        except KeyError:
            return df
        if data[0] == " 0":
            return df
        else:
            for record in data:
                record = record.split(' ')
                df = df.append(pd.Series(record, index=columns), ignore_index=True)
        return df

    def get_transaction_detail(self, code: str, start_date: str, end_date: str) -> Optional[DataFrame]:
        assert self.provider == 'S', "Only Sina Stock interface is supported!"
        code = code.lower()
        assert len(code) == 8 and re.compile(r"(sh68|sh60|sh90|sh00\d{4})|(sz\d{6})").match(code) is not None, \
            f"Stock Code {code} is illegal."
        assert time.strptime(start_date, '%Y-%m-%d') or time.strptime(end_date, '%Y-%m-%d')
        days = int(time.mktime(time.strptime(end_date, '%Y-%m-%d'))
                   - time.mktime(time.strptime(start_date, '%Y-%m-%d'))) // 86400
        assert days < 270, "Query date range is too large, please narrow the scope with 10 months."

        self.url = "https://market.finance.sina.com.cn/pricehis.php?"
        params = f"symbol={code}&startdate={start_date}&enddate={end_date}"
        self.requests = self.session.get(f"{self.url}{params}")
        data = re.findall(r'>[0-9].*<', self.requests.text)
        columns = ["成交价（元）", "成交量（股）", "占比"]
        df = pd.DataFrame(columns=columns)

        for element in range(0, len(data), 3):
            val_1 = float(data[element][1: -1].replace(',', ''))
            val_2 = int(data[element + 1][1: -1])
            val_3 = float(data[element + 2][1: -2])
            df = df.append(pd.Series([val_1, val_2, val_3], index=columns), ignore_index=True)
        return df

    def clean(self, record: str) -> Optional[Series]:
        """
        Used for get_real_time_data() and get_batch_real_time_data(), Transfer raw data to normalized series.
        :param record: raw data
        :return: normalized series with 64 factors
        """
        if record is None:
            return None
        raw_data = ''
        record_list = record.split(';')

        for rec in record_list:
            raw_data += rec.split('=')[1][1: -1].replace("~~~", '~').replace("~~", '~').replace("~ ", '~')
        data_list = raw_data.split('~')
        series = []
        index = []

        for key, val in zip(self.raw_columns, data_list):
            if 'x' not in key:
                index.append(key)
                series.append(eval(val))
        return pd.Series(series, index=index)
