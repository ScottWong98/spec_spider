import os
import re
from typing import Dict, List
import pandas as pd

from spec_spider.utils import (
    clean_date_1,
    clean_date_3,
    clean_vendor,
    get_cpu_vendor,
    get_full_url,
    get_memory_number,
    get_submit_quarter,
    get_submit_year,
    parse_cpu_char,
    parse_system_name,
)


class Jvm2008Extrator:
    def __init__(
        self, data_folder, filename,
    ):
        filepath: str = os.path.join(data_folder, filename)

        self.df: pd.DataFrame = pd.read_csv(filepath)

    def _extract_columns(self, columns: List[str]):
        self.df = self.df[columns]

    def _rename_columns(self, rename_dict: Dict[str, str]):
        self.df.rename(columns=rename_dict, inplace=True)

    def _delete_nan(self):
        self.df = self.df[~self.df.isnull().any(axis=1)].reset_index(drop=True)

    def _clean_vendor(self):
        self.df['HW Vendor'] = self.df['HW Vendor'].apply(lambda x: clean_vendor(x))

    def _parse_system_name(self):
        self.df['System Series'] = self.df['System Name'].apply(
            lambda x: parse_system_name(x)
        )

    def _get_cpu_vendor(self):
        self.df['CPU Vendor'] = self.df['CPU Name'].apply(lambda x: get_cpu_vendor(x))

    def _get_cpu_ghz(self):
        def _func(info):
            if 'GHz' in info:
                info = re.sub('[ ]*GHz', '', info)
                val = float(info)
            else:
                val = float(info) / 1000
            val = round(val, 2)
            return val

        self.df['CPU GHz'] = self.df['CPU MHz'].apply(lambda x: _func(x))

    def _get_max_ghz(self):
        self.df['Max GHz'] = self.df['CPU Name'].apply(lambda x: parse_cpu_char(x))
        f = (self.df['Max GHz'] == 0.0) | (self.df['Max GHz'] < self.df['CPU GHz'])
        indices = self.df[f].index
        self.df.loc[indices, 'Max GHz'] = self.df.loc[indices, 'CPU GHz']

    def _format_cpu_name(self):
        def _func(info):
            return re.sub('[(].*?[)]', '', info)

        self.df['CPU Name'] = self.df['CPU Name'].apply(lambda x: _func(x))

    def _get_memory_amount(self):
        def get_memory_amount(info):
            items = re.findall('\d+\.*\d*[ ]?GB', info)
            value = 0
            if len(items) == 0:
                items = re.findall('\d+\.*\d*[ ]?MB', info)
                if len(items):
                    info = re.sub('[ ]*MB', '', info)
                value = float(info) / 1024
            else:
                info = re.sub('[ ]*GB', '', info)
                value = float(info)
            value = round(value, 2)
            return value

        self.df['Memory Amount'] = self.df['Memory Size'].apply(
            lambda x: get_memory_amount(x)
        )

    def _get_memory_number(self):
        def _func(info):
            items = re.findall('\d+[ ]*x', info)
            if len(items) == 0:
                number = 1
            else:
                number = int(re.sub('[ ]*x', '', items[0]))
            return number

        self.df['Memory Number'] = self.df['# and size of DIMM(s)'].apply(
            lambda x: _func(x)
        )

    def _parse_file_system(self):
        self.df['File System'] = self.df['File System'].apply(lambda x: x.lower())

    def _get_jvm(self):
        self.df['JVM'] = self.df.apply(
            lambda item: f"{item['JVM Name']} {item['JVM Version']}", axis=1
        )

    def _get_submit_quarter(self):
        self.df['Submit Quarter'] = self.df['URL Suffix'].apply(
            lambda x: get_submit_quarter(x)
        )

    def _get_submit_year(self):
        self.df['Submit Year'] = self.df['URL Suffix'].apply(
            lambda x: get_submit_year(x)
        )

    def _get_full_url(self):
        self.df['Full URL'] = self.df['URL Suffix'].apply(lambda x: get_full_url(x))

    def _to_csv(self, filename):
        target_folder = './data/clean/java'
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        self.df.to_csv(os.path.join(target_folder, filename), index=False)

    def _get_cores_per_chip(self):
        self.df['Cores Per Chip'] = self.df['Total Cores'] / self.df['Chips']

    def _get_memory_number(self):
        self.df['Memory Number'] = self.df['Memory'].apply(
            lambda x: get_memory_number(x)
        )
    
    def _clean_test_date(self):
        self.df['Test Date'] = self.df['Test Date'].apply(lambda x: clean_date_3(x))

    def _clean_hw_avail(self):
        self.df['HW Avail'] = self.df['HW Avail'].apply(lambda x: clean_date_1(x))
    
    def _format_result(self):
        self.df['Result'] = self.df['Result'].apply(lambda x: float(x))

    def run(self, used_columns, rename_dict):
        self._extract_columns(used_columns)
        self._rename_columns(rename_dict)
        self._delete_nan()
        self._clean_vendor()
        self._parse_system_name()
        self._get_cpu_vendor()
        self._get_cpu_ghz()
        self._get_max_ghz()
        self._get_cores_per_chip()
        self._parse_file_system()
        self._get_memory_amount()
        self._get_memory_number()
        self._get_submit_quarter()
        self._get_submit_year()
        self._get_full_url()
        self._get_jvm()
        self._clean_test_date()
        self._clean_hw_avail()
        self._format_result()

        final_columns = [
            'Suite',
            'HW Vendor',
            'System Series',
            'Result',
            'CPU Vendor',
            'CPU Name',
            'CPU GHz',
            'Max GHz',
            'Threads Per Core',
            'Cores Per Chip',
            'Chips',
            'Total Cores',
            'L1 Cache',
            'L2 Cache',
            'Memory',
            'Memory Number',
            'Memory Amount',
            'OS',
            'File System',
            'JVM',
            'URL Suffix',
            'Test Date',
            'HW Avail',
            'Submit Quarter',
            'Submit Year',
            'Full URL',
        ]
        self.df = self.df[final_columns]
        self._to_csv('jvm2008.csv')
        print(self.df.head())
        print(self.df.shape)
        print(self.df.columns)


def run_jvm2008(data_folder):
    jvm_file = 'java/jvm2008/jvm2008.csv'

    jvm2008_columns = [
        'Suite',
        'HW vendor',
        'HW model',
        'Result',
        'Test date:',
        'HW available',
        'CPU name',
        'CPU frequency',
        '# of logical cpus',
        '# of chips',
        '# of cores',
        'Cores per chip',
        'Threads per core',
        'Primary cache',
        'Secondary cache',
        'Memory size',
        'Memory details',
        'OS name',
        'Filesystem',
        'JVM name',
        'JVM version',
        'URL Suffix',
    ]

    jvm2008_rename_dict = {
        'HW vendor': 'HW Vendor',
        'HW model': 'System Name',
        'Test date:': 'Test Date',
        'HW available': 'HW Avail',
        'CPU name': 'CPU Name',
        'CPU frequency': 'CPU MHz',
        'Cores oer chip': 'Cores Per Chips',
        'Threads per core': 'Threads Per Core',
        'Primary cache': 'L1 Cache',
        'Secondary cache': 'L2 Cache',
        'Memory size': 'Memory Size',
        'Memory details': 'Memory',
        'OS name': 'OS',
        'Filesystem': 'File System',
        'JVM name': 'JVM Name',
        'JVM version': 'JVM Version',
        '# of chips': 'Chips',
        '# of cores': 'Total Cores',
    }

    jvm2008_extractor = Jvm2008Extrator(data_folder, jvm_file)
    jvm2008_extractor.run(jvm2008_columns, jvm2008_rename_dict)


if __name__ == '__main__':
    data_folder = '/home/scott/Documents/SPEC_Spider'
    run_jvm2008(data_folder)
