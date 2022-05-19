import os
import re
from typing import Dict, List
import pandas as pd

from spec_spider.utils import (
    clean_date_1,
    clean_date_2,
    clean_vendor,
    get_cpu_vendor,
    get_full_url,
    get_submit_quarter,
    get_submit_year,
    parse_cpu_char,
    parse_storage,
    parse_system_name,
)


class Ssj2008Extractor:
    def __init__(self, data_folder, filename):
        filepath = os.path.join(data_folder, filename)

        self.df: pd.DataFrame = pd.read_csv(filepath)

    def _extract_columns(self, columns: List[str]):
        self.df = self.df[columns]

    def _rename_columns(self, rename_dict: Dict[str, str]):
        self.df.rename(columns=rename_dict, inplace=True)

    def _delete_nan(self):
        self.df = self.df[~self.df.isnull().any(axis=1)].reset_index(drop=True)
        self.df = self.df[self.df['Test Date'] != 'Various'].reset_index(drop=True)

    def _clean_vendor(self):
        self.df['HW Vendor'] = self.df['HW Vendor'].apply(lambda x: clean_vendor(x))

    def _parse_system_name(self):
        self.df['System Series'] = self.df['System Name'].apply(
            lambda x: parse_system_name(x)
        )

    def _get_cpu_vendor(self):
        self.df['CPU Vendor'] = self.df['CPU Name'].apply(lambda x: get_cpu_vendor(x))

    def _get_cpu_ghz(self):
        self.df['CPU GHz'] = self.df['CPU MHz'].apply(lambda x: round(x / 1000, 2))

    def _get_max_ghz(self):
        self.df['Max GHz'] = self.df['CPU Characteristics'].apply(
            lambda x: parse_cpu_char(x)
        )
        f = (self.df['Max GHz'] == 0.0) | (self.df['Max GHz'] < self.df['CPU GHz'])
        indices = self.df[f].index
        self.df.loc[indices, 'Max GHz'] = self.df.loc[indices, 'CPU GHz']

    def _get_total_cores(self):
        self.df['Total Cores'] = self.df['CPU Enabled'].apply(
            lambda x: int(x.split(',')[0].split()[0])
        )

    def _get_chips(self):
        self.df['Chips'] = self.df['CPU Enabled'].apply(
            lambda x: int(x.split(',')[1].split()[0])
        )

    def _get_cores_per_chip(self):
        self.df['Cores Per Chip'] = self.df['CPU Enabled'].apply(
            lambda x: int(x.split(',')[2].split()[0])
        )

    def _get_threads_per_chip(self):
        self.df['Threads Per Core'] = self.df['Hardware Threads'].apply(
            lambda x: int(re.sub(' / core', '', re.findall('\d+ / core', x)[0]))
        )

    def _format_memory_amount(self):
        self.df['Memory Amount'] = self.df['Memory Amount'].apply(lambda x: int(x))

    def _get_memory_number(self):
        def _func(info):
            items = re.findall('\d+[ ]*x', info)
            if len(items) == 0:
                number = 1
            else:
                number = int(re.sub('[ ]*x', '', items[0]))
            return number

        self.df['Memory Number'] = self.df['# and size of DIMM'].apply(
            lambda x: _func(x)
        )

    def _parse_storage(self):
        self.df['Storage Type'] = self.df['Storage'].apply(lambda x: parse_storage(x))

    def _parse_file_system(self):
        self.df['File System'] = self.df['File System'].apply(lambda x: x.lower())

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
        target_folder = './data/clean/power'
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        self.df.to_csv(os.path.join(target_folder, filename), index=False)

    def _get_os(self):
        self.df['OS'] = self.df.apply(
            lambda item: f"{item['OS']}; {item['OS Version']}", axis=1
        )

    def _get_jvm(self):
        self.df['JVM'] = self.df.apply(
            lambda item: f"{item['JVM Vendor']}; {item['JVM Version']}", axis=1
        )

    def _clean_test_date(self):
        self.df['Test Date'] = self.df['Test Date'].apply(lambda x: clean_date_2(x))

    def _clean_hw_avail(self):
        self.df['HW Avail'] = self.df['HW Avail'].apply(lambda x: clean_date_1(x))

    def _format_result(self):
        self.df['Result'] = self.df['Result'].apply(lambda x: float(x.replace(',', '')))

    def run(self, used_columns, rename_dict):
        self._extract_columns(used_columns)
        self._rename_columns(rename_dict)
        self._delete_nan()
        self._clean_vendor()
        self._parse_system_name()
        self._get_cpu_vendor()
        self._get_cpu_ghz()
        self._get_max_ghz()
        self._get_total_cores()
        self._get_chips()
        self._get_cores_per_chip()
        self._get_threads_per_chip()
        self._parse_file_system()
        self._format_memory_amount()
        self._get_memory_number()
        self._parse_storage()
        self._get_submit_quarter()
        self._get_submit_year()
        self._get_full_url()
        self._get_os()
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
            'L3 Cache',
            'Memory',
            'Memory Number',
            'Memory Amount',
            'Storage Type',
            'Storage',
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
        self._to_csv('ssj2008.csv')

        print(self.df.head())
        print(self.df.shape)
        print(self.df.columns)


def run_ssj2008(data_folder):
    ssj_file = 'power/ssj2008.csv'

    ssj2008_columns = [
        'Suite',
        'Hardware Vendor',
        'Model',
        'Test Date',
        'Hardware Availability',
        'Benchmark',
        'CPU Name',
        'CPU Characteristics',
        'CPU Frequency (MHz)',
        'CPU(s) Enabled',
        'CPU(s) Orderable',
        'Hardware Threads',
        'Primary Cache',
        'Secondary Cache',
        'Tertiary Cache',
        'Memory Amount (GB)',
        '# and size of DIMM',
        'Memory Details',
        'Disk Drive',
        'Operating System (OS)',
        'OS Version',
        'Filesystem',
        'JVM Vendor',
        'JVM Version',
        'URL Suffix',
    ]

    ssj2008_rename_dict = {
        'Hardware Vendor': 'HW Vendor',
        'Model': 'System Name',
        'Hardware Availability': 'HW Avail',
        'Benchmark': 'Result',
        'CPU Frequency (MHz)': 'CPU MHz',
        'CPU(s) Enabled': 'CPU Enabled',
        'CPU(s) Orderable': 'CPU Orderable',
        'Primary Cache': 'L1 Cache',
        'Secondary Cache': 'L2 Cache',
        'Tertiary Cache': 'L3 Cache',
        'Memory Amount (GB)': 'Memory Amount',
        'Memory Details': 'Memory',
        'Disk Drive': 'Storage',
        'Operating System (OS)': 'OS',
        'Filesystem': 'File System',
    }

    ssj2008_extractor = Ssj2008Extractor(data_folder, ssj_file)
    ssj2008_extractor.run(ssj2008_columns, ssj2008_rename_dict)


if __name__ == '__main__':
    data_folder = '/home/scott/Documents/SPEC_Spider'
    run_ssj2008(data_folder)
