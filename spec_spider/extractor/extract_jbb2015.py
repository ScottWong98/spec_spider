import os
import re
from typing import Dict, List, Optional

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


class Jbb2015Extractor:
    def __init__(
        self, data_folder: str, com_file: str, mul_file: str, dis_file: str,
    ):
        com_path: str = os.path.join(data_folder, com_file)
        mul_path: str = os.path.join(data_folder, mul_file)
        dis_path: str = os.path.join(data_folder, dis_file)

        self.com: pd.DataFrame = pd.read_csv(com_path)
        self.mul: pd.DataFrame = pd.read_csv(mul_path)
        self.dis: pd.DataFrame = pd.read_csv(dis_path)

        self.df: Optional[pd.DataFrame] = None

    def _extract_columns(self, columns: List[str]):
        self.com = self.com[columns]
        self.mul = self.mul[columns]
        self.dis = self.dis[columns]

    def _concat_df(self):
        self.df = pd.concat([self.com, self.mul, self.dis]).reset_index(drop=True)

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
        self.df['CPU GHz'] = self.df['CPU MHz'].apply(lambda x: round(x / 1000, 2))

    def _get_max_ghz(self):
        self.df['Max GHz'] = self.df['CPU Characteristics'].apply(
            lambda x: parse_cpu_char(x)
        )
        f = self.df['Max GHz'] == 0.0
        indices = self.df[f].index
        self.df.loc[indices, 'Max GHz'] = self.df.loc[indices, 'CPU GHz']

    def _format_memory_amount(self):
        def _formatter(item):
            if type(item) != int:
                if 'GB' in item:
                    item = re.sub('GB', '', item)
                item = item.strip()
                item = int(item)
            return item

        self.df['Memory Amount'] = self.df['Memory Amount'].apply(
            lambda x: _formatter(x)
        )

    def _parse_file_system(self):
        self.df['File System'] = self.df['File System'].apply(lambda x: x.lower())

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

    def _parse_storage(self):
        self.df['Storage Type'] = self.df['Storage'].apply(lambda x: parse_storage(x))

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

    def _get_os(self):
        self.df['OS'] = self.df.apply(
            lambda item: f"{item['OS Name']}; {item['OS Version']}", axis=1
        )

    def _get_jvm(self):
        self.df['JVM'] = self.df.apply(
            lambda item: f"{item['JVM Name']}; {item['JVM Version']}", axis=1
        )

    def _clean_test_date(self):
        self.df['Test Date'] = self.df['Test Date'].apply(lambda x: clean_date_2(x))

    def _clean_hw_avail(self):
        self.df['HW Avail'] = self.df['HW Avail'].apply(lambda x: clean_date_1(x))
    
    def _format_result(self):
        self.df['Result'] = self.df['Result'].apply(lambda x: float(x))

    def run(self, used_columns, rename_dict):
        self._extract_columns(used_columns)
        self._concat_df()
        self._rename_columns(rename_dict)
        self._delete_nan()
        self._clean_vendor()
        self._parse_system_name()
        self._get_cpu_vendor()
        self._get_cpu_ghz()
        self._get_max_ghz()
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
            'Nodes',
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
        self._to_csv('jbb2015.csv')

        print(self.df.head())
        print(self.df.shape)
        print(self.df.columns)


def run_jbb2015(data_folder):
    com_file = 'java/jbb2015/SPECjbb2015-Composite.csv'
    mul_file = 'java/jbb2015/SPECjbb2015-Distributed.csv'
    dis_file = 'java/jbb2015/SPECjbb2015-MultiJVM.csv'

    jbb2015_columns = [
        'Suite',
        'Vendor',
        'System Name',
        # 'max_jOPS',
        'cirtical_jOPS',
        'Test date',
        'Hardware Availability',
        'Nodes Per System',
        'CPU Name',
        'CPU Characteristics',
        'Number of Systems',
        'Chips Per System',
        'Cores Per System',
        'Cores Per Chip',
        'Threads Per Core',
        'CPU Frequency (MHz)',
        'Primary Cache',
        'Secondary Cache',
        'Tertiary Cache',
        'Disk',
        'File System',
        'Memory Amount (GB)',
        '# and size of DIMM(s)',
        'Memory Details',
        'OS Name',
        'OS Vendor',
        'OS Version',
        'JVM Name',
        'JVM Vendor',
        'JVM Version',
        'URL Suffix',
    ]

    jbb2015_rename_dict = {
        'cirtical_jOPS': 'Result',
        'Vendor': 'HW Vendor',
        'Test date': 'Test Date',
        'Hardware Availability': 'HW Avail',
        'CPU Frequency (MHz)': 'CPU MHz',
        'Primary Cache': 'L1 Cache',
        'Secondary Cache': 'L2 Cache',
        'Tertiary Cache': 'L3 Cache',
        'Disk': 'Storage',
        'Total Memory Amount (GB)': 'Total Memory Amount',
        'Memory Amount (GB)': 'Memory Amount',
        'Memory Details': 'Memory',
        'Nodes Per System': 'Nodes',
        'Chips Per System': 'Chips',
        'Cores Per System': 'Total Cores',
    }

    jbb2015_extractor = Jbb2015Extractor(data_folder, com_file, mul_file, dis_file)
    jbb2015_extractor.run(jbb2015_columns, jbb2015_rename_dict)


if __name__ == '__main__':
    data_folder = '/home/scott/Documents/SPEC_Spider'
    run_jbb2015(data_folder)
