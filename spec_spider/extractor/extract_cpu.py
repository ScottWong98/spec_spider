import os
from typing import Dict, List, Optional

import pandas as pd

from spec_spider.utils import (
    clean_date_1,
    clean_vendor,
    get_chips,
    get_cpu_vendor,
    get_full_url,
    get_memory_number,
    get_submit_quarter,
    get_submit_year,
    get_threads_per_core,
    get_total_cores,
    get_total_memory_amount,
    parse_cpu_char,
    parse_cpu_orderable,
    parse_storage,
    parse_system_name,
)


class CpuExtractor:
    def __init__(
        self,
        data_folder: str,
        rfp_file: str,
        sfp_file: str,
        rint_file: str,
        sint_file: str,
    ):
        rfp_path: str = os.path.join(data_folder, rfp_file)
        sfp_path: str = os.path.join(data_folder, sfp_file)
        rint_path: str = os.path.join(data_folder, rint_file)
        sint_path: str = os.path.join(data_folder, sint_file)

        self.rfp: pd.DataFrame = pd.read_csv(rfp_path)
        self.sfp: pd.DataFrame = pd.read_csv(sfp_path)
        self.rint: pd.DataFrame = pd.read_csv(rint_path)
        self.sint: pd.DataFrame = pd.read_csv(sint_path)

        self.df: Optional[pd.DataFrame] = None

    def _extract_columns(self, columns: List[str]):
        self.rfp = self.rfp[columns]
        self.sfp = self.sfp[columns]
        self.rint = self.rint[columns]
        self.sint = self.sint[columns]

    def _concat_df(self):
        self.df = pd.concat([self.rfp, self.sfp, self.rint, self.sint]).reset_index(
            drop=True
        )

    def _rename_columns(self, rename_dict: Dict[str, str]):
        self.df.rename(columns=rename_dict, inplace=True)

    def _delete_nan(self):
        self.df = self.df[~self.df.isnull().any(axis=1)].reset_index(drop=True)
        self.df = self.df[self.df['Result'] != 'NC'].reset_index(drop=True)

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

    def _parse_cpu_orderable(self):
        self.df['Max Chips'] = self.df['CPU Orderable'].apply(
            lambda x: parse_cpu_orderable(x)
        )

    def _get_total_cores(self):
        self.df['Total Cores'] = self.df['CPU Enabled'].apply(
            lambda x: get_total_cores(x)
        )

    def _get_chips(self):
        self.df['Chips'] = self.df['CPU Enabled'].apply(lambda x: get_chips(x))

    def _get_threads_per_core(self):
        self.df['Threads Per Core'] = self.df['CPU Enabled'].apply(
            lambda x: get_threads_per_core(x)
        )

    def _get_cores_per_chip(self):
        self.df['Cores Per Chip'] = self.df.apply(
            lambda item: item['Total Cores'] // item['Chips'], axis=1
        )

    def _parse_file_system(self):
        self.df['File System'] = self.df['File System'].apply(lambda x: x.lower())

    def _get_total_memory_amount(self):
        self.df['Memory Amount'] = self.df['Memory'].apply(
            lambda x: get_total_memory_amount(x)
        )

    def _get_memory_number(self):
        self.df['Memory Number'] = self.df['Memory'].apply(
            lambda x: get_memory_number(x)
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
        target_folder = './data/clean/cpu'
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        self.df.to_csv(os.path.join(target_folder, filename), index=False)
    
    def _clean_test_date(self):
        self.df['Test Date'] = self.df['Test Date'].apply(lambda x: clean_date_1(x))
    
    def _clean_hw_avail(self):
        self.df['HW Avail'] = self.df['HW Avail'].apply(lambda x: clean_date_1(x))
    
    def _format_result(self):
        self.df['Result'] = self.df['Result'].apply(lambda x: float(x))


class Cpu2017Extrator(CpuExtractor):
    def __init__(
        self,
        data_folder: str,
        rfp_file: str,
        sfp_file: str,
        rint_file: str,
        sint_file: str,
    ):
        super().__init__(data_folder, rfp_file, sfp_file, rint_file, sint_file)

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
        self._parse_cpu_orderable()
        self._get_total_cores()
        self._get_chips()
        self._get_threads_per_core()
        self._get_cores_per_chip()
        self._parse_file_system()
        self._get_total_memory_amount()
        self._get_memory_number()
        self._parse_storage()
        self._get_submit_quarter()
        self._get_submit_year()
        self._get_full_url()
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
            'URL Suffix',
            'Test Date',
            'HW Avail',
            'Submit Quarter',
            'Submit Year',
            'Full URL',
        ]

        self.df = self.df[final_columns]
        self._to_csv('cpu2017.csv')

        print(self.df.head())
        print(self.df.shape)
        print(self.df.columns)

    def _get_max_ghz(self):
        self.df['Max GHz'] = self.df['Max MHz'].apply(lambda x: round(x / 1000, 2))


class Cpu2006Extrator(CpuExtractor):
    def __init__(
        self,
        data_folder: str,
        rfp_file: str,
        sfp_file: str,
        rint_file: str,
        sint_file: str,
    ):
        super().__init__(data_folder, rfp_file, sfp_file, rint_file, sint_file)

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
        self._parse_cpu_orderable()
        self._get_total_cores()
        self._get_chips()
        self._get_threads_per_core()
        self._get_cores_per_chip()
        self._parse_file_system()
        self._get_total_memory_amount()
        self._get_memory_number()
        self._parse_storage()
        self._get_submit_quarter()
        self._get_submit_year()
        self._get_full_url()
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
            'Memory Amount',
            'Memory Number',
            'Storage Type',
            'Storage',
            'OS',
            'File System',
            'URL Suffix',
            'Test Date',
            'HW Avail',
            'Submit Quarter',
            'Submit Year',
            'Full URL',
        ]

        self.df = self.df[final_columns]
        self.df = self.df[self.df['System Series'] != '']
        self.df.reset_index(drop=True, inplace=True)

        self._to_csv('cpu2006.csv')

        print(self.df.head())
        print(self.df.shape)
        print(self.df.columns)

    def _get_max_ghz(self):
        self.df['Max GHz'] = self.df['CPU Characteristics'].apply(
            lambda x: parse_cpu_char(x)
        )
        f = self.df['Max GHz'] == 0.0
        indices = self.df[f].index
        self.df.loc[indices, 'Max GHz'] = self.df.loc[indices, 'CPU GHz']


def run_cpu2017(data_folder):
    c2017_rfp = 'cpu/cpu2017/CFP2017_rate.csv'
    c2017_sfp = 'cpu/cpu2017/CFP2017_speed.csv'
    c2017_rint = 'cpu/cpu2017/CINT2017_rate.csv'
    c2017_sint = 'cpu/cpu2017/CINT2017_speed.csv'
    c2017_columns = [
        'Suite',
        'Hardware Vendor',
        'System Name',
        'Baseline',
        'Test Date',
        'HW Avail',
        'CPU Name',
        'Max MHz',
        'Nominal',
        'Enabled',
        'Orderable',
        'L1',
        'L2',
        'L3',
        'Memory',
        'Storage',
        'OS',
        'File System',
        'URL Suffix',
    ]
    c2017_rename_dict = {
        'Baseline': 'Result',
        'Hardware Vendor': 'HW Vendor',
        'Nominal': 'CPU MHz',
        'Enabled': 'CPU Enabled',
        'Orderable': 'CPU Orderable',
        'L1': 'L1 Cache',
        'L2': 'L2 Cache',
        'L3': 'L3 Cache',
    }

    c2017_extrator = Cpu2017Extrator(
        data_folder, c2017_rfp, c2017_sfp, c2017_rint, c2017_sint
    )
    c2017_extrator.run(c2017_columns, c2017_rename_dict)


def run_cpu2006(data_folder):
    c2006_rfp = 'cpu/cpu2006/SPECfp_rate.csv'
    c2006_sfp = 'cpu/cpu2006/SPECfp.csv'
    c2006_rint = 'cpu/cpu2006/SPECint_rate.csv'
    c2006_sint = 'cpu/cpu2006/SPECint.csv'

    c2006_columns = [
        'Suite',
        'Hardware Vendor',
        'System Name',
        'Baseline',
        'Test Date',
        'HW Avail',
        'CPU Name',
        'CPU Characteristics',
        'CPU MHz',
        'CPU(s) enabled',
        'CPU(s) orderable',
        'Primary Cache',
        'Secondary Cache',
        'L3 Cache',
        'Memory',
        'Disk Subsystem',
        'OS',
        'File System',
        'URL Suffix',
    ]
    c2006_rename_dict = {
        'Baseline': 'Result',
        'Hardware Vendor': 'HW Vendor',
        'CPU(s) enabled': 'CPU Enabled',
        'CPU(s) orderable': 'CPU Orderable',
        'Primary Cache': 'L1 Cache',
        'Secondary Cache': 'L2 Cache',
        'L3 Cache': 'L3 Cache',
        'Disk Subsystem': 'Storage',
    }

    c2006_extractor = Cpu2006Extrator(
        data_folder, c2006_rfp, c2006_sfp, c2006_rint, c2006_sint
    )
    c2006_extractor.run(c2006_columns, c2006_rename_dict)


if __name__ == '__main__':
    data_folder = '/home/scott/Documents/SPEC_Spider'

    run_cpu2017(data_folder)
    run_cpu2006(data_folder)

