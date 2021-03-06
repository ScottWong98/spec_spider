# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
from datetime import datetime
from scrapy.exporters import CsvItemExporter


class SpecSpiderPipeline:
    def process_item(self, item, spider):
        return item


def get_file_path(benchmark, suite):
    now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    folder = f"data/{benchmark}/{now}"
    if not os.path.exists(folder):
        os.makedirs(folder)

    return f"{folder}/{suite}.csv"


class CpuPipeline:
    cint_name: str
    rint_name: str
    cfp_name: str
    rfp_name: str
    benchmark: str

    def __init__(self,):
        self.cint_file = open(get_file_path(self.benchmark, self.cint_name), 'wb')
        self.rint_file = open(get_file_path(self.benchmark, self.rint_name), 'wb')
        self.cfp_file = open(get_file_path(self.benchmark, self.cfp_name), 'wb')
        self.rfp_file = open(get_file_path(self.benchmark, self.rfp_name), 'wb')

        self.cint_exporter = CsvItemExporter(self.cint_file)
        self.rint_exporter = CsvItemExporter(self.rint_file)
        self.cfp_exporter = CsvItemExporter(self.cfp_file)
        self.rfp_exporter = CsvItemExporter(self.rfp_file)

        self.cint_exporter.start_exporting()
        self.rint_exporter.start_exporting()
        self.cfp_exporter.start_exporting()
        self.rfp_exporter.start_exporting()

        self.cint_cnt = 0
        self.rint_cnt = 0
        self.cfp_cnt = 0
        self.rfp_cnt = 0

    def close_siper(self, spider):
        self.cint_exporter.finish_exporting()
        self.rfp_exporter.finish_exporting()
        self.cfp_exporter.finish_exporting()
        self.rfp_exporter.finish_exporting()

        self.cint_file.close()
        self.rint_file.close()
        self.cfp_file.close()
        self.rfp_file.close()

    def process_item(self, item, spider):
        # suite: CINT2017_speed, CINT2017_rate, CFP2017_speed, CFP2017_rate
        if item['Suite'] == self.cint_name:
            self.cint_exporter.export_item(item)
            self.cint_cnt += 1
            spider.logger.info(f"Crawl item {self.cint_cnt} for {self.cint_name}")
        elif item['Suite'] == self.rint_name:
            self.rint_exporter.export_item(item)
            self.rint_cnt += 1
            spider.logger.info(f"Crawl item {self.rint_cnt} for {self.rint_name}")
        elif item['Suite'] == self.cfp_name:
            self.cfp_exporter.export_item(item)
            self.cfp_cnt += 1
            spider.logger.info(f"Crawl item {self.cfp_cnt} for {self.cfp_name}")
        elif item['Suite'] == self.rfp_name:
            self.rfp_exporter.export_item(item)
            self.rfp_cnt += 1
            spider.logger.info(f"Crawl item {self.rfp_cnt} for {self.rfp_name}")


class Cpu2017Pipeline(CpuPipeline):
    # suite: CINT2017_speed, CINT2017_rate, CFP2017_speed, CFP2017_rate
    cint_name: str = 'CINT2017_speed'
    rint_name: str = 'CINT2017_rate'
    cfp_name: str = 'CFP2017_speed'
    rfp_name: str = 'CFP2017_rate'
    benchmark: str = 'cpu2017'

    def __init__(self):
        super().__init__()


class Cpu2006Pipeline(CpuPipeline):
    # suite: SPECint, SPECint_rate, SPECfp, SPECfp_rate
    cint_name: str = 'SPECint'
    rint_name: str = 'SPECint_rate'
    cfp_name: str = 'SPECfp'
    rfp_name: str = 'SPECfp_rate'
    benchmark: str = 'cpu2006'

    def __init__(self):
        super().__init__()


class Jbb2015Pipeline:
    composite_name = 'SPECjbb2015-Composite'
    multijvm_name = 'SPECjbb2015-MultiJVM'
    distributed_name = 'SPECjbb2015-Distributed'

    def __init__(self,):
        self.composite_file = open(get_file_path('jbb2015', self.composite_name), 'wb')
        self.multijvm_file = open(get_file_path('jbb2015', self.multijvm_name), 'wb')
        self.distributed_file = open(
            get_file_path('jbb2015', self.distributed_name), 'wb'
        )

        self.composite_exporter = CsvItemExporter(self.composite_file)
        self.multijvm_exporter = CsvItemExporter(self.multijvm_file)
        self.distributed_exporter = CsvItemExporter(self.distributed_file)

        self.composite_exporter.start_exporting()
        self.multijvm_exporter.start_exporting()
        self.distributed_exporter.start_exporting()

        self.c_cnt = 0
        self.m_cnt = 0
        self.d_cnt = 0

    def close_siper(self, spider):
        self.composite_exporter.finish_exporting()
        self.multijvm_exporter.finish_exporting()
        self.distributed_exporter.finish_exporting()
        self.composite_file.close()
        self.multijvm_file.close()
        self.distributed_file.close()

    def process_item(self, item, spider):
        if item['Suite'] == self.composite_name:
            self.composite_exporter.export_item(item)
            self.c_cnt += 1
            spider.logger.info(f"Crawl item {self.c_cnt} for {self.composite_name}")
        elif item['Suite'] == self.multijvm_name:
            self.multijvm_exporter.export_item(item)
            self.m_cnt += 1
            spider.logger.info(f"Crawl item {self.m_cnt} for {self.multijvm_name}")
        elif item['Suite'] == self.distributed_name:
            self.distributed_exporter.export_item(item)
            self.d_cnt += 1
            spider.logger.info(f"Crawl item {self.d_cnt} for {self.distributed_name}")


class Jvm2008Pipeline:
    def __init__(self,):
        self.file = open(get_file_path('jvm2008', 'jvm2008'), 'wb')
        self.exporter = CsvItemExporter(self.file)
        self.exporter.start_exporting()
        self.cnt = 0

    def close_siper(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        self.cnt += 1
        spider.logger.info(f"Crawl item {self.cnt} for jvm2008")


class Ssj2008Pipeline:
    def __init__(self,):
        self.file = open(get_file_path('ssj2008', 'ssj2008'), 'wb')
        self.exporter = CsvItemExporter(self.file)
        self.exporter.start_exporting()
        self.cnt = 0

    def close_siper(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        if item.get('Status') is None:
            self.exporter.export_item(item)
            self.cnt += 1
            spider.logger.info(f"Crawl item {self.cnt} for ssj2008")
