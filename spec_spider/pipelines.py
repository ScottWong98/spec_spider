# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter


class SpecSpiderPipeline:
    def process_item(self, item, spider):
        return item


class Cpu2017Pipeline:

    def __init__(self):
        self.file1 = open('detail.csv', 'wb')
        self.exporter1 = CsvItemExporter(self.file1)
        self.exporter1.start_exporting()

        self.file2 = open('benchmark.csv', 'wb')
        self.exporter2 = CsvItemExporter(self.file2)
        self.exporter2.start_exporting()
    
    def close_siper(self, spider):
        self.exporter1.finish_exporting()
        self.file1.close()
        self.exporter2.finish_exporting()
        self.file2.close()

    def process_item(self, item, spider):
        if item['type'] == 'detail':
            self._process_detail_item(item, spider)
        else:
            self._process_benchmark_item(item, spider)

    def _process_detail_item(self, item, spider):
        self.exporter1.export_item(item)

    def _process_benchmark_item(self, item, spider):
        self.exporter2.export_item(item)
