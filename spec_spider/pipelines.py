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
        self.int_rate_file = open('data/cint2017_rate.csv', 'wb')
        self.int_speed_file = open('data/cint2017_speed.csv', 'wb')
        self.fp_rate_file = open('data/cfp2017_rate.csv', 'wb')
        self.fp_speed_file = open('data/cfp2017_speed.csv', 'wb')

        self.int_rate_exporter = CsvItemExporter(self.int_rate_file)
        self.int_speed_exporter = CsvItemExporter(self.int_speed_file)
        self.fp_rate_exporter = CsvItemExporter(self.fp_rate_file)
        self.fp_speed_exporter = CsvItemExporter(self.fp_speed_file)

        self.int_rate_exporter.start_exporting()
        self.int_speed_exporter.start_exporting()
        self.fp_rate_exporter.start_exporting()
        self.fp_speed_exporter.start_exporting()

    def close_siper(self, spider):
        self.int_rate_exporter.finish_exporting()
        self.int_speed_exporter.finish_exporting()
        self.fp_rate_exporter.finish_exporting()
        self.fp_speed_exporter.finish_exporting()

        self.int_rate_file.close()
        self.int_speed_file.close()
        self.fp_rate_file.close()
        self.fp_speed_file.close()

    def process_item(self, item, spider):
        # suite: CINT2017_speed, CINT2017_rate, CFP2017_speed, CFP2017_rate
        cnt_int_rate, cnt_int_speed, cnt_fp_rate, cnt_fp_speed = 0, 0, 0, 0
        if item['Suite'] == 'CINT2017_speed':
            self.int_speed_exporter.export_item(item)
            cnt_int_speed += 1
            spider.logger.info(f"Crawl item {cnt_int_speed} for CINT2017_speed")
        elif item['Suite'] == 'CINT2017_rate':
            self.int_rate_exporter.export_item(item)
            cnt_int_rate += 1
            spider.logger.info(f"Crawl item {cnt_int_rate} for CINT2017_rate")
        elif item['Suite'] == 'CFP2017_speed':
            self.fp_speed_exporter.export_item(item)
            cnt_fp_speed += 1
            spider.logger.info(f"Crawl item {cnt_fp_speed} for CFP2017_speed")
        elif item['Suite'] == 'CFP2017_rate':
            self.fp_rate_exporter.export_item(item)
            cnt_fp_rate += 1
            spider.logger.info(f"Crawl item {cnt_fp_rate} for CFP2017_rate")
