import scrapy

from spec_spider.utils import delete_tag_and_br, get_detail_url


class Cpu2017Spider(scrapy.Spider):
    name = 'cpu2017'
    allowed_domains = ['spec.org']
    start_urls = [
        'https://spec.org/cpu2017/results/cint2017.html',
        'https://spec.org/cpu2017/results/rint2017.html',
        'https://spec.org/cpu2017/results/cfp2017.html',
        'https://spec.org/cpu2017/results/rfp2017.html',
    ]
    custom_settings = {
        'ITEM_PIPELINES': {'spec_spider.pipelines.Cpu2017Pipeline': 300,}
    }

    def parse(self, response):
        for tr_selector in response.css('tr.historical')[:1]:
            url_suffix = tr_selector.css('a::attr(href)').get()
            detail_url = get_detail_url(response.url, url_suffix)
            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={'url_suffix': url_suffix,},
            )

    def parse_detail(self, response, url_suffix):
        system_bar = [
            delete_tag_and_br(item) for item in response.css('td.systembar p').getall()
        ]
        vendor = system_bar[0]
        system_name = system_bar[1]

        suite_name = response.css('td.metricbar.base a::text').get()
        suite_value = response.css('td.metricbar span.value::text').get()
        spec_license = response.css('#license_num_val::text').get()

        hardware_dict = {
            k: delete_tag_and_br(v)
            for k, v in zip(
                response.css('#Hardware tbody a::text').getall(),
                response.css('#Hardware tbody td').getall(),
            )
        }
        hardware_dict.pop('Other', None)
        software_dict = {
            k: delete_tag_and_br(v)
            for k, v in zip(
                response.css('#Software tbody a::text').getall(),
                response.css('#Software tbody td').getall(),
            )
        }
        software_dict.pop('Other', None)
        software_dict.pop('Peak Pointers', None)

        yield {
            'type': 'detail',
            'vendor': vendor,
            'system_name': system_name,
            suite_name: suite_value,
            'spec_license': spec_license,
            **hardware_dict,
            **software_dict,
            'url_suffix': url_suffix,
        }
        for tr in response.css('.resultstable tbody tr'):
            benchmark_name = tr.css('td.bm a::text').get()
            threads_or_copies = tr.css('td.basecol::text').get()
            seconds = tr.css('td.time span.selected::text').get()
            ratio = tr.css('td.ratio span.selected::text').get()
            yield {
                'type': 'benchmark',
                'suite_name': suite_name,
                'system_name': system_name,
                'benchmark_name': benchmark_name,
                'threads_or_copies': threads_or_copies,
                'seconds': seconds,
                'ration': ratio,
            }
