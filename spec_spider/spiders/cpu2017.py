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
        # suite: CINT2017_speed, CINT2017_rate, CFP2017_speed, CFP2017_rate
        suite = response.css('.idx_table h2 a::attr(name)').get()
        for tr_selector in response.css('tr.historical')[:1]:
            url_suffix = tr_selector.css('a::attr(href)').get()
            detail_url = get_detail_url(response.url, url_suffix)
            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={'url_suffix': url_suffix, 'suite': suite},
            )

    def parse_detail(self, response, url_suffix, suite):

        info_dict = self._parse_info(response)
        benchmark_dict = self._parse_benchmark(response, suite)
        return {
            'Suite': suite,
            **info_dict,
            **benchmark_dict,
            'URL Suffix': url_suffix
        }


    def _parse_info(self, response):
        system_bar = [
            delete_tag_and_br(item) for item in response.css('td.systembar p').getall()
        ]
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
        return {
            'Hardware Vendor': system_bar[0],
            'System Name': system_bar[1],
            'Metric': response.css('td.metricbar.base a::text').get(),
            'Baseline': response.css('td.metricbar.base span.value::text').get(),
            'License': response.css('#license_num_val::text').get(),
            'Test Sponsor': response.css('#test_sponsor_val::text').get(),
            'Tested By': response.css('#tester_val::text').get(),
            'Test Date': response.css('#test_date_val::text').get(),
            'HW Avail': response.css('#hw_avail_val::text').get(),
            'SW Avail': response.css('#sw_avail_val::text').get(),
            **hardware_dict,
            **software_dict,
        }

    def _parse_benchmark(self, response, suite):
        benchmark_dict = {
            k: v
            for k, v in zip(
                response.css('.resultstable tbody td.bm a::text').getall(),
                response.css('td.basecol.ratio.selected span.selected::text').getall(),
            )
        }
        if suite.split('_')[-1] == 'rate':
            base_copies = response.css('td.basecol.copies::text').get()
            benchmark_dict['Base Copies'] = base_copies
        else:
            base_threads = response.css('td.basecol.threads::text').get()
            benchmark_dict['Base Threads'] = base_threads
        return benchmark_dict
