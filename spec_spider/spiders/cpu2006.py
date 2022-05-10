import scrapy

from spec_spider.utils import delete_tag_and_br, get_detail_url


class Cpu2006Spider(scrapy.Spider):
    name = 'cpu2006'
    allowed_domains = ['spec.org']
    start_urls = [
        'http://spec.org/cpu2006/results/cint2006.html',
        'http://spec.org/cpu2006/results/rint2006.html',
        'http://spec.org/cpu2006/results/cfp2006.html',
        'http://spec.org/cpu2006/results/rfp2006.html',
    ]
    custom_settings = {
        'ITEM_PIPELINES': {'spec_spider.pipelines.Cpu2006Pipeline': 300,}
    }

    def parse(self, response):
        # suite: SPECint, SPECint_rate, SPECfp, SPECfp_rate
        suite = response.css('.idx_table h2 a::attr(name)').get()
        for tr_selector in response.css('tbody tr'):
            url_suffix = tr_selector.css('a::attr(href)').get()
            if url_suffix is None or (url_suffix is not None and len(url_suffix) == 0):
                continue
            detail_url = get_detail_url(response.url, url_suffix)
            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={'url_suffix': url_suffix, 'suite': suite},
            )

    def parse_detail(self, response, url_suffix, suite):

        info_dict = self._parse_info(response)
        benchmark_dict = self._parse_benchmark(response, suite)
        return {'Suite': suite, **info_dict, **benchmark_dict, 'URL Suffix': url_suffix}

    def _parse_hw_info(self, response):
        hw_keys = [
            'CPU Name',
            'CPU Characteristics',
            'CPU MHz',
            'FPU',
            'CPU(s) enabled',
            'CPU(s) orderable',
            'Primary Cache',
            'Secondary Cache',
            'L3 Cache',
            'Other Cache',
            'Memory',
            'Disk Subsystem',
            'Other Hardware',
        ]
        hw_values = [
            delete_tag_and_br(value)
            for value in response.css('#Hardware tbody td').getall()
        ]
        hw_values = hw_values[: len(hw_keys)]
        return {k: v for k, v in zip(hw_keys, hw_values)}

    def _parse_sw_info(self, response):
        sw_keys = [
            'OS',
            'Compiler',
            'Parallel',
            'File System',
            'System State',
            'Base Pointers',
            'Peak Pointers',
            'Other Software',
        ]
        sw_values = [
            delete_tag_and_br(value)
            for value in response.css('#Software tbody td').getall()
        ]
        sw_values = sw_values[: len(sw_keys)]
        return {k: v for k, v in zip(sw_keys, sw_values)}

    def _parse_info(self, response):
        system_bar = [
            delete_tag_and_br(item) for item in response.css('.systembar p').getall()
        ]
        hw_dict = self._parse_hw_info(response)
        sw_dict = self._parse_sw_info(response)
        return {
            'Hardware Vendor': system_bar[0],
            'System Name': system_bar[1],
            'Metric': response.css('.metricbar#base a::text').get(),
            'Baseline': response.css('.metricbar#base span.value::text').get(),
            'License': response.css('#license_num_val::text').get(),
            'Test Sponsor': response.css('#test_sponsor_val::text').get(),
            'Tested By': response.css('#tester_val::text').get(),
            'Test Date': response.css('#test_date_val::text').get(),
            'HW Avail': response.css('#hw_avail_val::text').get(),
            'SW Avail': response.css('#sw_avail_val::text').get(),
            **hw_dict,
            **sw_dict,
        }

    def _parse_benchmark(self, response, suite):
        benchmark_dict = {
            k: v
            for k, v in zip(
                response.css('.resultstable tbody td.bm a::text').getall(),
                response.css('td.basecol.ratio.selected span.selected::text').getall(),
            )
        }
        if suite[-4:] == 'rate':
            base_copies = response.css('td.basecol.bm::text').get()
            benchmark_dict['Base Copies'] = base_copies
        return benchmark_dict

