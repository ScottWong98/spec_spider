import re
import scrapy

from spec_spider.utils import delete_tag_and_br, get_detail_url


class Ssj2008Spider(scrapy.Spider):
    name = 'ssj2008'
    allowed_domains = ['spec.org']
    start_urls = ['http://spec.org/power_ssj2008/results/power_ssj2008.html']
    custom_settings = {
        'ITEM_PIPELINES': {'spec_spider.pipelines.Ssj2008Pipeline': 300,}
    }

    def parse(self, response):
        for tr_selector in response.css('tbody tr'):
            url_suffix = tr_selector.css('a::attr(href)').get()
            if url_suffix is None or (url_suffix is not None and len(url_suffix) == 0):
                continue
            detail_url = get_detail_url(response.url, url_suffix)
            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={'url_suffix': url_suffix},
            )

    def parse_detail(self, response, url_suffix):
        if response.css('.noncompliant').get() is not None:
            return {'Status': 'Non-Compliant'}

        suite = delete_tag_and_br(response.css('.benchmarkName::text').get())
        info_dict = self._parse_info(response)
        bm_dict = self._parse_benchmark(response)
        hw_dict, sw_dict = {}, {}
        for tbody in response.css('.configSection tbody'):
            first_key = tbody.css('tr a::text').get()
            keys = tbody.css('tr a::text').getall()
            if len(keys) > 25 and 'Hardware Vendor' in first_key:
                hw_dict = self._parse_hw(tbody)
            elif len(keys) > 15 and 'Power Management' in first_key:
                sw_dict = self._parse_sw(tbody)
        return {
            'Suite': suite,
            **info_dict,
            **bm_dict,
            **hw_dict,
            **sw_dict,
            'URL Suffix': url_suffix,
        }

    def _parse_info(self, response):
        items = response.css('.resultHeader tbody td').getall()[2:]
        items = [re.sub(':', '', delete_tag_and_br(item)) for item in items]
        info_dict = {k: v for k, v in zip(items[::2], items[1::2])}
        info_dict.pop('Test Location', None)
        info_dict.pop('System Source', None)
        info_dict.pop('System Designation', None)
        info_dict.pop('Power Provisioning', None)
        return info_dict

    def _parse_benchmark(self, response):
        trs = response.css('.resultsTable tbody tr')
        bm_dict = {}
        for tr in trs[:10]:
            tds = tr.css('td::text').getall()
            bm_dict[f"ssj_ops @ {tds[0]} of target load"] = tds[2]
            bm_dict[f"Average watts @ {tds[0]} of target load"] = tds[3]
            bm_dict[f"Performance/power @ {tds[0]} of target load"] = tds[4]
        bm_dict[f"Average watts @ active idle"] = trs[10].css('td::text').getall()[1]
        bm_dict["Benchmark"] = trs[11].css('td::text').getall()[-1]
        return bm_dict

    def _parse_hw(self, tbody):
        keys = tbody.css('tr a::text').getall()
        values = tbody.css('tr td::text').getall()
        keys = [re.sub(':', '', key) for key in keys]
        hw_dict = {k: v for k, v in zip(keys, values)}
        hw_dict.pop('Keyboard', None)
        hw_dict.pop('Mouse', None)
        hw_dict.pop('Monitor', None)
        hw_dict.pop('Optical Drives', None)
        return hw_dict

    def _parse_sw(self, tbody):
        keys = tbody.css('tr a::text').getall()
        values = tbody.css('tr td::text').getall()
        keys = [re.sub(':', '', key) for key in keys]
        sw_dict = {k: v for k, v in zip(keys, values)}
        sw_dict.pop('JVM Command-line Options', None)
        sw_dict.pop('JVM Affinity', None)
        return sw_dict

