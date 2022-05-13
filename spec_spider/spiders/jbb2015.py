import scrapy
from scrapy.selector import SelectorList

from spec_spider.utils import delete_tag_and_br, get_detail_url


class Jbb2015Spider(scrapy.Spider):
    name = 'jbb2015'
    allowed_domains = ['spec.org']
    start_urls = [
        'http://spec.org/jbb2015/results/jbb2015composite.html',
        'http://spec.org/jbb2015/results/jbb2015multijvm.html',
        'http://spec.org/jbb2015/results/jbb2015distributed.html',
    ]
    custom_settings = {
        'ITEM_PIPELINES': {'spec_spider.pipelines.Jbb2015Pipeline': 300,}
    }

    def parse(self, response):
        suite = response.css('.idx_table h2 a::attr(name)').get()
        for tr_selector in response.css('tbody tr'):
            url_suffix = tr_selector.css('a::attr(href)').get()
            if url_suffix is None or (url_suffix is not None and len(url_suffix) == 0):
                continue
            max_jOPS, critical_jOPS = tr_selector.css('td::text').getall()[-2:]
            detail_url = get_detail_url(response.url, url_suffix)

            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={
                    'url_suffix': url_suffix,
                    'suite': suite,
                    'max_jOPS': max_jOPS,
                    'critical_jOPS': critical_jOPS,
                },
            )

    def parse_detail(self, response, url_suffix, suite, max_jOPS, critical_jOPS):
        top_bar_dict = {
            k: v
            for k, v in zip(
                response.css('.section.mainDesc tr')[1:].css('td a::text').getall(),
                response.css('.section.mainDesc tr')[1:].css('td::text').getall(),
            )
        }

        overall_dict = self.parse_overall_sut(response)
        hw_dict = self.parse_hw(response)
        sw_dict = self.parse_sw(response)

        return {
            'Suite': suite,
            'max_jOPS': max_jOPS,
            'cirtical_jOPS': critical_jOPS,
            **top_bar_dict,
            **overall_dict,
            **hw_dict,
            **sw_dict,
            'URL Suffix': url_suffix,
        }

    def parse_overall_sut(self, response):
        selector = response.css('table.alternate')[1].css('tr')
        trs = SelectorList()
        [trs.append(selector[idx]) for idx in [0, 4, 6, 8, 9, 10, 11, 12]]
        return {
            k: v
            for k, v in zip(trs.css('a::text').getall(), trs.css('td::text').getall())
        }

    def parse_hw(self, response):
        selector = response.css('table.alternate')[2].css('tr')
        trs = SelectorList()
        indices = [i for i in range(29)]
        indices = indices[1:2] + indices[6:16] + indices[17:]
        [trs.append(selector[idx]) for idx in indices]
        hw_dict = {
            k: v
            for k, v in zip(trs.css('a::text').getall(), trs.css('td::text').getall())
        }
        hw_dict['System Name'] = hw_dict.pop('Name')
        return hw_dict

    def parse_sw(self, response):
        os_dict = self.get_dict(response, 3, [1, 2, 4])
        os_dict['OS Name'] = os_dict.pop('Name')
        os_dict['OS Vendor'] = os_dict.pop('Vendor')
        os_dict['OS Version'] = os_dict.pop('Version')

        jvm_dict = self.get_dict(response, 3, [9, 10, 12])
        jvm_dict['JVM Name'] = jvm_dict.pop('Name')
        jvm_dict['JVM Vendor'] = jvm_dict.pop('Vendor')
        jvm_dict['JVM Version'] = jvm_dict.pop('Version')

        return {**os_dict, **jvm_dict}

    def get_dict(self, response, sid, tr_ids):
        selector = response.css('table.alternate')[sid].css('tr')
        trs = SelectorList()
        [trs.append(selector[idx]) for idx in tr_ids]
        return {
            k: v
            for k, v in zip(trs.css('a::text').getall(), trs.css('td::text').getall())
        }

