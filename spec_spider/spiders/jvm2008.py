import scrapy

from spec_spider.utils import delete_tag_and_br, get_detail_url


class Jvm2008Spider(scrapy.Spider):
    name = 'jvm2008'
    allowed_domains = ['spec.org']
    start_urls = ['http://spec.org/jvm2008/results/jvm2008.html']
    custom_settings = {
        'ITEM_PIPELINES': {'spec_spider.pipelines.Jvm2008Pipeline': 300,}
    }

    def parse(self, response):
        suite = response.css('.idx_table h2 a::attr(name)').get()
        for tr_selector in response.css('tbody tr'):
            url_suffix = tr_selector.css('a::attr(href)').get()
            if url_suffix is None or (url_suffix is not None and len(url_suffix) == 0):
                continue
            detail_url = get_detail_url(response.url, url_suffix)

            yield scrapy.Request(
                detail_url,
                callback=self.parse_detail,
                cb_kwargs={'url_suffix': url_suffix, 'suite': suite,},
            )

    def parse_detail(self, response, url_suffix, suite):
        trs = response.css('tbody tr')
        href = trs[-2].css('a::attr(href)').get()
        result = trs[-4].css('td::text').getall()[-1]
        if href is None:
            href = trs[-1].css('a::attr(href)').get()
            result = trs[-3].css('td::text').getall()[-1]

        detail_url = get_detail_url(response.url, href[2:])
        yield scrapy.Request(
            detail_url,
            callback=self.parse_detail_2,
            cb_kwargs={'url_suffix': url_suffix, 'suite': suite, 'result': result},
        )

    def parse_detail_2(self, response, url_suffix, suite, result):
        selectors = response.css('table tbody table')
        info_dict = self._parse_info(selectors[0])
        sw_dict = self._parse_sw(selectors[1])
        jvm_dict = self._parse_jvm(selectors[2])
        hw_dict = self._parse_hw(selectors[3])
        return {
            'Suite': suite,
            'Result': result.split(' ')[0],
            **info_dict,
            **hw_dict,
            **sw_dict,
            **jvm_dict,
            'URL Suffix': url_suffix,
        }

    def _parse_info(self, selector):
        table = self._parse_table(selector)
        table.pop('Submitter URL', None)
        table.pop('Tester', None)
        table.pop('Location', None)
        return table

    def _parse_hw(self, selector):
        table = self._parse_table(selector)
        table.pop("HW vendor's URL", None)
        table.pop("CPU vendor's URL", None)
        return table

    def _parse_sw(self, selector):
        table = self._parse_table(selector)
        table.pop('OS address bits', None)
        table.pop('OS tuning', None)
        table.pop('Other s/w name', None)
        table.pop('Other s/w tuning', None)
        table.pop('Other s/w available', None)
        return table

    def _parse_jvm(self, selector):
        table = self._parse_table(selector)
        table['JVM Vendor'] = table.pop('Vendor', None)
        pop_fields = [
            'Vendor URL',
            'Java Specification',
            'JVM address bits',
            'JVM initial heap memory',
            'JVM maximum heap memory',
            'JVM command line',
            'JVM command line startup',
            'JVM launcher startup',
            'Additional JVM tuning',
            'JVM class path',
            'JVM boot class path',
        ]
        [table.pop(field, None) for field in pop_fields]
        return table

    def _parse_table(self, selector):
        items = [
            delete_tag_and_br(item) for item in selector.css('tbody tr td').getall()
        ]
        return {k: v for k, v in zip(items[::2], items[1::2])}
