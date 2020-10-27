import json
import os
import re
import scrapy
from scrapy.selector import Selector as S


class BBFSpider(scrapy.Spider):
    name = "bbf"

    def __init__(self):
        self.first_page = 1
        self.last_page = 3000
        self.directory = 'results/bbf'
        self.filename = 'bbf_%s_%s.txt' % (self.first_page, self.last_page)

    def start_requests(self):
        for page in range(self.first_page, self.last_page + 1):
            url = "https://bbf.ru/quotes/?page=%s" % page
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        quotes = response.xpath("//article[contains(@class, 'sentence')]")
        page = response.url.split("page=")[1]

        for item in quotes:
            try:
                raw_quote = S(text=item.extract()).xpath("//div[contains(@class, 'sentence__body')]/p/text()").get()
                if raw_quote:
                    quote = ' '.join(raw_quote.split())
                else:
                    quote = S(text=item.extract()).xpath("//div[contains(@class, 'sentence__body')]/p").get()

                scraped_info = {
                    'quote':  quote,
                }

                other_links = S(text=item.extract()).xpath("//a/@href").extract()
                other_tags = [re.findall(r'\?(\w*)\=', link)[0] for link in other_links]
                other_texts = S(text=item.extract()).xpath("//a/text()").extract()

                for ind, tag in enumerate(other_tags):
                    if tag not in scraped_info.keys():
                        scraped_info[tag] = other_texts[ind]
                    else:
                        if type(scraped_info[tag]) is str:
                            scraped_info[tag] = [scraped_info[tag], other_texts[ind]]
                        elif type(scraped_info[tag]) is list:
                            scraped_info[tag] = str(' / '.join(scraped_info[tag]) + ' / ' + other_texts[ind]).split('/')
                        else:
                            raise AssertionError('Variables should be string or list')

                with open(os.path.join(self.directory, self.filename), 'a', encoding='utf8') as f:
                    json.dump(scraped_info, f, ensure_ascii=False)
                    f.write('\n')

            except Exception:
                self.log('An error was found on the page %s' % page)

        self.log('Page %s scraped successfully' % page)
