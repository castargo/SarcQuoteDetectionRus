import json
import os
import re
import scrapy
from scrapy.selector import Selector as S


class CitatyInfoQuoteByQuoteSpider(scrapy.Spider):
    name = "citaty_info_qbq"

    def __init__(self):
        self.first_quote = 560001
        self.last_quote = 561161
        self.directory = 'results/citaty_info/qbq'
        self.filename = 'qbq_%s_%s.txt' % (self.first_quote, self.last_quote)

    def start_requests(self):
        for quote in range(self.first_quote, self.last_quote + 1):
            url = "https://citaty.info/quote/%s" % quote
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            all_info = response.xpath("//article[contains(@class, 'node-full')]")[0]

            texts = S(text=all_info.extract()).xpath("//div[contains(@class, 'field-name-body')]//text()").extract()
            rating = S(text=all_info.extract()).xpath("//div[contains(@class, 'rating__value__digits')]//text()").get()
            r_pos = S(text=all_info.extract()).xpath("//div[contains(@class, 'rating__control-plus')]//text()").get()
            r_neg = S(text=all_info.extract()).xpath("//div[contains(@class, 'rating__value-negative')]//text()").get()
            submit = S(text=all_info.extract()).xpath("//div[contains(@class, 'node__submitted')]//text()").extract()

            com_block = response.xpath("//div[contains(@id,'block-system-main')]//div[contains(@id, 'comments')]").get()
            com_tags = re.findall(r'article', com_block)

            refs = S(text=all_info.extract()).xpath("//div[contains(@class, 'field-type-taxonomy-term-reference')]//a")
            references = dict()
            tags = []
            for ref in refs:
                title = S(text=ref.extract()).xpath("//@title").get()
                text = S(text=ref.extract()).xpath("//text()").get()
                if title:
                    if title not in references.keys():
                        references[title] = text
                    else:
                        if type(references[title]) is str:
                            references[title] = [references[title], text]
                        elif type(references[title]) is list:
                            references[title] = str(' / '.join(references[title]) + ' / ' + text).split('/')
                        else:
                            raise AssertionError('Variables should be string or list')
                else:
                    tags.append(text)

            scraped_info = {
                'link': response.url,
                'text': ' '.join(' '.join(texts).split()),
                'references': references,
                'tags': tags,
                'rating': ''.join(rating.split()),
                'rating_positive': ''.join(r_pos.split()),
                'rating_negative': ''.join(r_neg.split()),
                'submitted_by': submit[1],
                'submitted_date': submit[2],
                'comments_count': len(com_tags) // 2,
            }

            with open(os.path.join(self.directory, self.filename), 'a', encoding='utf8') as f:
                json.dump(scraped_info, f, ensure_ascii=False)
                f.write('\n')

            self.log('Page %s scraped successfully' % response.url.split('/')[-1])

        except Exception:
            self.log('An error was found on the page %s' % response.url.split('/')[-1])
