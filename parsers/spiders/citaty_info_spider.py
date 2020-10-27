import json
import os
import scrapy
from scrapy.selector import Selector as S


class CitatyInfoSpider(scrapy.Spider):
    name = "citaty_info"

    def __init__(self):
        self.start_url = "https://citaty.info/category/sarkastichnye-citaty"
        self.directory = 'results/citaty_info'
        self.filename = self.start_url.split('/')[-1] + '.txt'

    def start_requests(self):
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response):
        quotes = response.xpath("//div[contains(@class, 'quotes-row')]")
        next_page = response.xpath("//li[contains(@class, 'pager-next')]/a/@href").get()
        page_url = response.url.split("page=")
        page_number = 1 if len(page_url) == 1 else int(page_url[1]) + 1

        for item in quotes:
            link = S(text=item.extract()).xpath("//a[contains(@class, 'citatyinfo-quote')]/@href").get()
            texts = S(text=item.extract()).xpath("//a[contains(@class, 'citatyinfo-quote')]//text()").extract()
            rating = S(text=item.extract()).xpath("//div[contains(@class, 'rating__value__digits')]//text()").get()
            r_pos = S(text=item.extract()).xpath("//div[contains(@class, 'rating__control-plus')]//text()").get()
            r_neg = S(text=item.extract()).xpath("//div[contains(@class, 'rating__value-negative')]//text()").get()
            submit = S(text=item.extract()).xpath("//div[contains(@class, 'node__submitted')]//text()").extract()
            com_count = S(text=item.extract()).xpath("//span[contains(@class, 'action__count')]//text()").get()

            refs = S(text=item.extract()).xpath("//div[contains(@class, 'field-type-taxonomy-term-reference')]//a")
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
                'link': link,
                'text': ' '.join(' '.join(texts).split()),
                'references': references,
                'tags': tags,
                'rating': ''.join(rating.split()),
                'rating_positive': ''.join(r_pos.split()),
                'rating_negative': ''.join(r_neg.split()),
                'submitted_by': submit[1],
                'submitted_date': submit[2],
                'comments_count': com_count,
            }

            with open(os.path.join(self.directory, self.filename), 'a', encoding='utf8') as f:
                json.dump(scraped_info, f, ensure_ascii=False)
                f.write('\n')

        self.log('Page %s scraped successfully' % page_number)

        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)
