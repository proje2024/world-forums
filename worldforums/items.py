# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ForumItem(scrapy.Item):
    type = scrapy.Field()
    source = scrapy.Field()
    provider = scrapy.Field()
    identifier = scrapy.Field()
    created_date = scrapy.Field()
    scraped_date = scrapy.Field()
    metadata = scrapy.Field()