import scrapy


class SingerItem(scrapy.Item):
    name = scrapy.Field()
    alias = scrapy.Field()
    avatar = scrapy.Field()
    description = scrapy.Field()
    remote_id = scrapy.Field()
    source = scrapy.Field()
