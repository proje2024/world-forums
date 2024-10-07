import scrapy
from scrapy import signals
from scrapy.exceptions import NotConfigured
import time
from datetime import datetime
import re
import hashlib
from langdetect import detect
from dateutil import parser as date_parser
import pytz
from worldforums.items import ForumItem

# 3 hours, 20 minutes, 39 seconds

class WorldForum(scrapy.Spider):
    name = "arnavutluk_spider"
    allowed_domains = ["forum-al.com"]
    start_urls = ["https://www.forum-al.com/"]

    def __init__(self, *args, **kwargs):
        super(WorldForum, self).__init__(*args, **kwargs)
        self.start_time = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WorldForum, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_opened(self, spider):
        self.start_time = time.time()
        self.logger.info(
            "********************************Program Başladı********************************\n: %s",
            spider.name,
        )

    def spider_closed(self, spider):
        end_time = time.time()
        elapsed_time = end_time - self.start_time
        elapsed_hours, rem = divmod(elapsed_time, 3600)
        elapsed_minutes, elapsed_seconds = divmod(rem, 60)
        self.logger.info("Spider closed: %s", spider.name)
        self.logger.info(
            "Elapsed Time: %d hours, %d minutes, %d seconds",
            elapsed_hours,
            elapsed_minutes,
            elapsed_seconds,
        )
        print(
            f"***************************Program Sonlandı********************\n: {spider.name}"
        )
        print(
            f"*********************Çalışma Süresi*********************\n: {int(elapsed_hours)} hours, {int(elapsed_minutes)} minutes, {int(elapsed_seconds)} seconds"
        )

    def generate_unique_id(self, identifier):
        sha_signature = hashlib.sha256(identifier.encode()).hexdigest()
        return sha_signature
    
    def parse(self, response):
        categories = response.xpath('//div[@class="block-container"]//h3[@class="node-title"]/a')
                
        for category in categories:  # kategorileri dolaşma
            category_title = category.xpath("text()").get()
            category_url = response.urljoin(category.xpath("@href").get())
            yield scrapy.Request(
                url=category_url,
                callback=self.parse_category,
                meta={"category_title": category_title},
            )

    def parse_category(self, response):
        category_title = response.meta["category_title"]

        topics = response.xpath('//div[@class="structItem-title"]//a')
        for topic in topics:
            topic_title = topic.xpath("text()").get()
            topic_url = response.urljoin(topic.xpath("@href").get())
            yield scrapy.Request(
                url=topic_url,
                callback=self.parse_topic,
                meta={
                    "category_title": category_title,
                    "topic_title": topic_title,
                },
            )

        current_page_number = response.url.split("/")[-1]
        if "page-" in current_page_number:
            current_page_number = int(current_page_number.replace("page-", ""))
        else:
            current_page_number = 1

        next_page_number = current_page_number + 1
        base_url = response.url.split("/page-")[0]
        if current_page_number == 1:
            next_page_url = f"{base_url}page-{next_page_number}"
        else:
            next_page_url = f"{base_url}/page-{next_page_number}"

        if response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]'):
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_category,
                meta={
                    "category_title": category_title,
                },
            )

    def parse_topic(self, response):
        category_title = response.meta["category_title"]
        topic_title = response.meta["topic_title"]

        posts = response.xpath('//div[contains(@class, "message-inner")]')
        if posts:
            metadata_list = []

            created_date_str = (
                    response.xpath(
                        '//a[@class="u-concealed"]/time[@class="u-dt"]/@datetime'
                    ).get()
                    or "unknown"
                )
            if created_date_str != "unknown":
                created_date = date_parser.parse(created_date_str)
                created_date = created_date.astimezone(pytz.utc).isoformat()
            else:
                created_date = "unknown"

            for post in posts:
                content = (
                    " ".join(
                        post.xpath(
                            './/div[contains(@class, "bbWrapper")]/descendant-or-self::*/text()'
                        ).extract()
                    ).strip()
                    or ""
                )
                if content:
                    content = (
                        content.strip()
                        .replace("\r", "")
                        .replace("\n", "")
                        .replace("\t", "")
                    )

                author = (
                    post.xpath(
                        './/div[@class="message-userDetails"]//span/text()'
                    ).get()
                    or "unknown"
                )
                if author != "unknown":
                    author = (
                        author.strip()
                        .replace("\r", "")
                        .replace("\n", "")
                        .replace("\t", "")
                    )

                avatar_image = (
                    post.xpath(
                        './/a[contains(@class, "avatar avatar--m")]/img/@src'
                    ).get()
                    or ""
                )
                img_sources = response.xpath(
                    '//div[contains(@class, "bbImageWrapper  js-lbImage")]//img/@src'
                ).extract()

                media_links = (
                    [avatar_image] + img_sources if avatar_image else img_sources
                )

                try:
                    if content.strip():
                        lang = detect(content)
                    elif topic_title.strip():
                        lang = detect(topic_title)
                    elif topic_title.strip():
                        lang = detect(topic_title)
                    else:
                        lang = detect(category_title)
                except:
                    lang = "unknown"
                
                metadata={
                        "category_title": category_title,
                        "topic_title": topic_title,
                        "content": content,
                        "author": author,
                        "media_links": media_links,
                        "lang": lang,
                        "country": "al",
                    },
                
                metadata_list.append(metadata)
                
            forum_item = ForumItem(
                    type="forum",
                    source="forum-al",
                    provider="scrapy",
                    identifier=self.generate_unique_id(f"{topic_title}-{created_date_str}"),
                    created_date=created_date,
                    scraped_date=datetime.now(pytz.utc).isoformat(),
                    metadata = metadata_list
                )
            
            yield forum_item

        current_page_number = response.url.split("/")[-1]
        if "page-" in current_page_number:
            current_page_number = int(current_page_number.replace("page-", ""))
        else:
            current_page_number = 1

        next_page_number = current_page_number + 1
        base_url = response.url.split("/page-")[0]
        if current_page_number == 1:
            next_page_url = f"{base_url}page-{next_page_number}"
        else:
            next_page_url = f"{base_url}/page-{next_page_number}"

        if response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]'):
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_topic,
                meta={
                    "category_title": category_title,
                    "topic_title": topic_title,
                },
            )