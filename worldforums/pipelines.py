# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import json
from dotenv import load_dotenv

class WorldforumsPipeline:
    def process_item(self, item, spider):
        return item

class CustomJsonWriterPipeline:
    def __init__(self):
        self.file = None
        self.file_number = 1
        self.max_file_size = 50 * 1024 * 1024

    def open_spider(self, spider):
        load_dotenv()
        base_path = os.getenv("OUTPUT_PATH")
        self.base_filename = base_path.replace('.json', '')
        self.current_filename = f"{self.base_filename}_{self.file_number}.json"
        self.file = open(self.current_filename, 'w', encoding='utf-8', buffering=1)

    def close_spider(self, spider):
        if self.file:
            self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False)
        self.file.write(line + '\n')
        self.file.flush()
        
        if self.file.tell() >= self.max_file_size:
            self.file.close()
            self.file_number += 1
            self.current_filename = f"{self.base_filename}_{self.file_number}.json"
            self.file = open(self.current_filename, 'w', encoding='utf-8', buffering=1)
            
        return item