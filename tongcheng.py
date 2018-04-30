
import requests
import time
import random
import queue
import hashlib
import pymongo
from lxml import etree
from setting import *

# 连接mongodb
MONGO_CLIENT = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
MONGO_DB = MONGO_CLIENT[MONGO_DATABASE]
MONGO_COLL = MONGO_DB[MONGO_COllECTION]


class WubaSpider(object):
    start_url = 'http://bj.58.com/chuzu/pn1'

    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': random.choice(USER_AGENTS),
            'Connection': 'keep-alive'
        }
        self.urls_queue = queue.Queue()
        self.urls_set = set()
        self.page_urls_queue = queue.Queue()

    def page_request(self):
        """首次请求解析下一页url及房源信息url"""
        page_url = self.page_urls_queue.get()
        if page_url is not None:
            print(page_url)
            response = requests.get(page_url, headers=self.headers)
        else:
            print(self.start_url)
            response = requests.get(self.start_url, headers=self.headers)

        print('请求完成')
        time.sleep(random.random()*3)
        html = etree.HTML(response.text)
        # 房源urls
        house_urls = html.xpath(r'//div[@class="des"]/h2/a/@href')
        # 下一页urls
        page_url_list = html.xpath(r'//div[@class="pager"]/a/@href')
        print('解析完成')

        for each in house_urls:
            # 将url加密
            h_url = self.encryption(each)
            if h_url not in self.urls_set:
                self.urls_set.add(h_url)
        for each in page_url_list:
            # 将url进行加密
            h_url = self.encryption(each)
            if h_url not in self.urls_set:
                self.urls_set.add(h_url)
                self.page_urls_queue.put(each)
                self.urls_queue.put(each)
        print(self.urls_queue)
        print(self.page_urls_queue)

    @classmethod
    def encryption(cls, url):
        """将传进来的url进行md5加密，返回加密后的url"""
        h_url = hashlib.md5()
        h_url.update(url.encode('utf-8'))
        return h_url.hexdigest()

    def house_info_request(self):
        house_url = self.urls_queue.get()
        response = requests.get(house_url, headers=self.headers)
        # 每次请求间隔0-3秒
        time.sleep(random.random()*3)
        html = etree.HTML(response.text)
        # 解析房屋信息
        house_title = html.xpath(r'//div[@class="house-title"]/h1/text()')
        house_updata = html.xpath(r'//div[@class="house-title"]/p[@class="house-update-info c_888 f12"]/text()')
        house_price = html.xpath(r'//div[@class="house-desc-item fl c_333"]/div[@class="house-pay-way f16"]/span/b/text()').append('元/月')
        pay_style = html.xpath(r'//div[@class="house-desc-item fl c_333"]/div[@class="house-pay-way f16"]/span[2]/text()')
        rent_style = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[1]/span[2]/text()')
        house_type = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[2]/span[2]/text()')
        house_floor = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[3]/span[2]/text()')
        house_community = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[4]/span[2]/a/text()')
        house_area = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[5]/span[2]/a/text()')
        house_addr = html.xpath(r'//div[@class="house-desc-item fl c_333"]/ul/li[6]/span[2]/text()')
        phone_num = html.xpath(r'//div[@class="house-chat-phone"]/span/text()')
        house_disposal = html.xpath(r'//div[@class="main-detail-info fl"]/ul[@class="house-disposal"]/li/text()')
        house_introduce = html.xpath(r'//div[@class="house-word-introduce f16 c_555"]/ul[@class="introduce-item"]/li[1]/span[2]/em/text()')
        house_detail = html.xpath(r'//div[@class="house-word-introduce f16 c_555"]/ul[@class="introduce-item"]/li[2]/span[2]/span/strong/text()')
        if house_detail is None:
            house_detail = html.xpath(r'//div[@class="house-word-introduce f16 c_555"]/ul[@class="introduce-item"]/li[2]/span[2]/text()')
        if house_detail is None:
            house_detail = html.xpath(r'//div[@class="house-word-introduce f16 c_555"]/ul[@class="introduce-item"]/li[2]/span[2]/p/text()')

        item = {
            'house_title': house_title,
            'house_update': house_updata,
            'house_price': house_price,
            'pay_style': pay_style,
            'rent_style': rent_style,
            'house_type': house_type,
            'house_floor': house_floor,
            'house_community': house_community,
            'house_area': house_area,
            'house_addr': house_addr,
            'phone_num': phone_num,
            'house_disposal': house_disposal,
            'house_introduce': house_introduce,
            'house_detail': house_detail,
            'house_link': response.url
        }
        return item


class SaveDate(object):
    """保存相关数据"""
    @classmethod
    def save_data(cls, item):
        MONGO_COLL.insert(item)


def main():
    print('--------爬虫开始--------')
    crawl = WubaSpider()
    crawl.page_request()
    # 爬取所有页面网页url和房屋信息url
    page_count = 0
    while crawl.page_urls_queue.empty():
        crawl.page_request()
        print('已爬取网页：%s' % page_count)
        page_count += 1

    # 爬取所有房屋信息url
    count = 0
    while crawl.urls_queue.empty():
        item = crawl.house_info_request()
        SaveDate.save_data(item)
        print('已保存%s条数据' % count)
        count += 1


if __name__ == '__main__':
    # main()
    crawl = WubaSpider()
    crawl.page_request()
