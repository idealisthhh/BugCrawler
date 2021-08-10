# -*- coding: utf-8 -*-
"""
# @Time : 2019/8/12 9:43
# @Author : Lee
# @Github : harveyleeh
"""
import requests as rq
import re
import pandas as pd
import threading
import time
from bs4 import BeautifulSoup
from queue import Queue
from functools import wraps

# request中header设置
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/75.0.3770.100 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate",
    "Upgrade-Insecure-Requests": "1"
}
# re正则匹配中各项的匹配规则
pats = {
    "number": '#[0-9]*',
    "affect1": 'sprite (.*<)?',
    "affect2": '>.*<?',
    "status": 'value status[A-Z]*',
    "tasks": 'tasksummary[0-9]+'
}
url = "https://bugs.launchpad.net/ubuntu"
urls = Queue()  # 记录URL
names = ['Number', 'Package', 'Title', 'Reporter', 'Affects', 'Importance', 'Status', 'Heat', 'Description']  # 信息名称
bug_data = pd.DataFrame(columns=names)  # 信息处理及写入
count = 0


# 继承构建自己的多线程类，继承父类threading.Thread
class Mythread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print("Starting " + self.name + '\n')
        get_all_details(self.name)
        print("Exiting " + self.name)


def get_url(url_in):
    # 获取当前页的url地址
    req = rq.get(url_in, headers=headers, timeout=60)
    soup = BeautifulSoup(req.text, 'lxml')
    # urls_new = soup.select('a[class= "bugtitle"]')
    urls_new = soup.find_all("a", class_="bugtitle")
    for url_ in urls_new:
        urls.put(url_['href'])

    return soup


def get_url_next(soup):
    # 获取下一页的url地址
    url_next = soup.find("a", text="Next")['href']
    get_url(url_next)


def get_pages(num):
    # 设置所需获取的页数, 从1开始, 1表示第一页
    print('Pages getting starts.')
    print('page: 1')
    soup = get_url(url)
    for i in range(num-1):
        print('page: ' + str(i+2))
        get_url_next(soup)
    print('Pages getting ends')


def get_html(url_in):
    req = rq.get(url_in, headers=headers, timeout=60)
    rq.adapters.DEFAULT_RETRIES = 5
    try:
        soup = BeautifulSoup(req.text, 'lxml')
    except ConnectionError:
        print("Information getting failed")

    return soup


def get_details(soup, sleep_time=5):
    # 获取单页数据, sleep_time为每两次获取之间的间隔时间
    ####################################################################################################################
    global count
    count += 1
    ####################################################################################################################
    number_details = soup.find('div', class_="registering")
    package_details = soup.find('h2', id="watermark-heading")
    title_details = soup.find('span', class_="yui3-editable_text-text ellipsis")
    reporter_details = soup.find('a', class_="sprite person")
    heat_details = soup.find('a', class_="sprite flame")
    description_details = soup.find('div', class_="yui3-editable_text-text")
    # 通过id="affected-software"来获取Status和Importance
    tasks_details = soup.find('tbody')
    tasksummary_list = []
    for line in str(tasks_details).splitlines():
        result = re.search(pats["tasks"], line)
        if result is not None and result.group() not in tasksummary_list:
            tasksummary_list.append(result.group())
    # ###
    ####################################################################################################################

    ####################################################################################################################
    # 数据清洗
    number = re.search(pats["number"], number_details.get_text()).group()
    package = package_details.get_text()
    title = (title_details.get_text().strip('\n')).strip()
    reporter = reporter_details.get_text()
    heat = heat_details.get_text()
    temp = []
    ##########################################################
    for task_id in tasksummary_list:
        task = soup.find('tr', id=task_id)
        affects_match = re.search(pats["affect1"], str(task)).group()
        importance_match = (task.find('div', class_='importance-content')).find('span')
        status_match = task.find('div', class_="status-content")
        affects = (re.search(pats["affect2"], affects_match).group())[1:-1]
        importance = importance_match.get_text()
        status = (status_match.get_text()).strip('\n')
        description = description_details.get_text()
        temp_data = pd.DataFrame({'Number': number, 'Package': package, 'Title': title, 'Reporter': reporter,
                                  'Affects': str(affects), 'Importance': str(importance), 'Status': str(status),
                                  'Heat': heat, 'Description': description}, index=list('0'))
        temp.append(temp_data)
        # affectss.append(str(affects))
        # importances.append(str(importance))
        # statuses.append(str(status))
        # print(affects, importance, status)
    ###########################################################
    # description = description_details.get_text()
    ############################################################################################################
    # 创建临时Dataframe记录数据
    # temp_data = pd.DataFrame({'Number': number, 'Package': package, 'Title': title, 'Reporter': reporter,
    #                           'Affects': affectss, 'Importance': importances, 'Status': statuses,
    #                           'Heat': heat, 'Description': description}, index=list('0'))
    ############################################################################################################

    time.sleep(sleep_time)
    return temp


def get_all_details(threadname):
    print(threadname)
    while not urls.empty():
        url_task = urls.get()
        soup_task = get_html(url_task)
        urls.task_done()
        temp_data = get_details(soup_task)
        global bug_data
        for i in range(len(temp_data)):
            bug_data = bug_data.append(temp_data[i], ignore_index=True)
        put_in()
        # print(bug_data)
        process = 100*(0.999998 - (urls.qsize() / tasks))
        print('%.2f%%' % process)


def put_in():
    global count
    if not count % 100:
        bug_data.to_csv('E:/Workplace/Python_file/data' + str(count // 100) + '.csv', encoding='utf-8')
        bug_data.drop(bug_data.index, inplace=True)

# soup = get_html('https://bugs.launchpad.net/ubuntu/+bug/1')
# print(get_details(soup))

# # get_pages(1)
# # get_all_details('thread1')


if __name__ == "__main__":
    get_pages(10)
    tasks = urls.qsize()
    # while not urls.empty():
    #     print(urls.get())
    # lock = threading.Lock()
    threads = []
    thread_num = 30
    for i in range(1, thread_num+1):
        thread = Mythread(str(i), ('Thread' + str(i)))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    count = count * 100
    put_in()
