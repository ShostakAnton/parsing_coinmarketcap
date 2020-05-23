import requests
from bs4 import BeautifulSoup
import csv
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError

from multiprocessing import Pool

headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}  # имуляция действия поведения браузера


def get_html(url):
    # r = requests.get(url)  # Response
    # return r.text  # возвращает html код

    session = requests.Session()  # непрерывность действия во времени (имитация человека)
    request = session.get(url, headers=headers)  # имуляция открытия странички в браузере
    return request.content


def get_html_for_page(url):
    adapter = HTTPAdapter(max_retries=3)
    session = requests.Session()
    # использование `adapter` для всех запросов, которые начинаются с указанным URL
    session.mount(url, adapter)

    try:
        r = session.get(url)
        return r.content
    except ConnectionError as ce:
        print(ce)


def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')

    tds = soup.find('div', class_="cmc-table").find_all('td',
                                                        class_="cmc-table__cell cmc-table__cell--sticky cmc-table__cell--sortable cmc-table__cell--left cmc-table__cell--sort-by__name")
    links = []
    for td in tds:
        a = td.find('a').get('href')
        link = 'https://coinmarketcap.com' + a
        links.append(link)

    return links


def get_page_data(html):
    soup = BeautifulSoup(html, 'lxml')
    try:
        name = soup.find('div', class_="cmc-details-panel-header").find('h1').text
    except:
        name = ''
    try:
        price = soup.find('div', class_="cmc-details-panel-price jta9t4-0 fcilTk").find('span',
                                                                                        class_="cmc-details-panel-price__price").text.strip()
    except:
        price = ''

    data = {'name': name, 'price': price}

    return data


def write_csv(data):
    with open('coinmarketcap.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow((data['name'],
                         data['price']))

        print(data['name'], 'parsed')


def make_all(url):
    html = get_html_for_page(url)
    data = get_page_data(html)
    write_csv(data)


def main():
    url = "https://coinmarketcap.com/all/views/all/"
    all_links = get_all_links(get_html(url))

    # for url in all_links:
    #     html = get_html_for_page(url)
    #     data = get_page_data(html)
    #     write_csv(data)

    with Pool(40) as p:
        p.map(make_all, all_links)


if __name__ == '__main__':
    main()
