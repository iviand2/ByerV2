import json
import threading
import requests
import os
import time
import pickle
import datetime as dt
import numpy as np
import pandas as pd
import PySimpleGUI as Sg
import xlsxwriter
import inspect
import copy
from data.loggy import loggy
import data.db
import sys


lock = threading.RLock()
print_lock = threading.RLock()
ozon_lock = threading.RLock()
ts = pd.Timestamp
update_started = False


def spent_time():
    caller_variables = sys._getframe(1).f_locals
    try:
        spended_time = dt.datetime.now() - caller_variables['previous_time']
    except KeyError:
        spended_time = 'Начали отсчет'
    caller_variables['previous_time'] = dt.datetime.now()
    return spended_time


def lock_print(text):
    with print_lock:
        if type(text) != str:
            loggy.exception(text, stack_info=True)
        print(threading.current_thread().name, ": ", text)


class DataStore:
    def __init__(self, dte='sales'):
        self.dte = dte
        self.db = data.db.Database()

    # def __getitem__(self, item):
    #     if item in self.data.index.get_level_values(0):
    #         lev_name = 'partner'
    #     elif item in self.data.index.get_level_values(1):
    #         lev_name = 'sale_id'
    #     elif item in self.data.index.get_level_values(2):
    #         lev_name = 'article'
    #     else:
    #         return pd.DataFrame()
    #     return self.data.query(f'{lev_name} == "{item}"').drop(columns='other')

    def __str__(self):
        return f'База данных {self.dte}'

    def set(self, index: tuple, data: list):
        assert len(data) == len(self.data.columns), \
            f'В списке входящих значений должно быть {len(self.data.columns)} значений'
        self.data.loc[index, :] = data
        self._save()

    def inbox(self, update: list):
        if update:
            c = Catalogue().dictionary(update[0][0][0])
            cat = Catalogue()
            time = dt.datetime.now().time()
            lock_print(f'Начали обновление фрейма.')
            lock_print(f'Начали преобразование из списка. Выполнение прошлой операции: {spent_time()}')
            lock_print(f'Длина массива к обновлению: {len(update)}')
            update_dict = {}
            lock_print(f'Формируем словарь обновления. Выполнение прошлой операции: {spent_time()}')
            for item in update:
                partner = item[0][0]
                try:
                    code = c[item[0][2]]
                except KeyError:
                    lock_print(f'Товар {item[0][2]} не найден, присваиваем неизвестный')
                    code = cat[partner, item[1][1], item[0][2]]
                update_dict[(item[0][0], item[0][1], code)] = {}
                for num, col in enumerate(self.data.columns):
                    update_dict[(item[0][0], item[0][1], code)][col] = item[1][num]
            upd_frame = pd.DataFrame(update_dict).T
            upd_frame.index.names = self.data.index.names
            # # self.data = upd_frame.stack().combine_first(self.data.stack()).unstack()
            # self.data = self.data.append(upd_frame)
            # self.data = self.data.loc[~self.data.index.duplicated(keep='first')].sort_index()
            lock_print(f'')
            self.data = upd_frame.combine_first(self.data)
            lock_print(f'Фрейм обновлен, сохраняем. Выполнение прошлой операции: {spent_time()}')
            self._save()
            Params()[f'last_{self.dte}_load_date_{partner}'] = dt.datetime.now()
            lock_print(f'Готово. Выполнение прошлой операции: {spent_time()}')
        else:
            lock_print(f'{threading.current_thread().name}: Список обновления продаж пуст')

    def get(self, partner):
        return self.data.loc[partner]

    def upd_code(self, upd: dict):
        self.data = self.data.rename(index=upd)
        self._save()


class Update:

    def __init__(self):
        self.c = Catalogue()
        self.threads = []

    def __call__(self, forcibly=None, target=None):
        now = dt.datetime.now()
        global update_started
        if update_started is True:
            print('Процесс обновления уже запущен, дождитесь его окончания, пожалуйста')
        else:
            if target is not None:
                method = Params().data.loc[f'{target}_auth']['method']
                auth = Params().data.loc[f'{target}_auth']['value']
                name = target
                starter = [name, auth, 1, 1]
                exec(f'self.{method}({starter})')
                self.starter('')
            elif forcibly is None:
                stack = Params().data.copy()
                stack = stack[stack.index.str.contains("auth")].copy()
                for name in stack.index:
                    method = stack.loc[name]['method']
                    auth = stack.loc[name]['value']
                    name = name.split("_")[0]
                    starter = [name, auth, 0, 0]
                    if (now - Params()[f'last_sales_load_date_{name}']).days >= 1:
                        starter[2] = 1
                    if (now - Params()[f'last_stocks_load_date_{name}']).days >= 1:
                        starter[3] = 1
                    exec(f'self.{method}({starter})')
                self.starter('')
            elif forcibly is not None:
                stack = Params().data.copy()
                stack = stack[stack.index.str.contains("auth")]
                for name in stack.index:
                    method = stack.loc[name]['method']
                    auth = stack.loc[name]['value']
                    name = name.split("_")[0]
                    starter = [name, auth, 1, 1]
                    exec(f'self.{method}({starter})')
                self.starter('')

    def starter(self, master_thread):
        if master_thread:
            global update_started
            update_started = True
            self.threads.append(master_thread)
        else:
            for thread in self.threads:
                thread.start()
            for thread in self.threads:
                thread.join()
            lock_print('Обновление завершено.')
            update_started = False

    def beru(self, starter):
        name = starter[0]
        auth = starter[1]
        threads = []
        if starter[2] == 1:
            date = min([Params()['last_sales_load_date_beru'], dt.datetime.now()]) - dt.timedelta(days=30)
            while date <= dt.datetime.now():
                threads.append(
                    threading.Thread(
                        target=self.__beru_sales, name=f'{name.capitalize()} {date.date()}:', args=(name, auth, date)))
                date = date + dt.timedelta(days=1)
                time.sleep(2)
        if starter[3] == 1:
            threads.append(threading.Thread(
                target=self.__beru_stocks, name=f'{name.capitalize()} остатки:', args=(name, auth)))
        self.starter(threading.Thread(target=self.__threads_start, args=(threads, 'beru'), name=name.capitalize()))

    def ozon(self, starter):
        name = starter[0]
        auth = starter[1]
        threads = []
        if starter[2] == 1:
            threads.append(threading.Thread(
                target=self.__ozon_sales, name=f'{name.capitalize()} ФБО:', args=(name, auth)))
            threads.append(threading.Thread(
                target=self.__ozon_fbs_sales, name=f'{name.capitalize()} ФБС:', args=(name, auth)))
        if starter[3] == 1:
            threads.append(threading.Thread(
                target=self.__ozon_stocks, name=f'{name.capitalize()} остатки:', args=(name, auth)))
        self.starter(threading.Thread(target=self.__threads_start, name=f'{name.capitalize()}', args=(threads, 'ozon')))

    def wb(self, starter):
        name = starter[0]
        auth = starter[1]
        threads = []
        if starter[2] == 1 or starter[3] == 1:
            threads.append(
                threading.Thread(target=self.__wb, name=f'{name.capitalize()}', args=(name, auth)))
        self.starter(threading.Thread(target=self.__threads_start, args=(threads, 'wb')))

    def __ozon_sales(self, name, auth):
        """Обновление списков продаж и остатков Озон

        Последовательно обновляет сначала продажи, затем остатки и отправляет на обновление фреймов"""
        # Обновление продаж


    def __ozon_fbs_sales(self, name, auth):
        """Запрос данных ФБС"""
        url = 'http://api-seller.ozon.ru/v2/posting/fbs/list'
        update = []
        now = dt.datetime.now()
        last_load = Params()[f'last_sales_load_date_{name}_fbs']
        delta = dt.timedelta(days=30) if not last_load.year < now.year - 1 else dt.timedelta(days=90)
        start = now - delta
        end = start + dt.timedelta(days=30)
        error = 0
        while start <= now and error != 1:
            body = {
                "dir": "asc",
                "filter": {"since": f"{start.strftime('%Y-%m-%d')}T00:00:00.197Z",
                           "to": f"{end.strftime('%Y-%m-%d')}T23:59:59.197Z"},
                'limit': 50,
                'offset': 0,
                'with': {
                    "analytics_data": True,
                    "financial_data": True
                }
            }
            while True:
                lock_print(f'Выполнено заказов: %s' % body['offset'])
                response = requests.post(url, headers=auth, data=json.dumps(body), timeout=30)
                if response.status_code != 200:
                    Sg.PopupError(
                        f'{threading.current_thread().name}: Ошибка загрузки заказов ОЗОН ФБС: %s' % response.content)
                    lock_print(f'Некорректный ответ сервера OZON: {response.content}, пробуем повторить...')
                    if response.status_code == 408:
                        time.sleep(10)
                        response = requests.post(url, headers=auth, data=json.dumps(body), timeout=60)
                        if response.status_code != 200:
                            Sg.PopupError('%s' % response.content)
                            lock_print(f'Некорректный ответ сервера OZON: {response.content}')
                            error = 1
                            break
                    break
                else:
                    if not json.loads(response.text)['result']:
                        break
                    else:
                        for sale in response.json()['result']:
                            for item in sale['products']:
                                with ozon_lock:
                                    update.append([
                                        (f'{name}_fbs', sale['posting_number'], item['offer_id']),
                                        [
                                            pd.Timestamp(sale['in_process_at']).tz_localize(None),
                                            item['name'],
                                            int(item['quantity']),
                                            float(item['price']),
                                            (int(item['quantity']) * float(item['price'])),
                                            'Наш склад',
                                            sale['status'],
                                            sale
                                        ]
                                    ])
                body['offset'] += 50
            lock_print(f'Продажи за {start.date()} - {end.date()} получены. Время {dt.datetime.now().time()}')
            start += dt.timedelta(days=30)
            end += dt.timedelta(days=30)
        lock_print(f'Первичные данные API успешно получены. Время {dt.datetime.now().time()}')
        with lock:
            DataStore().inbox(update)

    def __ozon_stocks(self, name, auth):
        # Запрос остатков
        update = []
        lock_print(f'Запрашиваем актуальные остатки')
        url = 'http://api-seller.ozon.ru/v1/report/stock/create'
        body = {
            "language": "DEFAULT"
        }
        code = requests.post(url, headers=auth, data=json.dumps(body)).json()['result']
        url = 'http://api-seller.ozon.ru/v1/report/info'
        i = 0
        file = ''
        while True and i < 40:
            lock_print(f'Попытка {i}')
            resp = requests.post(url, headers=auth, data=json.dumps(code))
            if resp.json()['result']['file']:
                file = resp.json()['result']['file']
                lock_print(f'Файл успешно получен')
                break
            time.sleep(10)
            i += 1
        if not file:
            lock_print(f'Ошибка получения данных')
        else:
            try:
                frame = pd.read_csv(file, sep=';')
                stock = frame.drop(columns=[
                    c for c in frame.columns if (('возврат' in c.lower()) or ('брак' in c.lower()))
                ])
                stock = stock.replace(0, np.nan) \
                    .dropna(subset=stock.columns[7:], how='all') \
                    .T \
                    .dropna(how='all') \
                    .T \
                    .set_index('Артикул') \
                    .replace(np.nan, 0)
                stock = stock.drop(columns=[c for c in stock.columns[:8]])
                rename_dict = {}
                for wn in stock.columns:
                    rename_dict[wn] = wn.split(' ')[0] + ' ' + wn.split(' ')[-2].split('_')[0].capitalize().replace(',', '')
                stock = stock.rename(columns=rename_dict).T.groupby(level=0).sum().T.to_dict()
                stock_dict = {}
                for warehouse in stock:
                    quantity_type = warehouse.split(' ')[0]
                    city = warehouse.split(' ')[1]
                    for item in stock[warehouse]:
                        stock_dict.setdefault(city, {}).setdefault(item, {})[quantity_type] = stock[warehouse][item]
                for city in stock_dict:
                    for item in stock_dict[city]:
                        update.append([
                            (name, city, item),
                            [
                                stock_dict[city][item]['Доступно'],
                                stock_dict[city][item]['Зарезервировано'] if 'Зарезервировано' in stock_dict[city][item]. \
                                    keys() else 0,
                                None
                            ]
                        ])
                with lock:
                    Stocks().inbox(update)
            except Exception as E:
                log.save(E)
                lock_print('Произошла ошибка загрузки данных по основному протоколу, запускаем резерв')
                self.__ozon_stocks_reserve(name, auth)

    def __ozon_stocks_reserve(self, name, auth):
        url_info = 'https://api-seller.ozon.ru/v1/products/info/'
        url_list = 'https://api-seller.ozon.ru/v1/product/list'
        url_stock = 'https://api-seller.ozon.ru/v2/product/info/stocks'
        body = {
                "page": 1,
                "page_size": 100
                }
        update = []
        lock_print('Начали запрос по резервному протоколу')
        while True:
            response = requests.post(url_stock, json.dumps(body), headers=auth)
            if response.status_code != 200:
                lock_print(response.text)
                break
            elif not response.json().get('result')['items']:
                break
            else:
                for item in response.json().get('result')['items']:
                    try:
                        update.append(
                            [
                                (name, 'Хоругвино', item['offer_id']),
                                [
                                    item['stocks'][0]['present'],
                                    item['stocks'][0]['reserved'],
                                    None
                                ]
                            ]
                        )
                    except:
                        continue
                lock_print(f'Резервный протокол: страница {body["page"]} - готово')
            body['page'] += 1

        with lock:
            Stocks().inbox(update)

    def __threads_start(self, threads, method):
        if len(threads) != 0:
            for thread in threads:
                thread.start()
                time.sleep(5)
            for thread in threads:
                thread.join()
            lock_print('Обновление потока завершено')

    def __wb(self, name, auth):
        update = []
        url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders'
        body = {
            'dateFrom': f'{dt.datetime.now().date() - dt.timedelta(days=90)}',
            'key': auth
        }
        resp = requests.get(url, params=body)
        if resp.status_code != 200:
            lock_print(f'{resp.text}, {resp.status_code}')
        else:
            sales = resp.json()
            if not sales:
                Sg.popup_error('Не смогли получить данные с сервера - ответ пуст')
                return 'Ошибка получения данных'
            else:
                for sale in sales:
                    if type(sale['date']) is str:
                        sale['date'] = dt.datetime.strptime(sale['date'], '%Y-%m-%dT%H:%M:%S')
                    update.append([
                        (name, sale['odid'], sale['supplierArticle']),
                        [
                            pd.Timestamp(sale['date']),
                            sale['barcode'],
                            int(sale['quantity']),
                            float(sale['totalPrice']),
                            int(sale['quantity']) * float(sale['totalPrice']),
                            sale['warehouseName'].capitalize(),
                            'cancelled' if sale['isCancel'] is True else 'actual',
                            sale
                        ]
                    ])
                lock_print(f'Массив продаж обработан, начинаем загрузку в базу. Время {dt.datetime.now().time()}')
                with lock:
                    DataStore().inbox(update)
                update = []
        url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks'
        body = {
            'dateFrom': f'{dt.datetime.now().date()}',
            'key': f'{Params()["wb_auth"]}'
        }
        lock_print('Спим')
        time.sleep(5)
        response = requests.get(url, params=body)
        if response.status_code != 200:
            lock_print(f'Ошибка соединения с Вайлдберрис')
            lock_print(f'Ответ: {response.reason}, {response.text}, {response.status_code}')
            lock_print(f'Дата последней успешной загрузки: {Params()["last_stocks_load_date_wb"]}')
        else:
            stocks = response.json()
            if not stocks:
                Sg.popup_error(f"""Ответ по остаткам вб получен, но внутри пусто, напишите в техподдержку комиссионера! 
Дата последней успешной загрузки: {Params()["last_stocks_load_date_wb"]}""")
                return
            else:
                for item in stocks:
                    update.append([
                        (name, item['warehouseName'], item['supplierArticle']),
                        [
                            item['quantity'],
                            0,
                            item
                        ]
                    ])
                lock_print(f'Остатки Вайлдберрис получены, начинаем загрузку в базу')
                with lock:
                    Stocks().inbox(update)

    def __beru_sales(self, name, auth, one_day=None):
        update = []
        url = 'https://api.partner.market.yandex.ru/v2/campaigns/21552289/stats/orders.json'
        date = Params()['last_sales_load_date_beru']
        if one_day is None:
            date_from = date - dt.timedelta(
                days=30) if date.year == dt.datetime.now().year else dt.datetime.now().replace(
                month=1, day=1)
            date_to = date + dt.timedelta(days=30)
        elif type(one_day) == dt.datetime:
            date_from = one_day
            date_to = one_day + dt.timedelta(days=1)
        else:
            raise ValueError(
                f'{threading.current_thread().name}: Дата загрузки должна быть указана в формате datetime.datetime')
        body = {
            "dateFrom": f"{date_from.date()}",
            "dateTo": f"{date_to.date()}"
        }
        params = {
            'campaignId': 21552289,
            'limit': 20
        }
        i = 1
        e = 0
        while True and e < 40:
            try:
                response = requests.post(url, headers=auth, data=json.dumps(body), params=params)
                if response.status_code != 200:
                    lock_print(response.reason)
                    e += 1
                    continue
                elif response.status_code == 420:
                    lock_print('Ошибка 420, попробуем подождать')
                    time.sleep(10)
                    e += 1
                    continue
                response = response.json()
                for sale in response['result']['orders']:
                    for item in sale['items']:
                        update.append([
                            (name, sale['id'], item['marketSku']),
                            [
                                pd.Timestamp(sale['creationDate']),
                                item['offerName'],
                                int(item['count']),
                                float(item['prices'][0]['costPerItem']),
                                float(item['prices'][0]['total']),
                                item['warehouse']['name'].split(', ')[-1].replace(')', '').split(' (')[-1],
                                sale['status'],
                                'Не загружаю'
                            ]
                            ])
            except requests.exceptions.ConnectionError:
                lock_print(f'Ошибка со стороны Беру - разрыв соединения сервером')
                continue
            try:
                params['page_token'] = response['result']['paging']['nextPageToken']
            except KeyError:
                break
            lock_print(f'Страница {i} - готово')
            i += 1
        if one_day is None:
            Params()[f'last_sales_load_date_{name}'] = min([date + dt.timedelta(days=30), dt.datetime.now()])
        with lock:
            DataStore().inbox(update)

    def __beru_stocks(self, name, auth):
        update = []
        params = {
            'campaignId': 21552289,
            'limit': 200
            # 'dbg': '3B000001777781B4'
        }
        url_cat = 'https://api.partner.market.yandex.ru/v2/campaigns/21552289/offer-mapping-entries.json'
        b_cat = []
        lock_print(f'Начали загрузку каталога наших товаров в Беру')
        e = 0
        count = 1
        while True and e < 10:
            try:
                response = requests.get(url_cat, headers=auth, params=params)
            except TimeoutError:
                e += 1
                continue
            if response.status_code != 200:
                lock_print(response.reason)
                e += 1
                continue
            else:
                response = response.json()
                b_cat.extend(response['result']['offerMappingEntries'])
                lock_print(f'Выполнено товаров: {count * 200}')
                count += 1
                try:
                    params['page_token'] = response['result']['paging']['nextPageToken']
                except KeyError:
                    break
        lock_print(f'Готово, всего товаров в каталоге: {len(b_cat)}')
        skus_list = []
        for offer in b_cat:
            skus_list.append(offer['offer']['shopSku'])
        del b_cat
        try:
            del params['page_token']
        except KeyError:
            pass
        params['limit'] = 50
        url_stocks = 'https://api.partner.market.yandex.ru/v2/campaigns/21552289/stats/skus.json'
        c = 0
        while True:
            if len(skus_list[c * 50:c * 50 + 50]) != 0:
                body = {"shopSkus": skus_list[c * 50:c * 50 + 50]}
            else:
                break
            try:
                response = requests.post(url_stocks, headers=auth, data=json.dumps(body), params=params)
                if response.status_code != 200:
                    lock_print(f'Ошибка {response.status_code}: {response.reason}')
                    continue
                else:
                    for item in response.json()['result']['shopSkus']:
                        try:
                            for city in item['warehouses']:
                                available, reserved = np.nan, np.nan
                                for stock_type in city['stocks']:
                                    if stock_type['type'].lower() == 'available':
                                        available = stock_type['count']
                                    elif stock_type['type'].lower() == 'freeze':
                                        reserved = stock_type['count']
                                update.append([
                                    (
                                        name,
                                        city['name'].split(' (')[-1].split(', ')[-1].replace(')', ''),
                                        item['marketSku']
                                    ),
                                    [
                                        available,
                                        reserved,
                                        city
                                    ]
                                ])
                        except KeyError as e:
                            continue
                    c += 1
            except Exception as e:
                lock_print(e)
                break
        lock_print(
            f'Остатки получены и обработаны, пакете {len(update)} товаров'
        )
        with lock:
            Stocks().inbox(update)

    def ozon_prices(self, size):
        auth = Params()['ozon_auth']
        url = 'http://api-seller.ozon.ru/v1/product/info/prices'
        # size = 1000
        body = {
            "page": 1,
            "page_size": size
        }
        price = {}
        body_list = []
        requests_list = []
        while True:
            body_list.append(body.copy())
            response = requests.post(url=url, headers=auth, data=json.dumps(body))
            if response.status_code != 200:
                print(response.status_code, response.text, response.reason)
                print('Запрос списка товаров завершился с ошибкой.')
                # requests_list.append(dump.dump_all(response))
                break
            else:
                requests_list.append(response.reason)
                for item in response.json()['result']['items']:
                    price[item['offer_id']] = item['price']
                print(f'Отработано {len(price)} товаров')
                if len(response.json()['result']['items']) != size:
                    break
                else:
                    body["page"] = body["page"] + 1
        return price, body_list, requests_list


class Params(DataStore):
    def __init__(self):
        super().__init__('params')

    def __getitem__(self, item: str):
        if '-method' in item:
            return self.data.loc[item.replace('-method', '')].method
        else:
            try:
                return self.data.loc[item].value
            except KeyError:
                if 'date' in item:
                    return dt.datetime(year=1990, month=4, day=21)
                else:
                    return f'<Данные настройки {item} в базе отсутствуют>'

    def __setitem__(self, key, value):
        try:
            self.data.loc[key]['value'] = value
            self._save()
        except KeyError:
            self.data.loc[key] = np.nan
            self.data.loc[key]['value'] = value
            self._save()

    def partners(self):
        return [c for c in self.data.index if '_auth' in c]


def return_unknown_file():
    file = pd.read_excel('Неопознанные_артикула.xlsx', index_col=0)
    file = file.drop(index=file[~file['Код'].notna()].index.to_list(), axis=0)
    result, message = Catalogue().check_upd(file)
    lock_print(message)
    if result is True:
        if 'Неопознанные_артикула_отработано.xlsx' in os.listdir():
            os.remove('Неопознанные_артикула_отработано.xlsx')
            os.rename('Неопознанные_артикула.xlsx', 'Неопознанные_артикула_отработано.xlsx')
        else:
            os.rename('Неопознанные_артикула.xlsx', 'Неопознанные_артикула_отработано.xlsx')


class Catalogue(DataStore):
    def __init__(self):
        super().__init__('catalogue')

    def __get_last_unknown(self):
        a = [int(c.split('_')[-1]) for c in self.data.index if '<Не указан>' in c]
        new_num = max(a) + 1 if a else 1
        return f'<Не указан>_{new_num}'

    def get_code(self, values: list):
        pass

    def brand(self):
        return self.data.brand

    def export(self):
        self.data.to_excel('Catalogue.xlsx')

    def _import(self):
        self.data.to_excel(f'archive/Catalogue_{dt.datetime.now().date()}.xlsx')
        self.data = pd.read_excel('Catalogue.xlsx', index_col=0)
        with DataStore().data.reset_index(level=[0, 1]) as sales:
            sales_check = sales[~sales.index.isin([c for c in self.data.index])]
        with Stocks().data.reset_index(level=[0, 1]) as stocks:
            stocks_check = stocks[stocks.index.isin([c for c in self.data.index])]
        if len(sales_check) != 0:
            lock_print('Ошибка, нарушение целостности массива продаж.')
            lock_print(sales_check)
        elif len(stocks_check) != 0:
            lock_print('Ошибка, нарушение целостности массива продаж.')
            lock_print(sales_check)
        else:
            self._save()
            os.remove('Catalogue.xlsx')

    def __getitem__(self, item):
        if type(item) is not set:
            assert len(item) == 3, '''Запрос должен содержать 3 элемента'''
            if item[0] not in self.data.columns:
                self.data[item[0]] = np.nan
                self.data[f'commit_{item[0]}'] = np.nan
                self.data.loc[self.__get_last_unknown(), ['name', item[0]]] = [item[1], item[2]]
                self._save()
            if item[2] not in self.data[item[0]].values:
                self.data.loc[self.__get_last_unknown(), ['name', item[0]]] = [item[1], item[2]]
                self._save()
            return self.data[self.data[item[0]] == item[2]].index.item()
        else:
            try:
                call_partner = sys._getframe(1).f_locals['self'].name
            except:
                call_partner = 'other'
            if call_partner != 'wb':
                matrix = [c for c in ['name', 'brand'] if c in self.data.columns]
            else:
                matrix = [c for c in ['name', 'brand', 'wb'] if c in self.data.columns]
            return self.data.loc[[c for c in item]][matrix]

    def dictionary(self, partner):
        try:
            return self.data[self.data[partner].notna()].reset_index().set_index(partner)['1C'].to_dict()
        except KeyError:
            return {}

    def create_unknown_file(self):
        file = self.data[self.data.index.str.contains('<Не')].copy()
        file.drop(columns=[c for c in file.columns if 'commit' in c], inplace=True)
        file['Код'] = np.nan
        file.to_excel('Неопознанные_артикула.xlsx')

    @staticmethod
    def check_upd(upd: pd.DataFrame) -> (bool, str):
        """Возвращает проверку интеграции обновления и проводит само обновление"""
        cat = Catalogue()
        stocks = Stocks()
        sales = DataStore()
        cart = Supplies()
        for item in upd.index:
            info = upd.loc[item].dropna().drop('name')
            cat.data.loc[info['Код'], info.keys()[0]] = info.values[0]
            stocks.data.rename({info.name: info['Код']}, inplace=True)
            stocks.data = stocks.data[~stocks.data.index.duplicated()].copy()
            sales.data.rename({info.name: info['Код']}, inplace=True)
            sales.data = sales.data[~sales.data.index.duplicated()].copy()
            cat.data.drop(index=info.name, inplace=True)
        sa = sales.data.copy().reset_index(level=[0, 1], drop=True)
        st = stocks.data.copy().reset_index(level=[0, 1], drop=True)
        if len(sa[~sa.index.isin([c for c in cat.data.index])]) != 0:
            return False, 'Ошибка проверки целостности массива продаж'
        elif len(st[~st.index.isin([c for c in cat.data.index])]) != 0:
            return False, 'Ошибка проверки целостности массива остатков'
        else:
            cat._save()
            stocks._save()
            sales._save()
            return True, 'Данные успешно обновлены'

    def upd(self, upd: pd.DataFrame):
        self.data.update(upd)
        self._save()

    def commits(self, partner, index):
        try:
            return self.data.loc[[c for c in index]][f'commit_{partner}', 'commit_all']
        except KeyError:
            return pd.DataFrame(index=[c for c in index], columns=[f'commit_{partner}', 'commit_all'])


class Result:
    def __init__(self):
        self.name = None
        self.data = []

    def __call__(self, partner):
        self.name = partner
        self.__table()
        self.__top_proceeds()
        return self.__file('')

    def __table(self):
        lock_print(f'Начали {dt.datetime.now().time()}')
        border = pd.Timestamp(dt.datetime.now() - dt.timedelta(days=365)).date()
        sales = DataStore().get(self.name)
        sales['date'] = sales['date'].apply(lambda x: x.to_pydatetime() if type(x) is pd.Timestamp else x)
        sales = sales.query('date > @border')
        sales = sales.astype({'date': 'datetime64'})
        week_now = dt.datetime.now().isocalendar()[1]
        stocks = Stocks()[self.name].replace(0, np.nan).dropna(how='all')
        cities = {c.split('_')[0].replace('-п', '-П') for c in sales['city'].array}
        c = Catalogue()
        for city in cities:
            index = {c for c in sales[sales['city'] == city.replace('-П', '-п')].reset_index(level=0, drop=True).index}
            try:
                index = index.union({c for c in stocks.loc[city].index})
            except KeyError:
                pass
            result = pd.DataFrame(index=index)
            result = result.join(c[index])
            if week_now < 9:
                year = dt.datetime.now().year
                last_week_last_year = dt.datetime(year - 1, 12, 31).isocalendar()[1]
                weeks_last_year = [c for c in range(1, last_week_last_year + 1)]
                target_weeks = []
                for i in range(week_now - 8, 0):
                    target_weeks.append(weeks_last_year[i])
                for i in range(1, week_now + 1):
                    target_weeks.append(i)
            else:
                target_weeks = [c for c in range(week_now - 8, week_now + 1)]
            for week in target_weeks:
                result = result.join(sales.query(
                    f'date.dt.week == {week} & status != "cancelled" & city == "{city.replace("-П", "-п")}"'
                ).reset_index().groupby('article')['quantity'].sum()).rename(columns={'quantity': f'Неделя {week}'})
            try:
                result = result.join(stocks.loc[city])
            except KeyError:
                result['quantity'] = np.nan
                result['reserved'] = np.nan
            price = sales.query('status != "cancelled"').reset_index(level=0, drop=True).copy()
            price = price.loc[~price.index.duplicated(keep='first')]['price']
            result = result.join(price)
            result['Заказ'] = np.nan
            result = result.join(c.commits(self.name, index))
            # result = result.join(Supplies()())
            self.__file({city: result})
        lock_print(f'Закончили {dt.datetime.now().time()}')

    def __file(self, frame):
        """Метод сбора и записи в файл. Аналитики тут же"""
        if frame != '':
            self.data.append(frame)
        else:
            writer = pd.ExcelWriter(f'{self.name}_{dt.datetime.now().date()}.xlsx')
            for table in self.data:
                for war in table:
                    table[war].style.apply(self.__stocks_color, axis=0).to_excel(writer, sheet_name=war)
                    sheet = writer.sheets[f'{war}']
                    sheet.set_column(0, 0, 20)
                    sheet.set_column(1, 1, 80)
            try:
                self.data = []
                # file = copy.deepcopy(writer.handles.handle.name)
                writer.save()
            except xlsxwriter.exceptions.FileCreateError:
                i = 1
                while True and i < 10:
                    try:
                        path = writer.book.filename
                        writer.book.filename = path.replace('.', '_c.')
                        # file = copy.deepcopy(writer.handles.handle.name)
                        writer.save()
                        break
                    except xlsxwriter.exceptions.FileCreateError:
                        i += 1
                        continue
            file = f'{os.getcwd()}\\{writer.book.filename.name}'
            writer.close()
            lock_print(f'Файл {file} сохранен')
            return file

    @staticmethod
    def __stocks_color(series):
        if series.name == 'quantity':
            return ['color: red' for c in series]
        else:
            return ['color: grey' for c in series]

    @staticmethod
    def proceeds():
        sales = DataStore().data.copy()
        partners = {c for c in sales.index.get_level_values(level=0)}
        start = pd.Timestamp((dt.datetime.now() - dt.timedelta(days=182)).replace(day=1))
        sales = sales.join(Catalogue().data['brand'], how='left', on='article')
        sales.date = sales.date.apply(lambda x: x.to_pydatetime() if type(x) is pd.Timestamp else x)
        sales.brand = sales.brand.fillna('Не указан')
        sales = sales.drop(index=sales.query('date < @start').index)
        months = []
        months_dict = {
            'Месяц 1': 'Январь',
            'Месяц 2': 'Февраль',
            'Месяц 3': 'Март',
            'Месяц 4': 'Апрель',
            'Месяц 5': 'Май',
            'Месяц 6': 'Июнь',
            'Месяц 7': 'Июль',
            'Месяц 8': 'Август',
            'Месяц 9': 'Сентябрь',
            'Месяц 10': 'Октябрь',
            'Месяц 11': 'Ноябрь',
            'Месяц 12': 'Декабрь'
        }
        writer = pd.ExcelWriter('Выручка по брендам.xlsx')
        for m in range(0, 6):
            months.append(start.month + m if start.month + m <= 12 else start.month + m - 12)
        for partner in partners:
            table = pd.DataFrame(index={c for c in sales.brand})
            for month in months:
                table = table.join(
                    sales.query(f'date.dt.month == {month} & partner == "{partner}" & status != "cancelled"'). \
                        groupby('brand')['summ'].sum()
                ).rename(columns={'summ': f'Месяц {month}'})
            table. \
                dropna(how='all', subset=[c for c in table.columns]). \
                rename(columns=months_dict). \
                to_excel(writer, sheet_name=partner)
        writer.save()

    def __top_proceeds(self):
        months_dict = {
            1: 'Январь',
            2: 'Февраль',
            3: 'Март',
            4: 'Апрель',
            5: 'Май',
            6: 'Июнь',
            7: 'Июль',
            8: 'Август',
            9: 'Сентябрь',
            10: 'Октябрь',
            11: 'Ноябрь',
            12: 'Декабрь'
        }
        cut = pd.Timestamp.now().replace(day=1)
        cut = cut.replace(year=cut.year - 1)
        sales = DataStore().get(self.name).query('date >= @cut & status != "cancelled"').reset_index(level=1)
        index = {c for c in sales.article}
        result = pd.DataFrame(index=index)
        result = result.join(Catalogue()[index])
        for date in pd.date_range(cut, pd.Timestamp.now(), freq=pd.offsets.MonthBegin(), closed=None):
            result = result.join(
                sales.query('date.dt.year == @date.year & date.dt.month == @date.month').\
                groupby('article')['summ'].sum()
            ).rename(columns={'summ': f'{months_dict[date.month]} {date.year}'})
        self.__file({'Выручка': result})


class Stocks(DataStore):
    def __init__(self):
        super().__init__('stocks')

    def __getitem__(self, item):
        idx = pd.IndexSlice
        if item in self.data.index.levels[0]:
            return self.data.loc[item, :].drop(columns='other')
        elif item in self.data.index.levels[1]:
            return self.data.loc[idx[:, item], :].drop(columns='other')
        elif item in self.data.index.levels[2]:
            return self.data.loc[idx[:, :, item], :].drop(columns='other')
        else:
            return pd.DataFrame(index=['partner', 'city', 'article'], columns=['quantity', 'reserved'])

    def inbox(self, update: list):
        if len(update) != 0:
            partner = update[0][0][0]
            try:
                self.data.drop(index=partner, inplace=True, level=0)
            except KeyError:
                pass
            super().inbox(update)
            self._save()
        else:
            lock_print(f'Пустой список обновления остатков')


def return_file(filepath: str):
    if '/' in filepath:
        name = filepath.split('/')[-1].split('_')[0]
    elif '\\' in filepath:
        name = filepath.split('\\')[-1].split('_')[0]
    else:
        raise ValueError
    c = Catalogue()
    file_matrix = {'commit_all': 'str', f'commit_{name}': 'str'}
    if 'xls' in filepath:
        file = pd.ExcelFile(filepath)
        for sheet in [c for c in file.sheet_names if c != 'Выручка']:
            try:
                frame = frame.append(pd.read_excel(file, sheet, index_col=0).astype(file_matrix))
            except NameError:
                frame = pd.read_excel(file, sheet, index_col=0).astype(file_matrix)
        sup = frame.fillna(0).query('Заказ != 0').groupby(level=0)['Заказ'].sum().to_frame()
        Supplies().inbox(sup.rename(columns={'Заказ': name}))
        upd_brand = frame.fillna(0).query('brand != 0')
        upd_brand = upd_brand.loc[~upd_brand.index.duplicated(keep='last')]['brand'].to_frame()
        c.upd(upd_brand)
        commits = [c for c in frame.columns if 'commit' in c]
        upd_commits = frame.fillna(0).query(f'{commits[0]} != 0 | {commits[1]} != 0')[commits]
        upd_commits = upd_commits[~upd_commits.index.duplicated()]
        c.upd(upd_commits)
        lock_print('Файл успешно загружен')
    elif 'fox' in filepath:
        pass
    else:
        raise ValueError('Неизвестный формат файла')


class Supplies(DataStore):
    def __init__(self):
        super().__init__('supplies')
        self.data = self.__read('supplies')

    def __read(self, dte):
        try:
            with open(f'storage/{dte}.fox', 'rb') as file:
                return pickle.load(file)
        except FileNotFoundError:
            return pd.DataFrame(columns=['1C']).set_index('1C')

    def clear_cart(self):
        self.data = pd.DataFrame(columns=['1C']).set_index('1C')
        self._save()

    def __call__(self):
        return self.data

    def upd_code(self, upd: dict):
        self.data = self.data.rename(index=upd)
        self.data = self.data.groupby(level=0).sum()
        self._save()

    def inbox(self, update: pd.DataFrame):
        self.data = update.combine_first(self.data)
        self._save()

    def to_file(self):
        result = self.data.join(Catalogue()[{c for c in self.data.index}])
        result = result[['name', 'brand'] + [c for c in self.data.columns]]
        result.to_excel(f'Заказ {pd.Timestamp.now().date()}.xlsx')

