import requests
import json
import time
import datetime as dt
import data.db as db
import pandas as pd
import numpy as np
import data.loggy as logger

database = db.Database()
lock_print = print


def sales_update(account):
    now = pd.Timestamp.now()
    account_data = database.get_and(table='accounts',
                                    conditions={'seller_id': str(account)})
    if account_data.empty:
        raise ValueError(f'Значение кабинета {account} в базе отсутствует')
    for t in ['fbo', 'fbs']:
        last_load = database.get_and(table='sales',
                                     conditions={'seller_id': str(account),
                                                 'type': t.upper()},
                                     order_by='change_date',
                                     limit=1)
        if last_load.empty:
            last_load = pd.Timestamp('2020-04-21')  # ДР разраба
        else:
            last_load = last_load['change_date'][0]
        if type(last_load) == pd.Timestamp:
            pass
        else:
            last_load = last_load.loc[0]
        delta = pd.Timedelta(days=7) if (now - last_load).days <= 365 else pd.Timedelta(days=365)
        start = last_load - delta
        end = start + pd.Timedelta(days=30)
        error = 0
        auth = {
            'Client-Id': str(account),
            'Api-Key': account_data.T.loc['api_key'][0],
            'Content-Type': 'application/json'
        }
        while start <= now and error != 1:
            body = {
                "dir": "asc",
                "filter": {"since": f"{str(start.date())}T00:00:00.197Z",
                           "to": f"{str(end.date())}T23:59:59.197Z"},
                'limit': 50,
                'offset': 0,
                'with': {
                    "analytics_data": True,
                    "financial_data": True
                }
            }
            parther_translator, ids, names = database.catalogue_dict(account_data.name[0])
            while True:
                update = []
                lock_print(f'Выполнено заказов: {body["offset"]}')
                response = requests.post(f'http://api-seller.ozon.ru/v2/posting/{t}/list',
                                         headers=auth,
                                         data=json.dumps(body),
                                         timeout=60)
                if response.status_code != 200:
                    lock_print(f'Ошибка загрузки заказов ОЗОН: {response.content}')
                    lock_print(f'Некорректный ответ сервера OZON: {response.content}, пробуем повторить...')
                    time.sleep(20)
                    error += 1
                    break
                else:
                    if not json.loads(response.text)['result']:
                        break
                    else:
                        for sale in response.json()['result']:
                            for item in sale['products']:
                                if not item['offer_id']:
                                    continue
                                try:
                                    article = parther_translator[item['offer_id']]
                                    name = names[item['offer_id']]
                                except KeyError:
                                    if item['offer_id'] in ids:
                                        article = item['offer_id']
                                        name = database.get_and('catalogue', {'id': item['offer_id']}).loc[0]['name']
                                        database.insert('catalogue', [{'id': article, account_data.name[0]: article}])
                                    else:
                                        article = database.get_sku({
                                            account_data.name[0]: item['offer_id'],
                                            'name': item['name']
                                        })
                                        name = item['name']
                                        parther_translator[item['offer_id']] = article
                                        names[item['offer_id']] = name
                                try:
                                    if sale['analytics_data'] is None:
                                        sale['analytics_data'] = {}
                                    update.append({
                                        'partner': account_data.name[0],
                                        'sale_id': sale['posting_number'],
                                        'seller_id': account,
                                        'date': str(pd.Timestamp(sale['in_process_at']).tz_convert('Europe/Moscow'))[:19],
                                        'type': t.upper(),
                                        'article': article,
                                        'name': name,
                                        'quantity': int(item['quantity']),
                                        'price': float(item['price']),
                                        'warehouse': sale.setdefault('analytics_data', {}).setdefault('warehouse_name',
                                                                                                      'Не указан').
                                            replace('-', '_').
                                            split('_')[0].
                                            capitalize(),
                                        'sale_region': sale['analytics_data'].setdefault('region', 'Не указан'),
                                        'sale_city': sale['analytics_data'].setdefault('city', 'Не указан'),
                                        'status': sale['status'],
                                        'full_data': json.dumps(sale)
                                    })
                                except AttributeError:
                                    ccc = 1
                body['offset'] += 50
                database.insert('sales', update)
            lock_print(f'Продажи за {start.date()} - {end.date()} получены. Время {dt.datetime.now().time()}')
            start += dt.timedelta(days=30)
            end += dt.timedelta(days=30)


def stocks_update(account) -> None:
    update = []
    lock_print(f'Запрашиваем актуальные остатки')
    url = 'http://api-seller.ozon.ru/v1/report/stock/create'
    body = {
        "language": "DEFAULT"
    }
    account_data = database.get_and(table='accounts',
                                    conditions={'seller_id': str(account)})
    auth = {
        'Client-Id': str(account),
        'Api-Key': account_data.T.loc['api_key'][0],
        'Content-Type': 'application/json'
    }
    code = requests.post(url, headers=auth, data=json.dumps(body)).json()['result']
    url = 'http://api-seller.ozon.ru/v1/report/info'
    try_counter = 0
    file = ''
    while True and try_counter < 40:
        lock_print(f'Попытка {try_counter}')
        resp = requests.post(url, headers=auth, data=json.dumps(code))
        if resp.json()['result']['file']:
            file = resp.json()['result']['file']
            lock_print(f'Файл успешно получен')
            break
        time.sleep(10)
        try_counter += 1
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
                    update.append({
                        'partner': f'{account_data.name[0]}_{account}',
                        'city': city,
                        'article': item,
                        'quantity': stock_dict[city][item]['Доступно'],
                        'reserved': stock_dict[city][item].setdefault('Зарезервировано', 0),
                        'full_data': ''
                    })
            if update:
                database.commit_query(f'delete from marketplace.stocks '
                                      f'where partner = "{account_data.name[0]}_{account}"')
                database.insert('stocks', update)
            else:
                lock_print('Отчет остатков пуст, обновление не произведено')
        except Exception as Ex:
            logger.loggy.exception(Ex, stack_info=True)
            raise


def start_update(account):
    try:
        sales_update(account=account)
        stocks_update(account=account)
    except Exception as Ex:
        logger.loggy.exception(Ex, stack_info=True)
        raise Ex
