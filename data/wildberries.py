import requests
import json
import time
import datetime as dt
import data.db as db
import pandas as pd
import data.loggy as logger


def sales_update(account):
    database = db.Database()
    try:
        account_data = database.get_and(table='accounts',
                                        conditions={'seller_id': str(account)}).loc[0]
    except:
        raise NameError(f'Данные по кабинету {account} в базе не найдены')
    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders'
    auth = account_data['api_key']
    body = {
        'dateFrom': f'{dt.datetime.now().date() - dt.timedelta(days=90)}',
        'key': auth
    }
    resp = requests.get(url, params=body)
    if resp.status_code != 200:
        print(f'Ошибка запроса данных продаж :: {resp.text}, {resp.status_code}')
    else:
        parther_translator, ids, names = database.catalogue_dict(account_data['name'])
        sales = resp.json()
        if not sales:
            print('Не смогли получить данные с сервера - ответ пуст')
            # Sg.popup_error('Не смогли получить данные с сервера - ответ пуст')
            return 'Ошибка получения данных'
        else:
            update = []
            for sale in sales:
                try:
                    article = parther_translator[sale['supplierArticle']]
                    name = names[sale['supplierArticle']]
                except KeyError:
                    if sale['supplierArticle'] in ids:
                        article = sale['supplierArticle']
                        name = database.get_and('catalogue', {'id': sale['supplierArticle']}).loc[0]['name']
                        database.insert('catalogue', [{'id': article,
                                                       account_data['name']: article,
                                                       'barcode': sale['barcode']}])
                        parther_translator[article] = article
                        names[article] = name
                    else:
                        name = f'{sale["subject"]} {str(sale["supplierArticle"])}'
                        article = database.get_sku({
                            account_data['name']: sale['supplierArticle'],
                            'name': name
                        })
                        parther_translator[article] = article
                        names[article] = name
                update.append({
                    'partner': account_data['name'],
                    'sale_id': sale['odid'],
                    'seller_id': 'wb',
                    'date': sale['date'], #.tz_convert('Europe/Moscow'),
                    'type': 'FBO',
                    'article': article,
                    'name': name,
                    'quantity': int(sale['quantity']),
                    'price': float(sale['totalPrice']),
                    'warehouse': sale['warehouseName'],
                    'sale_region': sale['oblast'],
                    'sale_city': '',
                    'status': 'cancelled' if sale['isCancel'] else 'Active',
                    'full_data': json.dumps(sale)
                })
            print(f'Массив продаж обработан, начинаем загрузку в базу. Время {dt.datetime.now().time()}')
            database.insert('sales', update)
            del database


def stocks_update(account):
    database = db.Database()
    update = []
    try:
        account_data = database.get_and(table='accounts',
                                        conditions={'seller_id': str(account)}).loc[0]
    except:
        raise NameError(f'Данные по кабинету {account} в базе не найдены')
    account_data['date'] = pd.Timestamp(account_data['date'])
    auth = account_data['api_key']
    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks'
    body = {
        'dateFrom': f'{dt.datetime.now().date()}',
        'key': auth
    }
    print('Спим')
    time.sleep(5)
    response = requests.get(url, params=body)
    if response.status_code != 200:
        print(f'Ошибка соединения с Вайлдберрис')
        print(f'Ответ: {response.reason}, {response.text}, {response.status_code}')
        # print(f'Дата последней успешной загрузки: {Params()["last_stocks_load_date_wb"]}')
    else:
        stocks = response.json()
        if not stocks:
            print('Ошибка обновления остатков Вайлдберрис')
    #         Sg.popup_error(f"""Ответ по остаткам вб получен, но внутри пусто, напишите в техподдержку комиссионера!
    # Дата последней успешной загрузки: {Params()["last_stocks_load_date_wb"]}""")
            return
        else:
            parther_translator, ids, names = database.catalogue_dict(account_data['name'])
            for item in stocks:
                try:
                    article = parther_translator[item['supplierArticle']]
                except:
                    a = 1+1
                update.append({
                    'partner': 'wb',
                    'city': item['warehouseName'],
                    'article': article,
                    'quantity': item['quantity'],
                    'reserved': item['inWayToClient'],
                    'full_data': json.dumps(item)
                })
            print(f'Остатки Вайлдберрис получены, начинаем загрузку в базу')
            database.commit_query(f'delete from marketplace.stocks '
                                  f'where partner = "wb"')
            database.insert('stocks', update)


def start_update(account):
    stocks_update(account)
    sales_update(account)

