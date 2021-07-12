import requests
import json
import time
import data.db as db
import pandas as pd


def sales_update():
    database = db.Database()
    url = 'https://api.partner.market.yandex.ru/v2/campaigns/21552289/stats/orders.json'
    try:
        account_data = database.get_and(table='accounts',
                                        conditions={'seller_id': 'beru'}).loc[0]
    except:
        raise NameError(f'Данные по кабинету Беру в базе не найдены')
    last_sale = database.get_and(table='sales', conditions={'partner': account_data['seller_id']}, limit=1)
    if last_sale.empty:
        date_from = pd.Timestamp.now() - pd.Timedelta(days=365)
    else:
        date_from = last_sale['date'] - pd.Timedelta(days=7)
    body = {
        "dateFrom": f"{date_from.date()}",
        "dateTo": f"{pd.Timestamp.now().date()}"
    }
    params = {
        'campaignId': 21552289,
        'limit': 20
    }
    auth = {
        'Authorization': account_data['api_key'],
        'Content-Type': 'application/json'
    }
    page = 1
    errors_catched = 0
    parther_translator, ids, names = database.catalogue_dict(account_data['name'])
    while True and errors_catched < 40:
        try:
            response = requests.post(url, headers=auth, data=json.dumps(body), params=params)
            if response.status_code != 200:
                print(response.reason)
                errors_catched += 1
                continue
            elif response.status_code == 420:
                print('Ошибка 420, попробуем подождать')
                time.sleep(10)
                errors_catched += 1
                continue
            response = response.json()
            update = []
            for sale in response['result']['orders']:
                for item in sale['items']:
                    try:
                        article = parther_translator[item['shopSku']]
                        name = names[item['shopSku']]
                    except KeyError:
                        if item['shopSku'] in ids:
                            article = item['shopSku']
                            name = database.get_and('catalogue', {'id': article}).loc[0]['name']
                            database.insert('catalogue', [{'id': article, account_data['name']: article}])
                        else:
                            name = item['offerName']
                            article = database.get_sku({
                                account_data['name']: item['shopSku'],
                                'name': name
                            })
                            parther_translator[item['shopSku']] = article
                            names[item['shopSku']] = name
                    update.append({
                        'partner': account_data['name'],
                        'sale_id': sale['id'],
                        'seller_id': account_data['name'],
                        'date': str(pd.Timestamp(sale['creationDate'])),
                        'type': 'FBO',
                        'article': article,
                        'name': name,
                        'quantity': int(item['count']),
                        'price': float(item['prices'][0]['costPerItem']),
                        'warehouse': item['warehouse']['name'].split(', ')[-1].replace(')', '').split(' (')[-1],
                        'sale_region': sale['deliveryRegion']['name'],
                        'sale_city': 'Не указан',
                        'status': 'Cancelled' if sale['status'].lower() == 'cancelled' else 'Active',
                        'full_data': json.dumps(sale)
                    }
                    #     [
                    #     (name, sale['id'], item['marketSku']),
                    #     [
                    #         pd.Timestamp(sale['creationDate']),
                    #         ,
                    #         int(item['count']),
                    #         float(item['prices'][0]['costPerItem']),
                    #         float(item['prices'][0]['total']),
                    #         item['warehouse']['name'].split(', ')[-1].replace(')', '').split(' (')[-1],
                    #         sale['status'],
                    #         'Не загружаю'
                    #     ]
                    # ]
                    )
            database.insert('sales', update)
        except requests.exceptions.ConnectionError:
            print(f'Ошибка со стороны Беру - разрыв соединения сервером')
            continue
        try:
            params['page_token'] = response['result']['paging']['nextPageToken']
        except KeyError:
            break
        print(f'Страница {page} - готово')
        page += 1
    del database

    # if one_day is None:
    #     Params()[f'last_sales_load_date_{name}'] = min([date + dt.timedelta(days=30), dt.datetime.now()])
    # with lock:
    #     DataStore().inbox(update)


def stocks():
    result = None
    return result