import requests

url = 'https://marketdata.websol.barchart.com/getQuote.json'
params = {
    'apikey': 'edfaca2b49e5b010c2c39298a89a37ac',
    'symbols': ','.join(['AMD', 'AAPL', 'MSFT']),
    'fields': ','.join(['fiftyTwoWkHigh', 'fiftyTwoWkHighDate',
                        'fiftyTwoWkLow', 'fiftyTwoWkLowDate'])
}

e = requests.get(url, params=params)

url = 'https://marketdata.websol.barchart.com/getHistory.json'
params = {
    'apikey': 'edfaca2b49e5b010c2c39298a89a37ac',
    'symbol': 'AMD',
    'type': 'dailyContinue',
    'startDate': '20180901'
}

e = requests.get(url, params=params)

def getQuote(symbols):
    if isinstance(symbols, str):
        symbols = [symbols]

    url = 'https://marketdata.websol.barchart.com/getQuote.json'
    params = {
        'apikey': 'edfaca2b49e5b010c2c39298a89a37ac',
        'symbols': ','.join(symbols),
        'fields': ','.join(['fiftyTwoWkHigh', 'fiftyTwoWkHighDate',
                            'fiftyTwoWkLow', 'fiftyTwoWkLowDate'])
    }
