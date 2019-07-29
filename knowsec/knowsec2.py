import intrinio_sdk
from intrinio_sdk.rest import ApiException
from datetime import datetime as dt
import pandas


class Intrinio2:
    def __init__(self, api_key=None, page_size=None):
        if api_key is None:
            api_key = 'OmRhZDIzNTAwZTU3ODI5MDdhOWY2ZjFjN2IyMmQ0NWEy'
        else:
            pass

        intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key

        self._endpoints = {
            'exchanges': intrinio_sdk.StockExchangeApi(),
            'securities': intrinio_sdk.SecurityApi(),
        }

        self._page_size = page_size if page_size is not None else 1000

    def get_exchanges(self, set_index=True, city=None, country=None,
                      country_code=None, page_size=None):
        api_endpoint = self._endpoints['exchanges']

        response = self._endpoint_gen(api_endpoint.get_all_stock_exchanges,
                                      has_paging=False,
                                      city=city,
                                      country=country,
                                      country_code=country_code,
                                      page_size=page_size)

        records = [recs for reschunk in response for recs in reschunk]
        result = pandas.DataFrame.from_records(records)

        # set the column order
        colnames = ['id', 'acronym', 'mic', 'name', 'country', 'country_code',
                    'city', 'website', 'first_stock_price_date',
                    'last_stock_price_date']
        result = result[colnames]

        if set_index is True:
            result = result.set_index(['id', 'acronym', 'mic'])

        return result

    def get_securities(self, exchange_mic=None, composite_mic='USCOMP',
                       currency='USD', active=True, delisted=False,
                       page_size=None, **kwargs):
        kwargs.update({
            'exchange_mic': exchange_mic,
            'composite_mic': composite_mic,
            'currency': currency,
            'active': active,
            'delisted': delisted,
            'page_size': page_size
        })

        api_endpoint = self._endpoints['securities']

        response = self._endpoint_gen(api_endpoint.get_all_securities,
                                      **kwargs)

        records = [recs for reschunk in response for recs in reschunk]
        result = pandas.DataFrame.from_records(records)

        return result

    def get_prices_exchange(self, date, identifier='USCOMP', set_index=True,
                            page_size=None):
        api_endpoint = self._endpoints['exchanges'].get_stock_exchange_prices

        response = self._endpoint_gen(api_endpoint, has_paging=False,
                                      identifier=identifier,
                                      date=date,
                                      page_size=page_size)

        records = [recs for reschunk in response for recs in reschunk]

        security_prices = pandas.DataFrame.from_records(records)
        securities = security_prices['security'].tolist()
        securities = pandas.DataFrame.from_records(securities)
        security_prices = security_prices.drop('security', 1)

        securities = securities[['ticker']].copy()
        securities['exchange'] = identifier

        result = pandas.concat([security_prices, securities], axis=1)

        if set_index is True:
            result = result.set_index(['ticker', 'exchange', 'date'])

        return result

    def _endpoint_gen(self, api_endpoint, has_paging=True, page_size=None,
                      **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        next_page_str = ''

        if page_size is None:
            kwargs['page_size'] = self._page_size

        def response_value(kwargs=kwargs, next_page_str=next_page_str,
                           has_paging=has_paging):
            if has_paging is True:
                kwargs['next_page'] = next_page_str

            value_key = None

            while True:
                try:
                    api_response = api_endpoint(**kwargs)

                except ApiException as e:
                    msg = "Exception when calling StockExchangeApi"
                    msg += f"->get_all_stock_exchanges: {e}\r\n"

                    raise ApiException(msg)

                if value_key is None:
                    response_keys = list(api_response.to_dict().keys())
                    value_key = [i for i in response_keys
                                 if i != 'next_page'][0]

                yield api_response.to_dict()[value_key]

                if api_response.to_dict()['next_page'] is not None:
                    kwargs['next_page'] = api_response.to_dict()['next_page']

                else:
                    break

        return response_value()
