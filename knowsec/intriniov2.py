import intrinio_sdk
from intrinio_sdk.rest import ApiException
import pandas


class Intrinio2:
    def __init__(self, api_key=None):
        if api_key is None:
            api_key = 'OmRhZDIzNTAwZTU3ODI5MDdhOWY2ZjFjN2IyMmQ0NWEy'
        else:
            pass

        intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key

        self._endpoints = {
            'stock-exchanges': intrinio_sdk.StockExchangeApi()
        }

    def get_exchanges(self, set_index=True, city=None, country=None,
                      country_code=None, page_size=100):
        api_endpoint = self._endpoints['stock-exchanges']

        response = self._execute_endpoint(api_endpoint, city=city,
                                          country=country,
                                          country_code=country_code,
                                          page_size=page_size)

        result = response.to_dict()['stock_exchanges']
        result = pandas.DataFrame.from_records(result)

        # set the column order
        colnames = ['id', 'acronym', 'mic', 'name', 'country', 'country_code',
                    'city', 'website', 'first_stock_price_date',
                    'last_stock_price_date']
        result = result[colnames]

        if set_index is True:
            result = result.set_index(['id', 'acronym', 'mic'])

        return result

    # def get_securities(self, active=active,)

    def _execute_endpoint(self, api_endpoint, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        try:
            api_response = api_endpoint.get_all_stock_exchanges(**kwargs)
            result = api_response

            return result

        except ApiException as e:
            msg = "Exception when calling StockExchangeApi"
            msg += f"->get_all_stock_exchanges: {e}\r\n"

            raise ApiException(msg)
