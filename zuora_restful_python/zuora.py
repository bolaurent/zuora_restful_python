"""
    Class Zuora

    wraps the Zuora rest api
"""

# pylint: disable=C0111,R0904,R0913


import datetime
import json
import time

import requests


ZUORA_CHUNKSIZE = 50


def _unpack_response(operation, path, response):
    if path != '/object/invoice/':
        assert response.status_code == 200, \
                '{} to {} failed: {}'.format(operation, path, response.content)
    if path.startswith('/files/'):
        return response.text

    return json.loads(response.text)


class Zuora(object):
    """
    instantiates a connection to Zuora service
    """

    def __init__(self, username, password, endpoint='production', headers={}):
        self.auth = (username, password)

        if endpoint == 'production':
            self.endpoint = 'https://rest.zuora.com/v1'
        elif endpoint == 'sandbox':
            self.endpoint = 'https://rest.apisandbox.zuora.com/v1'
        else:
            self.endpoint = endpoint

        self.accounting_periods = None
        self.headers = headers

    def _get(self, path, payload=None):
        response = requests.get(self.endpoint + path,
                                auth=self.auth, 
                                headers=self.headers,
                                params=payload)
        return _unpack_response('GET', path, response)

    def _delete(self, path):
        response = requests.delete(self.endpoint + path,
                                   auth=self.auth,
                                  headers=self.headers)
        return _unpack_response('GET', path, response)

    def _post(self, path, payload):
        response = requests.post(self.endpoint + path,
                                 json=payload,
                                 auth=self.auth,
                                 headers=self.headers)
        return _unpack_response('POST', path, response)

    def _put(self, path, payload):
        response = requests.put(self.endpoint + path,
                                json=payload,
                                auth=self.auth,
                                headers=self.headers)
        return _unpack_response('POST', path, response)

    def query(self, query_string):
        response = self._post("/action/query", {"queryString" : query_string})
        return response

    def query_all(self, query_string):
        records = []
        response = self.query(query_string)
        records += response['records']

        while not response['done']:
            response = self.query_more(response['queryLocator'])
            records += response['records']

        return records

    # Use query_more to request additional results from a previous query call.
    # If your initial query call returns more than 2000 results, you can use queryMore
    # to query for the additional results.

    def query_more(self, query_locator):
        return self._post("/action/queryMore", {"queryLocator" : query_locator})

    def revenue_recognition_rule(self, charge_key):
        if isinstance(charge_key, dict):
            if 'ChargeId' in charge_key:
                charge_key = charge_key['ChargeId']
        response = self._get("/revenue-recognition-rules/subscription-charges/" + charge_key)
        assert response['success'], response
        return response['revenueRecognitionRuleName']

    def get_revenue_schedules_for_invoice_item(self, object_id):
        response = self._get("/revenue-schedules/invoice-items/" + object_id)
        return response

    def get_revenue_schedules_for_subscription_charge(self, object_id):
        response = self._get("/revenue-schedules/subscription-charges/" + object_id)
        return response

    def delete(self, object_type, ids):
        results = []
        chunks = [ids[i:i + ZUORA_CHUNKSIZE] for i in range(0, len(ids), ZUORA_CHUNKSIZE)]
        for chunk in chunks:
            results += self._post('/action/delete', {'type': object_type, 'ids': chunk})

        return results

    def get_account_periods(self):
        if not self.accounting_periods:
            self.accounting_periods = {}
            response = self._get("/accounting-periods/")
            assert response['success'], response
            for period in response['accountingPeriods']:
                self.accounting_periods[period['name']] = period

        return self.accounting_periods

    # samplePayload = {
    #     "revenueDistributions": [
    #         {
    #             "accountingPeriodName": "Jan '16",
    #             "newAmount": "20"
    #         },
    #         {
    #             "accountingPeriodName": "Open-Ended",
    #             "newAmount": "30"
    #         }
    #     ],
    #     "revenueEvent": {
    #         "eventType": "Revenue Distributed",
    #         "eventTypeSystemId": "RevenueDistributed__z",
    #         "notes": "My notes"
    #     }
    # }

    def revenue_schedule_for_invoice_item(self, object_id, payload):
        response = self._post('/revenue-schedules/invoice-items/' + object_id, payload)
        assert response['success'], response
        return response

    def revenue_schedule_for_subscription_charge(self, object_id, payload):
        response = self._post('/revenue-schedules/subscription-charges/' + object_id, payload)
        assert response['success'], response
        return response

    def create_export(self, name, query,
                      convert_to_currencies='USD',
                      encrypted=False,
                      file_format='csv',
                      file_zip=False):
        payload = {
            'Name': name,
            'Query': query,
            'ConvertToCurrencies': convert_to_currencies,
            'Encrypted': encrypted,
            'Format': file_format,
            'Zip': file_zip
        }

        response = self._post('/object/export/', payload)
        assert response['Success'], response
        return response['Id']

    def retrieve_export(self, object_id, block=True):
        response = self._get('/object/export/' + object_id)

        if block:
            while response['Status'] in ['Pending', 'Processing']:
                time.sleep(2)
                response = self._get('/object/export/' + object_id)

        return response

    def delete_export(self, object_id):
        response = self._delete('/object/export/' + object_id)
        assert response['success']
        return response

    def get_files(self, object_id):
        response = self._get('/files/' + object_id)
        return response

    def query_export(self, query):
        export_id = self.create_export('temp.csv', query)
        export_response = self.retrieve_export(export_id, block=True)
        if export_response['Status'] != 'Completed':
            return export_response

        file_response = self.get_files(export_response['FileId'])
        self.delete_export(export_id)
        return file_response

    def update_object(self, object_name, object_id, payload):
        payload['Id'] = object_id
        response = self._put('/object/{}/'.format(object_name) + object_id, payload)
        assert response['Success'], response

    def create_object(self, object_name, payload):
        response = self._post('/object/{}/'.format(object_name), payload)
        assert response['Success'], response
        return response

    def create_invoice(self, account_id, invoice_date, target_date,
                       includes_one_time=True,
                       includes_recurring=True,
                       includes_usage=True):

        if isinstance(invoice_date, datetime.date):
            invoice_date = invoice_date.strftime('%Y-%m-%d')
        if isinstance(target_date, datetime.date):
            target_date = target_date.strftime('%Y-%m-%d')

        payload = {
            'AccountId': account_id,
            'IncludesOneTime': includes_one_time,
            'includesRecurring': includes_recurring,
            'IncludesUsage': includes_usage,
            'InvoiceDate': invoice_date,
            'TargetDate': target_date
        }

        response = self._post('/object/invoice/', payload)
        if not response['Success']:
            for error in response['Errors']:
                if error['Code'] == 'INVALID_VALUE' and 'no charges due' in error['Message']:
                    return None
        assert response['Success'], response
        return response

    def update_invoice(self, object_id, payload):
        return self.update_object('invoice', object_id, payload)

    def create_product(self, product):
        return self.create_object('product', product)

    def update_product(self, object_id, payload):
        return self.update_object('product', object_id, payload)

    def create_product_rate_plan(self, product_rate_plan):
        return self.create_object('product-rate-plan', product_rate_plan)

    def create_product_rate_plan_charge(self, product_rate_plan_charge):
        return self.create_object('product-rate-plan-charge', product_rate_plan_charge)

    def update_product_rate_plan_charge(self, object_id, payload):
        return self.update_object('rate', object_id, payload)

    def get_all_accounting_periods(self):
        response = self._get('/accounting-periods/')
        assert response['success'], response
        return response['accountingPeriods']

    def update_accounting_period(self, object_id, payload):
        response = self._put('/accounting-periods/' + object_id, payload)
        assert response['success'], response

    def create_invoice_item_adjustment(self, adjustment_type, amount, source_type, source_id,
                                       adjustment_date,
                                       invoice_number=None,
                                       invoice_id=None):
        payload = {
            'Type': adjustment_type,
            'Amount': amount,
            'SourceType': source_type,
            'SourceId': source_id,
            'AdjustmentDate': adjustment_date
        }

        if invoice_id:
            payload['InvoiceId'] = invoice_id
        elif invoice_number:
            payload['InvoiceNumber'] = invoice_number

        response = self._post('/object/invoice-item-adjustment/', payload)
        assert response['Success'], response
        return response

    def update_invoice_item_adjustment(self, object_id,
                                       reason_code=None,
                                       status=None,
                                       transferred_to_accounting=None):
        payload = {}
        if reason_code:
            payload['ReasonCode'] = reason_code
        if status:
            payload['Status'] = status
        if transferred_to_accounting:
            payload['TransferredToAccounting'] = transferred_to_accounting
        response = self._put('/object/invoice-item-adjustment/' + object_id, payload)
        assert response['Success'], response
        return response

    def create_bill_run(self, invoice_date, target_date,
                        account_id=None,
                        auto_email=False,
                        auto_post=False,
                        auto_renewal=False,
                        batch='AllBatches',
                        bill_cycle_day='AllBillCycleDays',
                        charge_type_to_exclude='',
                        no_email_for_zero_amount_invoice=False):
        # pylint: disable=line-too-long
        payload = {
            'InvoiceDate': invoice_date if isinstance(invoice_date, str) else invoice_date.strftime('%Y-%m-%d'),
            'TargetDate': target_date if isinstance(target_date, str) else target_date.strftime('%Y-%m-%d'),
            'AutoEmail': auto_email,
            'AutoPost': auto_post,
            'AutoRenewal': auto_renewal,
            'Batch': batch,
            'BillCycleDay': bill_cycle_day,
            'NoEmailForZeroAmountInvoice': no_email_for_zero_amount_invoice
        }
        # pylint: enable=line-too-long

        if account_id:
            payload['AccountId'] = account_id
        if charge_type_to_exclude:
            payload['ChargeTypeToExclude'] = charge_type_to_exclude

        response = self._post('/object/bill-run/', payload)
        assert response['Success'], response
        return response

    def create_payment(self, payment):
        return self.create_object('payment', payment)


    # https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E1_SOAP_API_Object_Reference/CreditBalanceAdjustment
    #
    # Requires you open a Zuora Support ticket to enable this feature
    #
    def create_credit_balance_adjustment(self, payload):
        return self.create_object('credit-balance-adjustment', payload)

    def create_invoice_split(self, invoice_id):
        payload = {
            'InvoiceId': invoice_id
        }
        response = self._post('/object/invoice-split/', payload)
        return response

    def create_invoice_split_item(self,
                                  invoice_split_id,
                                  split_percentage,
                                  invoice_date,
                                  payment_term):
        payload = {
            'InvoiceSplitId': invoice_split_id,
            'SplitPercentage': split_percentage,
            'InvoiceDate': invoice_date,
            'PaymentTerm': payment_term
        }

        response = self._post('/object/invoice-split-item/', payload)
        return response

    def execute_invoice_split(self, invoice_split_id):
        payload = {
            'type': 'invoicesplit',
            'synchronous': False,
            'ids': [invoice_split_id]
        }
        response = self._post('/action/execute/', payload)
        return response

    def create_usage(self, account_number, quantity, start_date_time, uom, extras=None):
        payload = {
            'AccountNumber': account_number,
            'quantity': quantity,
            'StartDateTime': start_date_time,
            'UOM': uom
        }

        payload.update(extras)
        response = self._post('/object/usage/', payload)
        return response
    
    def create_subscription(self, payload):
        response = self._post('/subscriptions/', payload)
        return response
    
        
