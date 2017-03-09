import requests
import json
import time
import datetime
import pdb


ZUORA_CHUNKSIZE = 50

class Zuora(object):
    def __init__(self, config):
        self.config = config
        self.auth = (config['user'], config['password'])
        self.accountingPeriods = None
        
    def _get(self, path, payload={}):
        response = requests.get(self.config['endpoint'] + path, 
                        auth=self.auth, params=payload)
        return self._unpackResponse('GET', path, response)
        
    def _delete(self, path):
        response = requests.delete(self.config['endpoint'] + path, 
                        auth=self.auth)
        return self._unpackResponse('GET', path, response)

    def _post(self, path, payload):
        response = requests.post(self.config['endpoint'] + path, 
                        json=payload,
                        auth=self.auth)
        return self._unpackResponse('POST', path, response)

    def _put(self, path, payload):
        response = requests.put(self.config['endpoint'] + path, 
                        json=payload,
                        auth=self.auth)
        return self._unpackResponse('POST', path, response)   

    def _unpackResponse(self, operation, path, response):
        if path != '/object/invoice/':
            assert response.status_code == 200, '{} to {} failed: {}'.format(operation, path, response.content)
        if path.startswith('/files/'):
            return response.text
        else:
            return json.loads(response.text)

    def query(self, queryString):    
        response = self._post("/action/query", {"queryString" : queryString})
        return response
        
    def queryAll(self, queryString):
        records = []
        response = self.query(queryString)
        records += response['records']
        
        while response['done'] == False:
            response = self.queryMore(response['queryLocator'])
            records += response['records']
        
        return records
        
    # Use queryMore to request additional results from a previous query call. If your initial query call returns more than 2000 results, you can use queryMore to query for the additional results.

    def queryMore(self, queryLocator):
        return self._post("/action/queryMore", {"queryLocator" : queryLocator})
        
    def revenueRecognitionRule(self, chargeKey):
        if isinstance(chargeKey, dict):
            if 'ChargeId' in chargeKey:
                chargeKey = chargeKey['ChargeId']
        response = self._get("/revenue-recognition-rules/subscription-charges/" + chargeKey)
        assert response['success'], response
        return response['revenueRecognitionRuleName']
                
    def getRevenueSchedulesForInvoiceItem(self, invoiceItemId):
        # assert len(self.query("select id from invoiceitem where id ='{}'".format(invoiceItemId))['records'])
        response = self._get("/revenue-schedules/invoice-items/" + invoiceItemId)
        return response

    def getRevenueSchedulesForSubscriptionCharge(self, chargeId):
        response = self._get("/revenue-schedules/subscription-charges/" + chargeId)
        return response

    # def deleteRevenueSchedule(self, rsNumber):
    #     response = self._delete("/revenue-schedules/" + rsNumber)
    #     assert response['success'], response

    def delete(self, objectType, ids):
        results = []
        chunks = [ids[i:i + ZUORA_CHUNKSIZE] for i in range(0, len(ids), ZUORA_CHUNKSIZE)]
        for chunk in chunks:
            results += self._post('/action/delete', {'type': objectType, 'ids': chunk})

        return results
        
    def getAccountPeriods(self):
        if not self.accountingPeriods:
            self.accountingPeriods = {}
            response = self._get("/accounting-periods/")
            assert response['success'], response
            for p in response['accountingPeriods']:
                self.accountingPeriods[p['name']] = p
            
        return self.accountingPeriods
    
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
    
    def revenueScheduleForInvoiceItem(self, invoiceItemId, payload):
        response = self._post('/revenue-schedules/invoice-items/' + invoiceItemId, payload)
        assert response['success'], response
        return response

    def revenueScheduleForSubscriptionCharge(self, invoiceItemId, payload):
        response = self._post('/revenue-schedules/subscription-charges/' + invoiceItemId, payload)
        assert response['success'], response
        return response

    def createExport(self, name, query, convertToCurrencies='USD', Encrypted=False, Format='csv', Zip=False):
        payload = {
            'Name': name,
            'Query': query,
            'ConvertToCurrencies': convertToCurrencies,
            'Encrypted': Encrypted,
            'Format': Format,
            'Zip': Zip
        }

        response = self._post('/object/export/', payload)
        assert response['Success'], response
        return response['Id']

    def retrieveExport(self, id, block=True):
        response = self._get('/object/export/' + id)

        if block:
            while response['Status'] in ['Pending', 'Processing']:
                time.sleep(2)
                response = self._get('/object/export/' + id)

        return response
    
    def deleteExport(self, id):
        response = self._delete('/object/export/' + id)
        assert response['success']
        return response

    def getFiles(self, id):
        response = self._get('/files/' + id)
        return response

    def queryExport(self, query):
        exportId = self.createExport('temp.csv', query)
        exportResponse = self.retrieveExport(exportId, block=True)
        if exportResponse['Status'] != 'Completed':
            return exportResponse

        fileResponse = self.getFiles(exportResponse['FileId'])
        self.deleteExport(exportId)
        return fileResponse

    def createInvoice(self, accountId, invoiceDate, targetDate,
                        includesOneTime=True,
                        includesRecurring=True,
                        includesUsage=True):

        if isinstance(invoiceDate, datetime.date):
            invoiceDate = invoiceDate.strftime('%Y-%m-%d')
        if isinstance(targetDate, datetime.date):
            targetDate = targetDate.strftime('%Y-%m-%d')

        payload={
            'AccountId': accountId,
            'IncludesOneTime': includesOneTime,
            'includesRecurring': includesRecurring,
            'IncludesUsage': includesUsage,
            'InvoiceDate': invoiceDate,
            'TargetDate': targetDate
        }

        response = self._post('/object/invoice/', payload)
        if not response['Success']:
            for error in response['Errors']:
                if error['Code'] == 'INVALID_VALUE' and 'no charges due' in error['Message']:
                    return None
        assert response['Success'], response
        return response

    def updateInvoice(self, invoiceId, payload):
        payload['Id'] = invoiceId
        response = self._put('/object/invoice/' + invoiceId, payload)
        assert response['Success'], response

    def createProduct(self, product):
        response = self._post('/object/product/', product)
        assert response['Success'], response
        return response['Id']                
    
    def updateProduct(self, productId, payload):
        payload['Id'] = productId
        response = self._put('/object/product/' + productId, payload)
        assert response['Success'], response

    def createProductRatePlan(self, ratePlan):
        response = self._post('/object/product-rate-plan/', ratePlan)
        assert response['Success'], response
        return response['Id']                
        
    def createProductRatePlanCharge(self, ratePlanCharge):
        response = self._post('/object/product-rate-plan-charge/', ratePlanCharge)
        assert response['Success'], response
        return response['Id']   

    def updateProductRatePlanCharge(self, ratePlanChargeId, payload):
        payload['Id'] = ratePlanChargeId
        response = self._put('/object/product-rate-plan-charge/' + ratePlanChargeId, payload)
        assert response['Success'], response

    def getAllAccountingPeriods(self):
        response = self._get('/accounting-periods/')
        assert response['success'], response
        return response['accountingPeriods']

    def updateAccountingPeriod(self, accountingPeriodId, payload):
        response = self._put('/accounting-periods/' + accountingPeriodId, payload)
        assert response['success'], response

    # not tested
    # def getCustomExchangeRates(self, currency, startDate, endDate):
    #     payload = {'startDate': startDate, 'endDate': endDate}
    #     response = self._get('/custom-exchange-rates/' + currency, payload=payload)
    #     pdb.set_trace()
    #     pass

    def createInvoiceItemAdjustment(self, type, amount, sourceType, sourceId, adjustmentDate, invoiceNumber=None, invoiceId=None):
        payload = {
            'Type': type,
            'Amount': amount,
            'SourceType': sourceType,
            'SourceId': sourceId,
            'AdjustmentDate': adjustmentDate
        }

        if invoiceId:
            payload['InvoiceId'] = invoiceId
        elif invoiceNumber:
            payload['InvoiceNumber'] = invoiceNumber
            
        response = self._post('/object/invoice-item-adjustment/', payload)
        if not response['Success']:
            pdb.set_trace()
        # assert response['Success'], response
        return response 

    def updateInvoiceItemAdjustment(self, id, reasonCode=None, status=None, transferredToAccounting=None):
        payload = {}
        if reasonCode:
            payload['ReasonCode'] = reasonCode
        if status:
            payload['Status'] = status
        if transferredToAccounting:
            payload['TransferredToAccounting'] = transferredToAccounting
        response = self._put('/object/invoice-item-adjustment/' + id, payload)
        assert response['Success'], response
        return response 
        
    def createBillRun(self, invoiceDate, targetDate,
                        accountId=None, 
                        autoEmail=False, 
                        autoPost=False, 
                        autoRenewal=False,
                        batch='AllBatches', 
                        billCycleDay='AllBillCycleDays',
                        chargeTypeToExclude='',
                        noEmailForZeroAmountInvoice=False):
        payload={
                    'InvoiceDate': invoiceDate if isinstance(invoiceDate, str) else invoiceDate.strftime('%Y-%m-%d'),
                    'TargetDate': targetDate if isinstance(targetDate, str) else targetDate.strftime('%Y-%m-%d'),
                    'AutoEmail': autoEmail, 
                    'AutoPost': autoPost, 
                    'AutoRenewal': autoRenewal,
                    'Batch': batch,
                    'BillCycleDay': billCycleDay,
                    'NoEmailForZeroAmountInvoice': noEmailForZeroAmountInvoice             
        }

        if accountId:
            payload['AccountId'] = accountId
        if chargeTypeToExclude:
            payload['ChargeTypeToExclude'] = chargeTypeToExclude

        response = self._post('/object/bill-run/', payload)
        assert response['Success'], response
        return response

    def createPayment(self, payload):
        response = self._post('/object/payment/', payload)
        return response


    # https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E1_SOAP_API_Object_Reference/CreditBalanceAdjustment
    # 
    # Requires you open a Zuora Support ticket to enable this feature
    #
    def createCreditBalanceAdjustment(self, payload):
        response = self._post('/object/credit-balance-adjustment/', payload)
        return response


    def createInvoiceSplit(self, invoiceId):
        payload = {
            'InvoiceId': invoiceId
        }
        response = self._post('/object/invoice-split/', payload)
        return response

    def createInvoiceSplitItem(self, invoiceSplitId, splitPercentage, invoiceDate, paymentTerm):
        payload = {
            'InvoiceSplitId': invoiceSplitId,
            'SplitPercentage': splitPercentage,
            'InvoiceDate': invoiceDate,
            'PaymentTerm': paymentTerm
        }

        response = self._post('/object/invoice-split-item/', payload)
        return response
        
    def executeInvoiceSplit(self, invoiceSplitId):
        payload = {
            'type': 'invoicesplit',
            'synchronous': False,
            'ids': [invoiceSplitId]
        }
        response = self._post('/action/execute/', payload)
        return response

    def createUsage(self, accountNumber, quantity, startDateTime, uom, extras={}):
        payload = {
            'AccountNumber': accountNumber,
            'quantity': quantity,
            'StartDateTime': startDateTime,
            'UOM': uom
        }

        payload.update(extras)
        response = self._post('/object/usage/', payload)
        return response
        
