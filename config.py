import os
import json

#
# Zuora REST configuration
#

ZUORA_CONFIGFILE = os.path.expanduser('~') + '/.zuora-sandbox-config.json'
ZUORA_REST_ENDPOINT = 'https://rest.apisandbox.zuora.com/v1'
with open(ZUORA_CONFIGFILE, 'r') as f:
     zuoraConfig = json.load(f)    
zuoraConfig['endpoint'] = ZUORA_REST_ENDPOINT
