import requests
import json
import time
from kafka import KafkaProducer

# configurações do produtor Kafka
kafka_topic = 'moedas'
kafka_bootstrap_servers = ['192.168.1.247:9092']
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# enviar solicitação HTTP e obter o valor de USD-BRL e EUR-BRL
response = requests.get('https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL')
json_data = json.loads(response.content)
dolar_bid = float(json_data['USDBRL']['bid'])
dolar_var = float(json_data['USDBRL']['varBid'])
dolar_pct = float(json_data['USDBRL']['pctChange'])
dolar_date = str(json_data['USDBRL']['create_date'])
euro_bid = float(json_data['EURBRL']['bid'])
euro_var = float(json_data['EURBRL']['varBid'])
euro_pct = float(json_data['EURBRL']['pctChange'])
euro_date = str(json_data['EURBRL']['create_date'])

print(dolar_bid)
print(dolar_pct)
print(dolar_var)
print(dolar_date)
print('###############################')
print(euro_bid)
print(euro_pct)
print(euro_var)
print(euro_date)

# criar mensagem para enviar para o Kafka
message = {
            'dolar_bid': dolar_bid,
            'dolar_var': dolar_var, 
            'dolar_pct': dolar_pct, 
            'dolar_date': dolar_date, 
            'euro_bid': euro_bid, 
            'euro_pct': euro_pct, 
            'euro_var': euro_var, 
            'euro_date':euro_date
        }

print(message)

# enviar mensagem para o Kafka
producer.send(kafka_topic, message)

