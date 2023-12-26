from kafka import KafkaProducer
import json


producer = KafkaProducer(bootstrap_servers='192.168.1.247:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

mensagem = {'nome': 'ERIC2', 'idade': 35, 'glicemia': 130, 'temperatura_corporal': 35.5, 'pressao_arterial': '110/80', 'batimentos_cardiacos': 90}


producer.send('dados-medicos', mensagem)