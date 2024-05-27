import json
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = 100 if not batch_size else batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()
            if not message:
                break
            object_id, object_type, sent_dttm, payload = (
                    message['object_id'], message['object_type'], message['sent_dttm'], message['payload']
            )
            self._stg_repository.order_events_insert(object_id, object_type, sent_dttm, json.dumps(payload))
            user_id, restaurant_id  = payload['user']['id'], payload['restaurant']['id']
            user_name = self._redis.get(user_id)['name']
            restaurant_name = self._redis.get(restaurant_id)['name']
            products_list=[]
            for product in message['payload']['order_items']:
                    for item in self._redis.get(message['payload']['restaurant']['id'])['menu']:
                        if item['_id']==product["id"]:
                            product['name']=item['name']
                            product['category']=item['category']
                            
                    products_list.append(product)
        
            result={
                "object_id": message['object_id'],
                "object_type":message['object_type'],
                "payload": {
                    "id": message['object_id'],
                    "date": message['payload']['date'],
                    "cost": message['payload']['cost'],
                    "payment": message['payload']['payment'],
                    "status": message['payload']['final_status'],
                    "restaurant": {
                                    "id": restaurant_id,
                                    "name": restaurant_name,
                                },
                    "user": {
                        "id": user_id,
                        "name": user_name,
                    },
                    "products": products_list
                    }
                }
            self._producer.produce(result)

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
