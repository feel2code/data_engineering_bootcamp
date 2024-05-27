from datetime import datetime, timezone
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int, 
                 logger: Logger
                 ) -> None:

        self._consumer=consumer
        self._cdm_repository=cdm_repository
        self._batch_size=batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")
        for _ in range(self._batch_size):
            message=self._consumer.consume()
            self._logger.info(f"{datetime.now(timezone.utc)}: {message}")
            if not message:
                break
            user_id=message['user_id']
            for product_id, product_name in zip(message['product_id_list'], message['product_name_list']):
                self._cdm_repository.cdm_insert('user_product', user_id, product_id, product_name)

            for category_id, category_name in zip(message['category_id_list'], message['category_name_list']):
                self._cdm_repository.cdm_insert('user_category', user_id, category_id, category_name)
    
        self._logger.info(f"{datetime.now(timezone.utc)}: CDM SENT")
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
