from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 ) -> None:
        
        self._consumer = consumer
        self._logger = logger
        self._batch_size = 100
        self._cdm_repository = cdm_repository

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume(self)
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")


            #Прогружаем данные по полученной витрине по категории
            for user_category in msg['user_category']:
                self._logger.info(user_category)
                self._cdm_repository.cdm_user_category_counters(
                    user_category[0],
                    user_category[1],
                    user_category[2])
                

            #Прогружаем данные по полученной витрине по продукту
            for user_product in msg['user_product']:
                self._logger.info(user_product)
                self._cdm_repository.cdm_user_product_counters(
                    user_product[0],
                    user_product[1],
                    user_product[2])

        self._logger.info(f"{datetime.utcnow()}: FINISH")
