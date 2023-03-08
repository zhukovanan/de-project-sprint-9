from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:
        
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume(self)
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            data = msg['payload']

            #Прогружаем хаб категорий

            category = tuple(set(_.get('category') for _ in data['products']['restaurant_info']['menu']))

            for category_name in category:
                self._dds_repository.h_category_insert(
                category_name,
                datetime.utcnow(),
                'kafka')
            
            self._logger.info(f"{datetime.utcnow()}: Category hub is updated")


            #Прогружаем хаб заказов

            self._dds_repository.h_order_insert(
                data['id'],
                data['date'],
                datetime.utcnow(),
                'kafka')
            
            self._logger.info(f"{datetime.utcnow()}: Order hub is updated")


            #Прогружаем хаб ресторанов

            self._dds_repository.h_restaurant_insert(
                data['restaurant']['id'],
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: Restaurant hub is updated")


            #Прогружаем хаб пользователей

            self._dds_repository.h_user_insert(
                data['user']['id'],
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: User hub is updated")


            #Прогружаем хаб id продуктов

            product_ids = tuple(set(_.get('_id') for _ in data['products']['restaurant_info']['menu']))

            for p_id in product_ids:
                self._dds_repository.h_product_insert(
                p_id,
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: Product hub is updated")


            #Прогружаем линк состава заказов 

            order_item  = tuple(set(_.get('id') for _ in data['products']['value']))

            for item in order_item:
                self._dds_repository.l_order_product_insert(
                item,
                data['id'],
                datetime.utcnow(),
                'kafka')
            self._logger.info(f"{datetime.utcnow()}: Order_item link is updated")


             #Прогружаем линк заказы и пользователи

            self._dds_repository.l_order_user_insert(
                data['user']['id'],
                data['id'],
                datetime.utcnow(),
                'kafka')
            

            self._logger.info(f"{datetime.utcnow()}: Order_user link is updated")
            

            #Прогружаем линк категорий продуктов и меню ресторанов


            product_category = data['products']['restaurant_info']['menu']

            for v in product_category:
                self._dds_repository.l_product_category_insert(
                v.get('_id'),
                v.get('category'),
                datetime.utcnow(),
                'kafka')


                self._dds_repository.l_product_restaurant_insert(
                v.get('_id'),
                data['restaurant']['id'],
                datetime.utcnow(),
                'kafka')



            #Прогружаем сателлит по стоимости заказа

            self._dds_repository.s_order_cost_insert(
                data['id'],
                data['cost'],
                data['payment'],
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: S_order_cost satellite is updated")

            #Прогружаем сателлит по статусу заказа

            self._dds_repository.s_order_status_insert(
                data['id'],
                data['status'],
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: S_order_status satellite is updated")


            #Прогружаем сателлит по названиям продуктов

            for _ in data['products']['restaurant_info']['menu']:

                self._dds_repository.s_product_names_insert(
                _.get('_id'),
                _.get('name'),
                datetime.utcnow(),
                'kafka')

            self._logger.info(f"{datetime.utcnow()}: S_product_names satellite is updated")


            #Прогружаем сателлит по названию ресторана

            self._dds_repository.s_restaurant_names_insert(
                data['restaurant']['id'],
                data['restaurant']['name'],
                datetime.utcnow(),
                'kafka')
            

            self._logger.info(f"{datetime.utcnow()}: S_restaurant_names satellite is updated")



            #Прогружаем сателлит по покупателям


            self._dds_repository.s_user_names_insert(
                data['user']['id'],
                data['user']['login'],
                data['user']['name'],
                datetime.utcnow(),
                'kafka')
            

            self._logger.info(f"{datetime.utcnow()}: S_user_names satellite is updated")


            dst_msg = {"user_product": self._dds_repository.get_user_product_counters(),
                       "user_category": self._dds_repository.get_user_category_counters()}
            

            self._logger.info(dst_msg)
            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")


        self._logger.info(f"{datetime.utcnow()}: FINISH")