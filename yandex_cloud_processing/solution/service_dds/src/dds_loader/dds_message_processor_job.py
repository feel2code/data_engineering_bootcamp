import uuid
import json
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer 
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository:DdsRepository,
                 batch_size: int, 
                 logger: Logger
                 ) -> None:
        self._consumer=consumer
        self._producer=producer
        self._dds_repository=dds_repository
        self._batch_size=batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")
        for _ in range(self._batch_size):
            message=self._consumer.consume()
            if not message:
                break
            load_dt=datetime.now(timezone.utc)
            load_src='stg-order-service'    
            msg_payload = json.dumps(message['payload'])
       
            category_list=[product['category'] for product in msg_payload['products']]
            products_id_list=[product['id'] for product in msg_payload['products']]
            products_name_list=[product['name'] for product in msg_payload['products']]
            
            self._logger.info(f"{datetime.now(timezone.utc)}: START OF THE DOWNLOAD CYCLE")

            #order hub 
            h_order_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(message['object_id']))     
            self._dds_repository.hubs_insert('order', h_order_pk, message['object_id'], msg_payload['date'], load_dt, load_src)
            self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_HUB_ORDER FINISHED")

            #user hub
            h_user_pk=uuid.uuid3(uuid.NAMESPACE_DNS, msg_payload['user']['id'])
            self._dds_repository.hubs_insert('user', h_user_pk, msg_payload['user']['id'], load_dt, load_src)
            self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_HUB_OTHERS FINISHED")
                    
            #restaurant hub 
            h_restaurant_pk=uuid.uuid3(uuid.NAMESPACE_DNS, msg_payload['restaurant']['id'])
            self._dds_repository.hubs_insert('restaurant', h_restaurant_pk, msg_payload['restaurant']['id'], load_dt, load_src)
            self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_HUB_OTHERS FINISHED")
                    
            #product hub   
            h_product_pk_list=[] 
            for product_id in products_id_list:
                h_product_pk=uuid.uuid3(uuid.NAMESPACE_DNS, product_id)
                h_product_pk_list.append(h_product_pk)
                self._dds_repository.hubs_insert('product', h_product_pk, product_id, load_dt, load_src)
                self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_HUB_PRODUCT FINISHED")

            #product name dictionary for product_names_sattelite
            product_pk_name_dict=dict(zip(h_product_pk_list, products_name_list))
            
            #category hub
            h_category_pk_list=[]
            for category_name in category_list:
                h_category_pk=uuid.uuid3(uuid.NAMESPACE_DNS, category_name)
                h_category_pk_list.append(h_category_pk)
                self._dds_repository.hubs_insert('category', h_category_pk, category_name, load_dt, load_src)
                self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_HUB_CATEGORY FINISHED")

            #order_product link
            for h_product_pk in h_product_pk_list:  
                hk_order_product_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(h_order_pk)+str(h_product_pk))
                self._dds_repository.links_insert('order_product', hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                
            #product_category link
            for h_product_pk, h_category_pk in zip(h_product_pk_list, h_category_pk_list):   
                hk_product_category_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(h_product_pk)+str(h_category_pk))
                self._dds_repository.links_insert('product_category', hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)

            #order_user link
            hk_order_user_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(h_order_pk)+str(h_user_pk))
            self._dds_repository.links_insert('order_user', hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)

            #product_restaurant link
            for h_product_pk in h_product_pk_list:  
                hk_product_restaurant_pk=uuid.uuid3(uuid.NAMESPACE_DNS,str(h_product_pk)+ str(h_restaurant_pk))
                self._dds_repository.links_insert('product_restaurant', hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)

            self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_LINKS  FINISHED")
            
            #'order_cost sattelite'
            cost=msg_payload['cost']
            payment=msg_payload['payment']

            hk_order_cost_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS,                     
                                         str(h_order_pk)+str(cost)+str(payment)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src  )
            self._dds_repository.sattelite_insert('order_cost', h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
            

            #'order_status sattelite'
            status=msg_payload['status']
            hk_order_status_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS,                     
                                         str(h_order_pk)+str(status)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src  )
            
            self._logger.info(f"hk_order_status_hashdiff: {hk_order_status_hashdiff}")
            self._dds_repository.sattelite_insert('order_status', h_order_pk, status,   load_dt, load_src, hk_order_status_hashdiff)

            #'product_names sattelite'     
            for h_product_pk in h_product_pk_list: 
                product_name=product_pk_name_dict[h_product_pk] 
                hk_product_names_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS,                     
                                         str(h_product_pk)+str(product_name)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src)
                self._dds_repository.sattelite_insert('product_names', h_product_pk, product_name,   load_dt, load_src, hk_product_names_hashdiff)
                             
            #'restaurant_names sattelite' 
            restaurant_name=msg_payload['restaurant']['name']
            hk_restaurant_names_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS,                     
                                         str(h_restaurant_pk)+str(restaurant_name)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src  )
            self._dds_repository.sattelite_insert('restaurant_names', h_restaurant_pk, restaurant_name,   load_dt, load_src, hk_restaurant_names_hashdiff)                     
            
            # 'user_names sattelite'
            username=msg_payload['user']['name']
            userlogin=msg_payload['user']['id']
            hk_user_names_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS,                     
                                         str(h_user_pk)+str(username)+str(userlogin)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src  )
            self._dds_repository.sattelite_insert('user_names', h_user_pk, username, userlogin,  load_dt, load_src, hk_user_names_hashdiff)                 


            self._logger.info(f"{datetime.now(timezone.utc)}: POSTGRES_SATTELITE  FINISHED")
         

            #output message sending
            output_message={
                "user_id": h_user_pk,
                "product_id_list": h_product_pk_list,
                "product_name_list": products_name_list,
                "category_id_list":   h_category_pk_list,
                "category_name_list": category_list
                
            }  
            self._logger.info(f"OUTPUT MESSAGE: {output_message}")
            self._producer.produce(output_message)
            self._logger.info(f"{datetime.now(timezone.utc)}: OUTPUT MESSAGE SEND")

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
