import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_user_category_counters(self,
                                   user_id: str,
                                   category_name: str,
                                   order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """

                     INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                     SELECT
                         h_user_pk
                        ,h_category_pk
                        ,%(category_name)s
                        ,%(order_cnt)s
                     FROM dds.h_user,dds.h_category
                     WHERE
                        h_user.user_id  = %(user_id)s
                        AND h_category.category_name  = %(category_name)s
                     ON CONFLICT (user_id,category_id) DO UPDATE
                    SET order_cnt = Excluded.order_cnt;
                    """,
                    {   
                        'user_id': user_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )


    def cdm_user_product_counters(self,
                                   user_id: str,
                                   product_name: str,
                                   order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    
                     INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                     SELECT
                         h_user_pk
                        ,h_product_pk
                        ,%(product_name)s
                        ,%(order_cnt)s
                     FROM dds.h_user,dds.s_product_names
                     WHERE
                        h_user.user_id  = %(user_id)s
                        AND s_product_names.name = %(product_name)s
                    
                    ON CONFLICT (user_id,product_id) DO UPDATE
                    SET order_cnt = Excluded.order_cnt;
                    """,
                    {   
                        'user_id': user_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )

