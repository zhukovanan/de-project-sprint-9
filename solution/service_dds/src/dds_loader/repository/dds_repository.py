import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_category_insert(self,
                          category_name: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.h_category (h_category_pk,category_name,load_dt,load_src)
                     VALUES(%(h_category_pk)s,%(category_name)s,%(load_dt)s,%(load_src)s)
                     ON CONFLICT (category_name) DO UPDATE
                     SET h_category_pk = Excluded.h_category_pk, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'h_category_pk': uuid.uuid1(),
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def h_order_insert(self,
                          order_id: int,
                          order_dt: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.h_order (h_order_pk,order_id,order_dt,load_dt,load_src)
                     VALUES(%(h_order_pk)s,%(order_id)s,%(order_dt)s,%(load_dt)s,%(load_src)s)
                     ON CONFLICT (order_id) DO UPDATE
                     SET h_order_pk = Excluded.h_order_pk,order_id = Excluded.order_id, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'h_order_pk': uuid.uuid1(),
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_product_insert(self,
                          product_id: int,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.h_product (h_product_pk,product_id,load_dt,load_src)
                     VALUES(%(h_product_pk)s,%(product_id)s,%(load_dt)s,%(load_src)s)
                     ON CONFLICT (product_id) DO UPDATE
                     SET h_product_pk = Excluded.h_product_pk, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'h_product_pk': uuid.uuid1(),
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_restaurant_insert(self,
                          restaurant_id: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.h_restaurant (h_restaurant_pk,restaurant_id,load_dt,load_src)
                     VALUES(%(h_restaurant_pk)s,%(restaurant_id)s,%(load_dt)s,%(load_src)s)
                     ON CONFLICT (restaurant_id) DO UPDATE
                     SET h_restaurant_pk = Excluded.h_restaurant_pk, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'h_restaurant_pk': uuid.uuid1(),
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )



    def h_user_insert(self,
                          user_id: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.h_user (h_user_pk,user_id,load_dt,load_src)
                     VALUES(%(h_user_pk)s,%(user_id)s,%(load_dt)s,%(load_src)s)
                     ON CONFLICT (user_id) DO UPDATE
                     SET h_user_pk = Excluded.h_user_pk, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'h_user_pk': uuid.uuid1(),
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_order_product_insert(self,
                          product_id: str,
                          order_id: int,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.l_order_product (hk_order_product_pk,h_product_pk,h_order_pk,load_dt,load_src)
                     SELECT
                        %(hk_order_product_pk)s
                        ,h_product_pk
                        ,h_order_pk
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_order,dds.h_product
                     WHERE
                        h_order.order_id = %(h_order_pk)s
                        AND h_product.product_id = %(h_product_pk)s
                        AND NOT EXISTS (SELECT
                                            h_product_pk
                                            ,h_order_pk
                                        FROM dds.l_order_product AS lop
                                        WHERE
                                            lop.h_product_pk = h_product.h_product_pk
                                            AND lop.h_order_pk = h_order.h_order_pk);

                    """,
                    {   
                        'hk_order_product_pk': uuid.uuid1(),
                        'h_product_pk': product_id,
                        'h_order_pk': order_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_order_user_insert(self,
                          user_id: str,
                          order_id: int,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.l_order_user (hk_order_user_pk,h_order_pk,h_user_pk,load_dt,load_src)
                     SELECT
                        %(hk_order_user_pk)s
                        ,h_order_pk
                        ,h_user_pk
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_order,dds.h_user
                     WHERE
                        h_order.order_id = %(h_order_pk)s
                        AND h_user.user_id = %(h_user_pk)s
                        AND NOT EXISTS (SELECT
                                            h_user_pk
                                            ,h_order_pk
                                        FROM dds.l_order_user AS lop
                                        WHERE
                                            lop.h_user_pk = h_user.h_user_pk
                                            AND lop.h_order_pk = h_order.h_order_pk);

                    """,
                    {   
                        'hk_order_user_pk': uuid.uuid1(),
                        'h_user_pk': user_id,
                        'h_order_pk': order_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_product_category_insert(self,
                          product_id: str,
                          category_name: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.l_product_category (hk_product_category_pk,h_product_pk,h_category_pk,load_dt,load_src)
                     SELECT
                        %(hk_product_category_pk)s
                        ,h_product_pk
                        ,h_category_pk
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_category,dds.h_product
                     WHERE
                        h_product.product_id = %(h_product_pk)s
                        AND h_category.category_name = %(h_category_pk)s
                        AND NOT EXISTS (SELECT
                                            h_product_pk
                                            ,h_category_pk
                                        FROM dds.l_product_category AS lop
                                        WHERE
                                            lop.h_product_pk = h_product.h_product_pk
                                            AND lop.h_category_pk = h_category.h_category_pk);

                    """,
                    {   
                        'hk_product_category_pk': uuid.uuid1(),
                        'h_product_pk': product_id,
                        'h_category_pk': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    
    def l_product_restaurant_insert(self,
                          product_id: str,
                          restaurant_id: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk,h_product_pk,h_restaurant_pk,load_dt,load_src)
                     SELECT
                        %(hk_product_restaurant_pk)s
                        ,h_product_pk
                        ,h_restaurant_pk
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_restaurant,dds.h_product
                     WHERE
                        h_product.product_id = %(h_product_pk)s
                        AND h_restaurant.restaurant_id = %(h_restaurant_pk)s
                        AND NOT EXISTS (SELECT
                                            h_product_pk
                                            ,h_restaurant_pk
                                        FROM dds.l_product_restaurant AS lop
                                        WHERE
                                            lop.h_product_pk = h_product.h_product_pk
                                            AND lop.h_restaurant_pk = h_restaurant.h_restaurant_pk);

                    """,
                    {   
                        'hk_product_restaurant_pk': uuid.uuid1(),
                        'h_product_pk': product_id,
                        'h_restaurant_pk': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    


    def s_order_cost_insert(self,
                          order_id: int,
                          cost: float,
                          payment: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.s_order_cost (hk_order_cost_pk,h_order_pk,cost,payment,load_dt,load_src)
                     SELECT
                        %(hk_order_cost_pk)s
                        ,h_order_pk
                        ,%(cost)s
                        ,%(payment)s
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_order
                     WHERE
                        h_order.order_id = %(h_order_pk)s

                     ON CONFLICT (h_order_pk) DO UPDATE
                     SET cost = Excluded.cost,  payment = Excluded.payment, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'hk_order_cost_pk': uuid.uuid1(),
                        'h_order_pk': order_id,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )



    def s_order_status_insert(self,
                          order_id: int,
                          status: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.s_order_status (hk_order_status_pk,h_order_pk,status,load_dt,load_src)
                     SELECT
                        %(hk_order_status_pk)s
                        ,h_order_pk
                        ,%(status)s
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_order
                     WHERE
                        h_order.order_id = %(h_order_pk)s
                     ON CONFLICT (h_order_pk) DO UPDATE
                     SET status = Excluded.status, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'hk_order_status_pk': uuid.uuid1(),
                        'h_order_pk': order_id,
                        'status': status,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    

    def s_product_names_insert(self,
                          product_id: str,
                          product_name: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.s_product_names (hk_product_names_pk,h_product_pk,name,load_dt,load_src)
                     SELECT
                        %(hk_product_names_pk)s
                        ,h_product_pk
                        ,%(product_name)s
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_product
                     WHERE
                        h_product.product_id = %(h_product_pk)s
                     ON CONFLICT (h_product_pk) DO UPDATE
                     SET name = Excluded.name, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'hk_product_names_pk': uuid.uuid1(),
                        'h_product_pk': product_id,
                        'product_name': product_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    
    def s_restaurant_names_insert(self,
                          restaurant_id: str,
                          restaurant_name: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.s_restaurant_names (hk_restaurant_names_pk,h_restaurant_pk,name,load_dt,load_src)
                     SELECT
                        %(hk_restaurant_names_pk)s
                        ,h_restaurant_pk
                        ,%(restaurant_name)s
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_restaurant
                     WHERE
                        h_restaurant.restaurant_id = %(h_restaurant_pk)s
                     ON CONFLICT (h_restaurant_pk) DO UPDATE
                     SET name = Excluded.name, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'hk_restaurant_names_pk': uuid.uuid1(),
                        'h_restaurant_pk': restaurant_id,
                        'restaurant_name': restaurant_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    
    def s_user_names_insert(self,
                          user_id: str,
                          user_login: str,
                          user_name: str,
                          load_dt: datetime,
                          load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     INSERT INTO dds.s_user_names (hk_user_names_pk,h_user_pk,username,userlogin,load_dt,load_src)
                     SELECT
                        %(hk_user_names_pk)s
                        ,h_user_pk
                        ,%(user_name)s
                        ,%(user_login)s
                        ,%(load_dt)s
                        ,%(load_src)s
                     FROM dds.h_user
                     WHERE
                        h_user.user_id = %(h_user_pk)s
                     ON CONFLICT (h_user_pk) DO UPDATE
                     SET username = Excluded.username, userlogin = Excluded.userlogin, load_dt = Excluded.load_dt, load_src = Excluded.load_src;
                    """,
                    {   
                        'hk_user_names_pk': uuid.uuid1(),
                        'h_user_pk': user_id,
                        'user_name': user_name,
                        'user_login' : user_login,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )



    def get_user_category_counters(self,):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                select
	                hu.user_id AS user_id
                    ,hc.category_name AS category_name
                    ,COUNT(distinct lou.h_order_pk) as order_cnt
                from dds.l_order_user lou
                	left join dds.h_user hu 
                		on hu.h_user_pk = lou.h_user_pk 
                    left join dds.l_order_product lop 
                        on lou.h_order_pk = lop.h_order_pk 
                    left join dds.l_product_category lpc 
                        on lpc.h_product_pk = lop.h_product_pk 
                    left join dds.h_category hc
                        on hc.h_category_pk  = lpc.h_category_pk
                group by 
                    hu.user_id 
                    ,hc.category_name
                """)

                return cur.fetchall()


    def get_user_product_counters(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                select
                     hu.user_id 
                    ,spn.name
                    ,COUNT(distinct lou.h_order_pk) as order_cnt
                from dds.l_order_user lou
                    left join dds.l_order_product lop 
                        on lou.h_order_pk = lop.h_order_pk
                    left join dds.h_user hu 
                		on hu.h_user_pk = lou.h_user_pk 
                    left join dds.s_product_names spn
                        on spn.h_product_pk  = lop.h_product_pk 
                group by 
                    hu.user_id
                    ,spn.name
                """)

                return cur.fetchall()

    
