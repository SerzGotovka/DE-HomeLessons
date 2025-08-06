from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging
import os
from models import (
    User,
    Card,
    Product,
    Delivery_companies,
    Couriers,
    Warehouse,
    Orders,
    Deliveri,
)

logger = logging.getLogger(__name__)


@task()
def extract_raw_data(table_name: str):
    """Чтение данных из raw-слоя"""
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = pg_hook.get_conn()

    df = pd.read_sql(f"select * from raw.{table_name}", conn)

    return df

@task
def transform_and_load_users(data_df):
    """Функция ддя записи users из raw в dds"""

    users_df = (
        data_df[["user_id", "name", "surname", "age", "email", "phone"]]
        .drop_duplicates()
        .copy()
    )

    users_list = users_df.to_dict(orient="records")
    logger.info(users_list)

    # Провалидировали
    valid_users = []
    for record in users_list:
        try:
            user = User(**record)
            valid_users.append(user.model_dump())
        except Exception as e:
            logger.error(f"Невалидная запись пользователя {record}", e)

    users_valid_df = pd.DataFrame(valid_users)

    # Создаем соединение
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    # убираем те, что уже были
    if not users_valid_df.empty:
        existing_users = pd.read_sql("select user_id from dds.users", engine)
        existing_users_ids = set(existing_users["user_id"])
        users_valid_df = users_valid_df[
            ~users_valid_df["user_id"].isin(existing_users_ids)
        ]

    # Записываем результаты
    users_valid_df.to_sql(
        "users", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"кол-во строчек вставлено - {users_valid_df.shape[0]}")

@task
def transform_and_load_cards(data_df):
    """Функция ддя записи cards из raw в dds"""
    cards_df = data_df[["card_number", "user_id"]].drop_duplicates().copy()

    cards_list = cards_df.to_dict(orient="records")
    logger.info(cards_list)

    # Валидация
    valid_cards = []
    for record in cards_list:
        try:
            card = Card(**record)
            valid_cards.append(card.model_dump())
        except Exception as e:
            logger.error(f"Невалидная запись cards {record}", e)

    valid_cards_df = pd.DataFrame(valid_cards)

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not valid_cards_df.empty:
        existing_cards = pd.read_sql("select user_id from dds.cards", engine)
        existing_cards_ids = set(existing_cards["user_id"])
        valid_cards_df = valid_cards_df[
            ~valid_cards_df["user_id"].isin(existing_cards_ids)
        ]

        valid_cards_df.to_sql(
            "cards", engine, schema="dds", if_exists="append", index=False
        )

        logger.info(f"Кол-во строк вставлено - {valid_cards_df.shape[0]}")

@task
def transform_and_load_products(data_df):
    """Функция ддя записи products из raw в dds"""
    products_df = data_df[["product"]].drop_duplicates().copy()

    products_list = products_df.to_dict(orient="records")
    logger.info(products_list)

    valid_products = []
    for record in products_list:
        try:
            product = Product(**record)
            valid_products.append(product.model_dump())
        except Exception as e:
            logger.error(f"Невалидная запись product {record}", e)

    valid_products_df = pd.DataFrame(valid_products)
    valid_products_df.columns = ["product_name"]

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    # - Проверка на пустоту DataFrame:
    if not valid_products_df.empty:
        # - Чтение существующих продуктов из базы данных:
        existing_products = pd.read_sql(
            "select product_name from dds.products", engine
        )
        # - Создание множества имен существующих продуктов:
        #  - Множество используется для обеспечения уникальности имен продуктов и ускорения последующих операций проверки наличия (поскольку проверка значения в множестве осуществляется быстрее, чем в списке).
        existings_products_name = set(existing_products["product_name"])

        """- Фильтрация валидных продуктов:
        - Используется условная фильтрация для удаления из valid_products_df тех продуктов, которые уже существуют в базе данных.
        - valid_products_df['product_name'].isin(existings_products_name) создает булев массив, где True соответствует продуктам, которые есть в existings_products_name.
        - ~ (операция "не") инвертирует эти значения, т.е. получает True для тех продуктов, которые не находятся в existings_products_name.
        - В итоге, только новые продукты (те, которые не существуют в базе данных) остаются в valid_products_df."""
        valid_products_df = valid_products_df[
            ~valid_products_df["product_name"].isin(existings_products_name)
        ]

    valid_products_df.to_sql(
        "products", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_products_df.shape[0]}")

    return valid_products_df

@task
def transform_and_load_deliveries_company(data_df):
    """Функция ддя записи deliveries_company из raw в dds"""
    delivery_companies = data_df[["company"]].drop_duplicates().copy()

    delivery_companies_list = delivery_companies.to_dict(orient="records")

    valid_delivery_companies = []

    for record in delivery_companies_list:
        try:
            delivery_company = Delivery_companies(**record)
            valid_delivery_companies.append(delivery_company.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись delivery_company {record}", e))

    valid_delivery_companies_df = pd.DataFrame(valid_delivery_companies)
    valid_delivery_companies_df.columns = ["company_name"]

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not valid_delivery_companies_df.empty:
        existing_delivery_company = pd.read_sql(
            "select company_name from dds.delivery_companies", engine
        )

        existing_delivery_company_name = set(
            existing_delivery_company["company_name"]
        )

        valid_delivery_companies_df = valid_delivery_companies_df[
            ~valid_delivery_companies_df["company_name"].isin(
                existing_delivery_company_name
            )
        ]

    valid_delivery_companies_df.to_sql(
        "delivery_companies", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_delivery_companies_df.shape[0]}")

    return valid_delivery_companies_df

@task
def transform_and_load_couriers(data_df):
    """Функция ддя записи couriers из raw в dds"""
    couriers = data_df[["courier_name", "courier_phone"]].drop_duplicates().copy()

    couriers_list = couriers.to_dict(orient="records")

    valid_couriers = []

    for record in couriers_list:
        try:
            couriers = Couriers(**record)
            valid_couriers.append(couriers.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись couriers {record}", e))

    valid_couriers_df = pd.DataFrame(valid_couriers)

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not valid_couriers_df.empty:
        existing_couriers = pd.read_sql(
            "select courier_name from dds.couriers", engine
        )

        existing_couriers_name = set(existing_couriers)

        valid_couriers = valid_couriers_df[
            ~valid_couriers_df["courier_name"].isin(existing_couriers_name)
        ]

        valid_couriers_df.to_sql(
            "couriers", engine, schema="dds", if_exists="append", index=False
        )

        logger.info(f"Кол-во строк вставлено - {valid_couriers_df.shape[0]}")

        return valid_couriers_df

@task
def transform_and_load_warehouse(data_df):
    """Функция ддя записи warehouse из raw в dds"""
    warehouses = data_df[["warehouse", "address"]].drop_duplicates().copy()

    warehouses_list = warehouses.to_dict(orient="records")

    valid_warehouses = []

    for record in warehouses_list:
        try:
            warehouses = Warehouse(**record)
            valid_warehouses.append(warehouses.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись warehouses {record}", e))

    valid_warehouses_df = pd.DataFrame(valid_warehouses)
    # print(valid_warehouses_df)

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not valid_warehouses_df.empty:
        existing_warehouses = pd.read_sql(
            "select warehouse from dds.warehouses", engine
        )

        existing_warehouses = set(existing_warehouses["warehouse"])

        valid_warehouses_df = valid_warehouses_df[
            ~valid_warehouses_df["warehouse"].isin(existing_warehouses)
        ]

    valid_warehouses_df.to_sql(
        "warehouses", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_warehouses_df.shape[0]}")
    return valid_warehouses_df

@task
def transform_and_load_orders(data_df):
    """Функция ддя записи orders из raw в dds"""

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    orders = pd.read_sql(
        """SELECT
            o.order_id,
            o.quantity,
            o.price_per_unit,
            o.total_price,
            o.card_number::text,
            o.user_id,
            p.product_id
            FROM raw.orders o
            JOIN dds.products p ON o.product = p.product_name""",
        engine,
    )

    ord = orders.drop_duplicates().copy()

    orders_list = ord.to_dict(orient="records")

    valid_orders = []

    for record in orders_list:
        try:
            order = Orders(**record)
            valid_orders.append(order.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись orders {record}", e))

    valid_orders_df = pd.DataFrame(valid_orders)

    if not valid_orders_df.empty:
        existing_orders = pd.read_sql("select order_id from dds.orders", engine)

        existing_orders_ids = set(existing_orders["order_id"])

        valid_orders_df = valid_orders_df[
            ~valid_orders_df["order_id"].isin(existing_orders_ids)
        ]

    valid_orders_df.to_sql(
        "orders", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_orders_df.shape[0]}")

    return valid_orders_df

@task
def transform_and_load_deliveries(data_df):
    """Функция ддя записи deliveries из raw в dds"""

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    DATA_DIR = "/opt/airflow/dags/input/"
    os.makedirs(DATA_DIR, exist_ok=True)

    deliveries = data_df.drop_duplicates().copy()
    deliveries_df = pd.DataFrame(deliveries)

    product_data = pd.read_sql("select * from dds.products", engine)
    company_data = pd.read_sql("select * from dds.delivery_companies", engine)
    courier_data = pd.read_sql("select * from dds.couriers", engine)
    warehoues_data = pd.read_sql("select * from dds.warehouses", engine)

    deliveries_merge_products = deliveries_df.merge(
        product_data, left_on="product", right_on="product_name", how="inner"
    )
    deliveries_merge_company = deliveries_merge_products.merge(
        company_data, left_on="company", right_on="company_name", how="inner"
    )
    deliveries_merge_courier = deliveries_merge_company.merge(
        courier_data, on="courier_name", how="inner"
    )
    deliveries_merge_warehoues = deliveries_merge_courier.merge(
        warehoues_data, on="warehouse", how="inner"
    )

    total_df_deliveries = deliveries_merge_warehoues[
        [
            "delivery_id",
            "order_id",
            "product_id",
            "company_id",
            "cost",
            "courier_id",
            "start_time",
            "end_time",
            "city",
            "warehouse_id",
        ]
    ].drop_duplicates(subset="delivery_id")

    deliveries_list = total_df_deliveries.to_dict(orient="records")

    valid_deliveries = []

    for record in deliveries_list:
        try:
            deliveri = Deliveri(**record)
            valid_deliveries.append(deliveri.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись deliveries {record}", e))

    valid_deliveries_df = pd.DataFrame(valid_deliveries)

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not valid_deliveries_df.empty:
        existing_deliveries = pd.read_sql(
            "select delivery_id from dds.deliveries", engine
        )

        existing_deliveries = set(existing_deliveries["delivery_id"])

        valid_deliveries_df = valid_deliveries_df[
            ~valid_deliveries_df["delivery_id"].isin(existing_deliveries)
        ]

    valid_deliveries_df.to_sql(
        "deliveries", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_deliveries_df.shape[0]}")
    return valid_deliveries_df
