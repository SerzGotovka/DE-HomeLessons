from airflow.decorators import task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from faker import Faker
import os
import random
from datetime import datetime
import glob
from airflow.sensors.filesystem import FileSensor
from utils_41.models import UserBase, ProductBase, OrderBase

logger = logging.getLogger()
logger.setLevel("INFO")
fake = Faker()


@task
def upload_data_to_datamart(**kwargs):
    "Функция для загрузки в data_mart"

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql(
        """SELECT 
            DATE_TRUNC('week', date_orders) AS order_week,
            COUNT(DISTINCT id_order) AS order_count,
            SUM(quantity) AS total_items,
            SUM(quantity * p.price_per_unit) AS total_revenue,
            COUNT(DISTINCT id_user) AS unique_customers
        FROM 
            dds.orders_41 o
        JOIN 
            dds.products_41 p ON o.id_product = p.id_product
        GROUP BY 
            order_week
        ORDER BY 
            order_week;""",
        engine,
    )

    data.to_sql(
        "dm_order_trends", engine, schema="data_mart", if_exists="append", index=False
    )

    logger.info(f"кол-во строчек вставлено - {data.shape[0]}")


DATA_DIR = "/opt/airflow/dags/input_41/"
PROCESSED_DIR = "/opt/airflow/dags/processed_41/"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


products_list = [
    "бананы",
    "арбуз",
    "апельсин",
    "виноград",
    "персики",
    "дыня",
    "яблоки",
    "груши",
    "авокадо",
    "абрикос",
    "мандарин",
]

start_date = datetime.strptime("2025-11-01", "%Y-%m-%d").date()
end_date = datetime.strptime("2025-12-30", "%Y-%m-%d").date()


def generate_data(**kwargs):
    orders = []
    product_details = {}

    for _ in range(random.randint(5, 300)):
        id_user = f"U-{random.randint(1000, 9999)}"
        name = f"{fake.first_name()} {fake.last_name()}"
        email = (
            f"{fake.first_name()}.{fake.last_name().lower()}@{fake.free_email_domain()}"
        )
        phone = fake.phone_number()

        product = random.choice(products_list)  # Случайный товар
        quantity = random.randint(1, 5)  # Случайное количество

        if product not in product_details:

            price_per_unit = round(random.uniform(10, 300), 2)  # Случайная цена
            product_details[product] = price_per_unit  # Сохраняем значения в словаре
        else:
            price_per_unit = product_details[product]

        order_id = random.randint(1000, 9999)  # Уникальный ID заказа
        date_orders = fake.date_between(start_date=start_date, end_date=end_date)

        orders.append(
            {
                "id_order": order_id,
                "id_user": id_user,
                "fullname": name,
                "email": email,
                "phone": phone,
                "product": product,
                "date_orders": date_orders,
                "quantity": quantity,
                "price_per_unit": price_per_unit,
            }
        )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    pd.DataFrame(orders).to_csv(
        os.path.join(DATA_DIR, f"orders_{timestamp}.csv"), index=False
    )

    logger.info(f"создан файл orders")


class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context["ti"].xcom_push(key="file_path", value=files[0])
            return True
        return False


def load_data_orders_to_postgres(**kwargs):
    table_name = kwargs["table_name"]
    ti = kwargs["ti"]

    file_path = ti.xcom_pull(task_ids=f"wait_for_{table_name}", key="file_path")

    try:
        df = pd.read_csv(file_path)
        print(df)

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(table_name, engine, schema="raw", if_exists="append", index=False)

        logger.info(f"Файл {file_path} загружен")

        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        os.rename(file_path, processed_path)
        logger.info(f"Файл перемещён в {processed_path}")

    except:
        logger.error(f"Ошибка при загрузке {file_path}")
        raise


@task
def extract_raw_data():
    """Чтение данных из raw-слоя"""
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = pg_hook.get_conn()

    df = pd.read_sql(f"select * from raw.orders_41", conn)
    # print(df)
    return df


@task
def transform_and_load_users(data_df):
    """Функция ддя записи users из raw в dds"""

    users_df = (
        data_df[["id_user", "fullname", "email", "phone"]]
        .drop_duplicates(subset="id_user")
        .copy()
    )

    users_list = users_df.to_dict(orient="records")

    users_valid = []

    for record in users_list:
        try:
            user = UserBase(**record)
            users_valid.append(user.model_dump())
        except Exception as e:
            logger.error(f"Невалидная запись пользователя {record}", e)

    users_valid_df = pd.DataFrame(users_valid)

    # Создаем соединение
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not users_valid_df.empty:
        users_existing = pd.read_sql("select id_user from dds.users_41", engine)
        users_existing_ids = set(users_existing["id_user"])
        users_valid_df = users_valid_df[
            ~users_valid_df["id_user"].isin(users_existing_ids)
        ]

    users_valid_df.to_sql(
        "users_41", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"кол-во строчек вставлено - {users_valid_df.shape[0]}")


@task
def transform_and_load_products(data_df):
    """Функция ддя записи products из raw в dds"""
    print(data_df)

    products_df = (
        data_df[["product", "price_per_unit"]].drop_duplicates(subset="product").copy()
    )

    products_list = products_df.to_dict(orient="records")

    products_valid = []

    for record in products_list:
        try:
            product = ProductBase(**record)
            products_valid.append(product.model_dump())
        except Exception as e:
            logger.error(f"Невалидная запись пользователя {record}", e)

    print(products_valid)

    products_valid_df = pd.DataFrame(products_valid)

    # Создаем соединение
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    if not products_valid_df.empty:
        products_existing = pd.read_sql("select product from dds.products_41", engine)
        products_existing_ids = set(products_existing["product"])
        products_valid_df = products_valid_df[
            ~products_valid_df["product"].isin(products_existing_ids)
        ]

    products_valid_df.to_sql(
        "products_41", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"кол-во строчек вставлено - {products_valid_df.shape[0]}")


@task
def transform_and_load_orders(data_df):
    """Функция ддя записи orders из raw в dds"""

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    users_data = pd.read_sql("select * from dds.users_41", engine)
    products_data = pd.read_sql("select * from dds.products_41", engine)

    orders_data = data_df.drop_duplicates(subset="id_order").copy()
    orders_df = pd.DataFrame(orders_data)

    users_merge_orders = orders_df.merge(
        users_data, left_on="id_user", right_on="id_user", how="inner"
    )

    products_merge_orders = users_merge_orders.merge(
        products_data, left_on="product", right_on="product", how="inner"
    )

    total_df_orders = products_merge_orders[
        ["id_order", "id_user", "id_product", "quantity", "date_orders"]
    ]

    order_list = total_df_orders.to_dict(orient="records")

    valid_order = []

    for record in order_list:
        try:
            order = OrderBase(**record)
            valid_order.append(order.model_dump())
        except Exception as e:
            logger.error((f"Невалидная запись deliveries {record}", e))

    valid_order_df = pd.DataFrame(valid_order)

    if not valid_order_df.empty:
        existing_order = pd.read_sql("select id_order from dds.orders_41", engine)
        existing_order = set(existing_order["id_order"])
        valid_order_df = valid_order_df[
            ~valid_order_df["id_order"].isin(existing_order)
        ]

    valid_order_df.to_sql(
        "orders_41", engine, schema="dds", if_exists="append", index=False
    )

    logger.info(f"Кол-во строк вставлено - {valid_order_df.shape[0]}")
