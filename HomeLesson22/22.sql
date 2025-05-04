Задание для самостоятельной работы:
Описание задачи
Необходимо создать базу данных для интернет-магазина "TechGadgets" с разграничением прав доступа для разных групп сотрудников.

Требования к реализации
Структура базы данных:
- Создать БД tech_gadgets
- Создать 3 схемы: production, analytics, sandbox

Таблицы:
- В схеме production: products, customers, orders, order_items
- В схеме analytics: sales_stats, customer_segments
- В схеме sandbox: оставить пустой (для экспериментов аналитиков)

Роли и права:
- Дата-инженеры: полный доступ ко всем схемам и таблицам
- Аналитики: чтение из всех схем + полный доступ к схеме sandbox
- Менеджеры: чтение только из схемы analytics


CREATE DATABASE "TechGadgets"
WITH 
ENCODING = 'UTF8'
CONNECTION LIMIT = -1;

CREATE SCHEMA production;
CREATE SCHEMA analytics;
CREATE SCHEMA sandbox;

CREATE TABLE production.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL
);

CREATE TABLE production.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE production.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES production.customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE production.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES production.orders(order_id),
    product_id INT REFERENCES production.products(product_id),
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

-- Таблицы в схеме analytics
CREATE TABLE analytics.sales_stats (
    stat_id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,
    top_product_id INT REFERENCES production.products(product_id)
);

CREATE TABLE analytics.customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL,
    criteria TEXT NOT NULL,
    customer_count INT NOT NULL
);

-- Создание ролей
CREATE ROLE data_engineer;
CREATE ROLE analyst;
CREATE ROLE manager;

-- Назначение прав доступа для дата-инженеров
GRANT CONNECT ON DATABASE TechGadgets TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA production TO data_engineer;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO data_engineer;
GRANT ALL PRIVILEGES ON SCHEMA sandbox TO data_engineer;

-- Назначение прав доступа для аналитиков
GRANT CONNECT ON DATABASE TechGadgets TO analyst;
GRANT USAGE ON SCHEMA cTO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA production TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analyst;
GRANT ALL PRIVILEGES ON SCHEMA sandbox TO analyst;

-- Назначение прав доступа для менеджеров
GRANT CONNECT ON DATABASE TechGadgets TO manager;
GRANT USAGE ON SCHEMA analytics TO manager;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO manager;


-- Создайте представление (VIEW) в схеме analytics с продажами по категориям
CREATE VIEW analytics.category_sales_summary AS
SELECT 
	product_id,
	sum(quantity*price) AS sum_sales
FROM production.order_items
GROUP BY product_id

-- Дайте доступ менеджерам только к этому представлению, а не ко всем таблицам
	
REVOKE ALL PRIVILEGES ON TABLE analytics.sales_stats FROM manager;
REVOKE ALL PRIVILEGES ON TABLE analytics.customer_segments FROM manager;

GRANT SELECT ON analytics.category_sales_summary TO manager;


-- Создайте роль senior_analysts с правами аналитиков
-- + возможностью создавать временные таблицы

CREATE ROLE senior_analysts;

GRANT analyst TO senior_analysts;

GRANT CREATE ON SCHEMA production TO senior_analysts;
GRANT CREATE ON SCHEMA analytics TO my_user;
GRANT CREATE ON SCHEMA sandbox TO my_user;


