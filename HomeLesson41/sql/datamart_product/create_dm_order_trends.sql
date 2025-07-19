CREATE TABLE IF NOT EXISTS data_mart.dm_order_trends (
    order_week DATE,
    order_count INTEGER,
    total_items INTEGER,
    total_revenue FLOAT,
    unique_customers INTEGER
);