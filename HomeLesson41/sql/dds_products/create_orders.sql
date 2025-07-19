CREATE TABLE IF NOT EXISTS dds.orders_41(
    id_order INTEGER PRIMARY KEY,
    id_user TEXT REFERENCES dds.users_41(id_user),
    id_product INTEGER REFERENCES dds.products_41(id_product),
    quantity INTEGER,
    date_orders DATE
);