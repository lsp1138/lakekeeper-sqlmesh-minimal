-- Bronze: raw ingested data, loaded from CSV seed
MODEL (
    name demo.bronze_orders,
    kind SEED (
        path '../seeds/raw_orders.csv'
    ),
    columns (
        order_id INT,
        customer_id INT,
        product VARCHAR,
        quantity INT,
        unit_price DOUBLE,
        order_date VARCHAR,
        status VARCHAR
    )
);
