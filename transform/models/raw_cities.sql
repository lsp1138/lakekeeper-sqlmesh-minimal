MODEL (
    name demo.raw_cities,
    kind SEED (
        path '../seeds/raw_cities.csv'
    ),
    columns (
        name VARCHAR,
        country VARCHAR,
        population INT,
        continent VARCHAR
    )
);
