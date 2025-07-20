CREATE TABLE IF NOT EXISTS {table_name} (
    shop VARCHAR(50),
    name VARCHAR(255),
    rating VARCHAR(50),
    description TEXT,
    url VARCHAR(255),
    variant TEXT,
    image_urls TEXT,
    price NUMERIC(10, 4),
    discounted_price NUMERIC(10, 4),
    discount_percentage NUMERIC(10, 4)
);