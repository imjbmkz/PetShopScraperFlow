CREATE TABLE IF NOT EXISTS {table_name} (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    shop VARCHAR(50),
    url VARCHAR(255),
    scrape_status varchar(25) CHARACTER SET utf8mb4 DEFAULT 'NOT STARTED',
    updated_date datetime
);