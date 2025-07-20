INSERT INTO pet_products (
    shop_id,
    name,
    rating,
    description,
    url
)
SELECT DISTINCT
    b.id,
    a.name,
    a.rating,
    a.description,
    a.url
FROM {table_name} a
LEFT JOIN shops b ON b.name = a.shop
LEFT JOIN pet_products c ON c.url = a.url AND c.shop_id = b.id
WHERE c.id IS NULL;
