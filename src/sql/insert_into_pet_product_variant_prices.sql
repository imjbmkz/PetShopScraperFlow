INSERT INTO pet_product_variant_prices (
    product_variant_id,
    shop_id,
    price,
    discounted_price,
    discount_percentage
)
SELECT DISTINCT 
    b.id,
    b.shop_id,
    a.price,
    a.discounted_price,
    a.discount_percentage
FROM {table_name} a
LEFT JOIN pet_product_variants b 
    ON b.url = a.url 
   AND IFNULL(b.variant, '') = IFNULL(a.variant, '')
LEFT JOIN pet_product_variant_prices c 
    ON c.product_variant_id = b.id 
   AND c.shop_id = b.shop_id
WHERE c.id IS NULL;
