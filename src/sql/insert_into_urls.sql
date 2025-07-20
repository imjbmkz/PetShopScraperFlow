INSERT INTO urls (
    shop
    ,url
    ,updated_date
)
SELECT DISTINCT
	a.shop
    ,a.url
    ,a.updated_date
FROM {table_name} a 
LEFT JOIN urls b ON b.url=a.url
WHERE b.id IS NULL;