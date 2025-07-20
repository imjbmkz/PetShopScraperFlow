UPDATE {table_name} 
SET scrape_status='{status}'
    ,updated_date='{timestamp}'
WHERE id={pkey}