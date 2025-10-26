
  
    
    
    
        
         


        insert into `default`.`sample_model`
        ("id", "message", "created_at")SELECT
    number AS id,
    'hello_clickhouse' AS message,
    now() AS created_at
FROM system.numbers
LIMIT 5
  