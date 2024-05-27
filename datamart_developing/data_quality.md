## Анализ качества данных

```SQL
select * from information_schema.table_constraints tc where constraint_schema='production';
select * from information_schema.constraint_column_usage where table_name='orders';
select * from information_schema.columns where table_name='orders';
select * from information_schema.columns where table_name='orderstatuses';
select order_id, count(*) from orders group by order_id HAVING COUNT(*) > 1;
```

*null данные не найдены, дубликатов нет*