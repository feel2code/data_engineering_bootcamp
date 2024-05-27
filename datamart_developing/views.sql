create or replace view analysis.Users as (select * from production.users);
create or replace view analysis.OrderItems as (select * from production.OrderItems);
create or replace view analysis.OrderStatuses as (select * from production.OrderStatuses);
create or replace view analysis.Products as (select * from production.Products);
create or replace view analysis.Orders as (select * from production.Orders);