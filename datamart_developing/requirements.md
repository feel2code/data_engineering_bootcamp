Таблицы необходимые для разработки витрины в базе `production`:

orders - информация о заказах
order_statuses - словарь статусов заказов
users - для получения всех идентификаторов пользователей

Успешно выполненный заказ имеет статус `Closed`, таким образом можно определить завершенные заказы по ключу:

`select * from production.orders o
left join production.orderstatuses o2 on o2.id=o.status
where o2.key = 'Closed';`