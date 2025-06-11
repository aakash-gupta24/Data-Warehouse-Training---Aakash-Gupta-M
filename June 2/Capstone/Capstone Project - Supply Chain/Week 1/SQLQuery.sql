if DB_ID('SupplyChainMonitoring') is null
begin 
create database SupplyChainMonitoring;
end
else
begin
print 'Database SupplyChainMoitoring already exists';
end

use SupplyChainMonitoring;

if OBJECT_ID('dbo.suppliers','U') is null
begin
create table suppliers (
supplier_id int primary key identity(1,1),
name varchar(100),
contact_info varchar(255)
);
end
else 
begin
print 'Table suppliers already exists';
end


if OBJECT_ID('dbo.inventory ','U') is null
begin
create table inventory (item_id int primary key identity(1,1),
item_name varchar(100),
quantity int,
reorder_level int,
supplier_id int,
foreign key (supplier_id) references suppliers(supplier_id)
);
end
else 
begin
print 'Table inventory already exists';
end

if OBJECT_ID('dbo.orders','U') is null
begin
create table orders (order_id int primary key identity(1,1),
order_date date,
supplier_id int,
status varchar(50),
quantity int,
foreign key (supplier_id) references suppliers(supplier_id)
);
end
else 
begin
print 'Table orders  already exists';
end



insert into suppliers (name, contact_info) values
('supplier 1', 'contact1@supplier.com'),
('supplier 2', 'contact2@supplier.com'),
('supplier 3', 'contact3@supplier.com'),
('supplier 4', 'contact4@supplier.com'),
('supplier 5', 'contact5@supplier.com'),
('supplier 6', 'contact6@supplier.com'),
('supplier 7', 'contact7@supplier.com'),
('supplier 8', 'contact8@supplier.com'),
('supplier 9', 'contact9@supplier.com'),
('supplier 10', 'contact10@supplier.com'),
('supplier 11', 'contact11@supplier.com'),
('supplier 12', 'contact12@supplier.com'),
('supplier 13', 'contact13@supplier.com'),
('supplier 14', 'contact14@supplier.com'),
('supplier 15', 'contact15@supplier.com'),
('supplier 16', 'contact16@supplier.com'),
('supplier 17', 'contact17@supplier.com'),
('supplier 18', 'contact18@supplier.com'),
('supplier 19', 'contact19@supplier.com'),
('supplier 20', 'contact20@supplier.com'),
('supplier 21', 'contact21@supplier.com'),
('supplier 22', 'contact22@supplier.com'),
('supplier 23', 'contact23@supplier.com'),
('supplier 24', 'contact24@supplier.com'),
('supplier 25', 'contact25@supplier.com'),
('supplier 26', 'contact26@supplier.com'),
('supplier 27', 'contact27@supplier.com'),
('supplier 28', 'contact28@supplier.com'),
('supplier 29', 'contact29@supplier.com'),
('supplier 30', 'contact30@supplier.com');


insert into inventory (item_name, quantity, reorder_level, supplier_id) values
('wireless mouse', 100, 20, 1),
('usb-c cable', 150, 30, 2),
('laptop stand', 200, 50, 3),
('keyboard', 120, 25, 4),
('external hard drive', 300, 75, 5),
('monitor 24 inch', 400, 100, 6),
('webcam hd', 80, 20, 7),
('bluetooth speaker', 90, 15, 8),
('gaming headset', 250, 60, 9),
('smartphone case', 180, 40, 10),
('portable charger', 210, 50, 11),
('desk lamp', 170, 45, 12),
('office chair', 300, 80, 13),
('graphic tablet', 260, 55, 14),
('router', 130, 30, 15),
('ssd 1tb', 160, 40, 16),
('mouse pad', 110, 25, 17),
('usb flash drive', 190, 45, 18),
('hdmi cable', 140, 35, 19),
('power strip', 220, 60, 20),
('webcam cover', 200, 50, 21),
('wireless keyboard', 100, 20, 22),
('desk organizer', 300, 70, 23),
('office desk', 250, 55, 24),
('laptop backpack', 275, 60, 25),
('monitor stand', 225, 45, 26),
('phone charger', 150, 30, 27),
('earbuds', 180, 40, 28),
('tablet case', 120, 25, 29),
('usb hub', 160, 35, 30),
('cable clips', 130, 30, 1),
('laptop cooling pad', 140, 35, 2),
('smartwatch', 190, 50, 3),
('desk calendar', 200, 60, 4),
('printer ink', 170, 40, 5),
('phone stand', 180, 45, 6),
('flashlight', 150, 30, 7),
('hdmi splitter', 160, 35, 8),
('tablet stylus', 190, 50, 9),
('usb charger', 210, 60, 10);


insert into orders (order_date, supplier_id, status, quantity) values
('2025-07-23', 23, 'cancelled', 7),
('2025-06-07', 7, 'delivered', 25),
('2025-06-28', 28, 'cancelled', 8),
('2025-07-12', 12, 'pending', 12),
('2025-07-05', 5, 'pending', 14),
('2025-06-01', 1, 'pending', 10),
('2025-07-19', 19, 'shipped', 15),
('2025-06-25', 25, 'delivered', 25),
('2025-07-02', 2, 'shipped', 21),
('2025-07-24', 24, 'shipped', 19),
('2025-07-31', 1, 'pending', 12),
('2025-07-28', 28, 'cancelled', 6),
('2025-06-16', 16, 'delivered', 19),
('2025-06-04', 4, 'pending', 12),
('2025-07-07', 7, 'delivered', 22),
('2025-07-16', 16, 'pending', 13),
('2025-06-21', 21, 'delivered', 18),
('2025-08-05', 6, 'delivered', 22),
('2025-07-13', 13, 'cancelled', 6),
('2025-07-10', 10, 'shipped', 20),
('2025-07-30', 30, 'delivered', 21),
('2025-06-03', 3, 'shipped', 15),
('2025-06-22', 22, 'cancelled', 6),
('2025-06-15', 15, 'pending', 13),
('2025-06-13', 13, 'pending', 16),
('2025-08-01', 2, 'pending', 18),
('2025-06-17', 17, 'shipped', 11),
('2025-06-29', 29, 'delivered', 23),
('2025-07-21', 21, 'delivered', 22),
('2025-08-02', 3, 'cancelled', 5),
('2025-07-26', 26, 'pending', 11),
('2025-06-02', 2, 'delivered', 20),
('2025-07-27', 27, 'shipped', 16),
('2025-06-20', 20, 'pending', 10),
('2025-06-23', 23, 'shipped', 17),
('2025-08-03', 4, 'delivered', 19),
('2025-07-22', 22, 'pending', 14),
('2025-06-18', 18, 'pending', 22),
('2025-08-07', 8, 'pending', 12),
('2025-07-15', 15, 'shipped', 17),
('2025-07-11', 11, 'delivered', 18),
('2025-06-11', 11, 'shipped', 14),
('2025-08-04', 5, 'pending', 13),
('2025-06-26', 26, 'pending', 14),
('2025-07-09', 9, 'pending', 11),
('2025-07-01', 1, 'pending', 13),
('2025-07-18', 18, 'cancelled', 5),
('2025-06-12', 12, 'cancelled', 7),
('2025-06-10', 10, 'delivered', 30),
('2025-06-08', 8, 'shipped', 17),
('2025-07-06', 6, 'shipped', 15),
('2025-06-19', 19, 'delivered', 20),
('2025-06-30', 30, 'pending', 9),
('2025-07-29', 29, 'pending', 13),
('2025-06-09', 9, 'pending', 9),
('2025-06-24', 24, 'pending', 12),
('2025-07-14', 14, 'delivered', 24),
('2025-06-27', 27, 'shipped', 16),
('2025-07-17', 17, 'delivered', 20),
('2025-07-25', 25, 'delivered', 23),
('2025-07-08', 8, 'cancelled', 9),
('2025-08-08', 9, 'cancelled', 7),
('2025-06-14', 14, 'delivered', 21),
('2025-07-20', 20, 'pending', 12),
('2025-06-06', 6, 'cancelled', 5),
('2025-08-06', 7, 'shipped', 15),
('2025-08-09', 10, 'shipped', 18),
('2025-07-04', 4, 'cancelled', 7);



-- 2. crud examples
insert into suppliers (name, contact_info) values ('abc corp', 'abc@example.com');
insert into inventory (item_name, quantity, reorder_level, supplier_id) values ('widget a', 5, 10, 1);
insert into orders (order_date, supplier_id, status,quantity) values (getdate(), 2, 'pending',1);

create procedure auto_reorder
as
begin
set nocount on;

declare @item_id int;
declare @supplier_id int;

declare reorder_cursor cursor for
select item_id, supplier_id
from inventory
where quantity < reorder_level;

open reorder_cursor;

fetch next from reorder_cursor into @item_id, @supplier_id;

while @@fetch_status = 0
begin
insert into orders (order_date, supplier_id, status)
values (getdate(), @supplier_id, 'auto-reorder');

fetch next from reorder_cursor into @item_id, @supplier_id;
end;

close reorder_cursor;
deallocate reorder_cursor;
end;

--execute stored procedure auto_reader
exec auto_reorder;
