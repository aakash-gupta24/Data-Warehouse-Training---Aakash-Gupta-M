if DB_ID('customerOrders') is null
begin 
create database customerOrders;
end
else
begin
print 'Database customerOrder already exists';
end

use customerorders;

if OBJECT_ID('dbo.customers','U') is null
begin
create table customers(customer_id int primary key identity(1,1),
name varchar(100) not null,
email varchar(100) unique not null,
phone varchar(15),
region varchar(50),
created_at datetime default getdate());
end
else 
begin
print 'Table customers already exists';
end


if OBJECT_ID('dbo.orders','U') is null
begin
create table orders(
order_id int primary key identity(1,1),
customer_id int not null,
product_name varchar(100) not null,
order_date date not null,
delivery_date date,
amount decimal(10,2),
foreign key (customer_id) references customers(customer_id)
);
end
else 
begin
print 'Table orders already exists';
end

if OBJECT_ID('dbo.delivery_status','U') is null
begin
create table delivery_status(status_id int primary key identity(1,1),
order_id int not null,
current_status varchar(50) check (current_status in ('Delivered','In Transit', 'Delayed', 'Cancelled')),
status_date datetime default getdate(),
foreign key (order_id) references orders(order_id));
end
else 
begin
print 'Table delivery_status  already exists';
end

insert into customers (name, email, phone, region) values
('Alice Johnson', 'alice.johnson@example.com', '555-0101', 'North'),
('Bob Smith', 'bob.smith@example.com', '555-0102', 'South'),
('Carol Davis', 'carol.davis@example.com', '555-0103', 'East'),
('David Wilson', 'david.wilson@example.com', '555-0104', 'West'),
('Eva Brown', 'eva.brown@example.com', '555-0105', 'Central'),
('Frank Moore', 'frank.moore@example.com', '555-0106', 'North'),
('Grace Lee', 'grace.lee@example.com', '555-0107', 'South'),
('Henry Walker', 'henry.walker@example.com', '555-0108', 'East'),
('Isla Harris', 'isla.harris@example.com', '555-0109', 'West'),
('Jack Young', 'jack.young@example.com', '555-0110', 'Central'),
('Karen King', 'karen.king@example.com', '555-0111', 'North'),
('Liam Scott', 'liam.scott@example.com', '555-0112', 'South'),
('Mia Adams', 'mia.adams@example.com', '555-0113', 'East'),
('Noah Baker', 'noah.baker@example.com', '555-0114', 'West'),
('Olivia Clark', 'olivia.clark@example.com', '555-0115', 'Central'),
('Paul Carter', 'paul.carter@example.com', '555-0116', 'North'),
('Quinn Mitchell', 'quinn.mitchell@example.com', '555-0117', 'South'),
('Rachel Perez', 'rachel.perez@example.com', '555-0118', 'East'),
('Sam Evans', 'sam.evans@example.com', '555-0119', 'West'),
('Tina Edwards', 'tina.edwards@example.com', '555-0120', 'Central'),
('Uma Turner', 'uma.turner@example.com', '555-0121', 'North'),
('Victor Collins', 'victor.collins@example.com', '555-0122', 'South'),
('Wendy Stewart', 'wendy.stewart@example.com', '555-0123', 'East'),
('Xander Morris', 'xander.morris@example.com', '555-0124', 'West'),
('Yara Rogers', 'yara.rogers@example.com', '555-0125', 'Central'),
('Zachary Reed', 'zachary.reed@example.com', '555-0126', 'North'),
('Amy Cook', 'amy.cook@example.com', '555-0127', 'South'),
('Brian Morgan', 'brian.morgan@example.com', '555-0128', 'East'),
('Cindy Bell', 'cindy.bell@example.com', '555-0129', 'West'),
('Derek Murphy', 'derek.murphy@example.com', '555-0130', 'Central'),
('Ella Bailey', 'ella.bailey@example.com', '555-0131', 'North'),
('Fred Rivera', 'fred.rivera@example.com', '555-0132', 'South'),
('Gina Cooper', 'gina.cooper@example.com', '555-0133', 'East'),
('Harry Richardson', 'harry.richardson@example.com', '555-0134', 'West'),
('Ivy Cox', 'ivy.cox@example.com', '555-0135', 'Central'),
('Jason Howard', 'jason.howard@example.com', '555-0136', 'North'),
('Kelly Ward', 'kelly.ward@example.com', '555-0137', 'South'),
('Luke Torres', 'luke.torres@example.com', '555-0138', 'East'),
('Maya Peterson', 'maya.peterson@example.com', '555-0139', 'West'),
('Nate Gray', 'nate.gray@example.com', '555-0140', 'Central'),
('Olga Ramirez', 'olga.ramirez@example.com', '555-0141', 'North'),
('Pete James', 'pete.james@example.com', '555-0142', 'South'),
('Queen Simmons', 'queen.simmons@example.com', '555-0143', 'East'),
('Raymond Foster', 'raymond.foster@example.com', '555-0144', 'West'),
('Sara Bryant', 'sara.bryant@example.com', '555-0145', 'Central'),
('Tommy Alexander', 'tommy.alexander@example.com', '555-0146', 'North'),
('Ursula Russell', 'ursula.russell@example.com', '555-0147', 'South'),
('Vince Griffin', 'vince.griffin@example.com', '555-0148', 'East'),
('Willa Diaz', 'willa.diaz@example.com', '555-0149', 'West'),
('Ximena Hayes', 'ximena.hayes@example.com', '555-0150', 'Central'),
('Yosef Myers', 'yosef.myers@example.com', '555-0151', 'North'),
('Zoe Ford', 'zoe.ford@example.com', '555-0152', 'South'),
('Adam Hamilton', 'adam.hamilton@example.com', '555-0153', 'East'),
('Bella Graham', 'bella.graham@example.com', '555-0154', 'West'),
('Caleb Sullivan', 'caleb.sullivan@example.com', '555-0155', 'Central'),
('Diana Wallace', 'diana.wallace@example.com', '555-0156', 'North'),
('Ethan West', 'ethan.west@example.com', '555-0157', 'South'),
('Fiona Cole', 'fiona.cole@example.com', '555-0158', 'East'),
('Gavin Hunt', 'gavin.hunt@example.com', '555-0159', 'West'),
('Holly Fisher', 'holly.fisher@example.com', '555-0160', 'Central'),
('Ian Warren', 'ian.warren@example.com', '555-0161', 'North'),
('Jade Reynolds', 'jade.reynolds@example.com', '555-0162', 'South'),
('Kyle Stevens', 'kyle.stevens@example.com', '555-0163', 'East'),
('Lara Porter', 'lara.porter@example.com', '555-0164', 'West'),
('Mark Coleman', 'mark.coleman@example.com', '555-0165', 'Central'),
('Nina Barnes', 'nina.barnes@example.com', '555-0166', 'North'),
('Owen Perry', 'owen.perry@example.com', '555-0167', 'South'),
('Paula Russell', 'paula.russell@example.com', '555-0168', 'East'),
('Quentin Boyd', 'quentin.boyd@example.com', '555-0169', 'West'),
('Rita Warren', 'rita.warren@example.com', '555-0170', 'Central');

INSERT INTO orders (customer_id, order_date, delivery_date, amount, product_name) VALUES
(1, '2025-05-01', '2025-05-06', 250.75, 'Wireless Mouse'),
(3, '2025-05-02', '2025-05-07', 89.99, 'Bluetooth Headphones'),
(5, '2025-05-03', '2025-05-08', 120.50, 'External Hard Drive'),
(7, '2025-05-04', '2025-05-10', 450.00, 'Smartphone'),
(10, '2025-05-05', '2025-05-12', 300.10, 'Gaming Keyboard'),
(3, '2025-05-06', '2025-05-11', 150.00, 'Portable Charger'),
(20, '2025-05-07', '2025-05-14', 499.99, '4K Monitor'),
(15, '2025-05-08', '2025-05-15', 99.95, 'Wireless Earbuds'),
(15, '2025-05-09', '2025-05-16', 75.45, 'USB-C Hub'),
(6, '2025-05-10', '2025-05-17', 199.00, 'Smartwatch'),
(21, '2025-05-11', '2025-05-18', 180.00, 'Laptop Stand'),
(22, '2025-05-12', '2025-05-19', 210.30, 'Desk Lamp'),
(23, '2025-05-13', '2025-05-20', 300.00, 'Wireless Router'),
(25, '2025-05-14', '2025-05-21', 120.00, 'Mechanical Keyboard'),
(1, '2025-05-15', '2025-05-22', 99.00, 'Gaming Mouse Pad'),
(30, '2025-05-16', '2025-05-23', 85.75, 'HD Webcam'),
(18, '2025-05-17', '2025-05-24', 65.50, 'Bluetooth Speaker'),
(33, '2025-05-18', '2025-05-25', 130.20, 'Graphic Tablet'),
(31, '2025-05-19', '2025-05-26', 75.00, 'Wireless Charger'),
(20, '2025-05-20', '2025-05-27', 250.00, 'External SSD'),
(14, '2025-05-21', '2025-05-28', 320.00, 'Noise Cancelling Headphones'),
(7, '2025-05-22', '2025-05-29', 175.50, 'Action Camera'),
(6, '2025-05-23', '2025-05-30', 80.25, 'Fitness Tracker'),
(2, '2025-05-24', '2025-05-31', 95.00, 'Smart Light Bulb'),
(27, '2025-05-25', '2025-06-01', 225.75, 'Wireless Keyboard'),
(26, '2025-05-26', '2025-06-02', 140.40, 'Bluetooth Mouse'),
(29, '2025-05-27', '2025-06-03', 110.00, 'USB Flash Drive'),
(34, '2025-05-28', '2025-06-04', 185.60, 'Portable Speaker'),
(12, '2025-05-29', '2025-06-05', 210.00, 'Smart Thermostat'),
(28, '2025-05-30', '2025-06-06', 190.75, 'VR Headset'),
(11, '2025-05-31', '2025-06-07', 300.00, 'Laptop Backpack'),
(17, '2025-06-01', '2025-06-08', 160.25, 'Wireless Headset'),
(9, '2025-06-02', '2025-06-09', 135.00, 'Tablet Stand'),
(16, '2025-06-03', '2025-06-10', 195.50, 'Smart Home Hub'),
(35, '2025-06-04', '2025-06-11', 210.75, 'Noise Cancelling Earbuds'),
(21, '2025-06-05', '2025-06-12', 175.00, 'Ergonomic Mouse'),
(20, '2025-06-06', '2025-06-13', 225.80, 'Mechanical Gaming Keyboard'),
(8, '2025-06-07', '2025-06-14', 280.00, '4K Action Camera'),
(4, '2025-06-08', '2025-06-15', 305.45, 'Smartphone Gimbal'),
(2, '2025-06-09', '2025-06-16', 330.00, 'Gaming Chair'),
(3, '2025-06-10', '2025-06-17', 150.00, 'Wireless Charging Pad'),
(1, '2025-06-11', '2025-06-18', 190.20, 'Bluetooth Speaker'),
(5, '2025-06-12', '2025-06-19', 170.00, 'Smartwatch'),
(10, '2025-06-13', '2025-06-20', 220.35, 'Laptop Cooling Pad'),
(19, '2025-06-14', '2025-06-21', 245.00, 'Portable Projector'),
(8, '2025-06-15', '2025-06-22', 275.75, 'Wireless Earbuds'),
(12, '2025-06-16', '2025-06-23', 225.50, 'Smart Doorbell'),
(14, '2025-06-17', '2025-06-24', 195.00, 'Fitness Smartwatch'),
(20, '2025-06-18', '2025-06-25', 210.00, '4K Monitor'),
(13, '2025-06-19', '2025-06-26', 315.40, 'Gaming Laptop');


insert into delivery_status (order_id, current_status, status_date) values
(1, 'Delivered', '2025-05-06'),
(2, 'In Transit', '2025-05-07'),
(3, 'Delayed', '2025-05-08'),
(4, 'Cancelled', '2025-05-09'),
(5, 'Delivered', '2025-05-12'),
(6, 'In Transit', '2025-05-11'),
(7, 'Delivered', '2025-05-14'),
(8, 'Delivered', '2025-05-15'),
(9, 'In Transit', '2025-05-16'),
(10, 'Delivered', '2025-05-17'),
(11, 'Delivered', '2025-05-18'),
(12, 'Delayed', '2025-05-19'),
(13, 'In Transit', '2025-05-20'),
(14, 'Delivered', '2025-05-21'),
(15, 'Delivered', '2025-05-22'),
(16, 'Cancelled', '2025-05-23'),
(17, 'Delivered', '2025-05-24'),
(18, 'In Transit', '2025-05-25'),
(19, 'Delivered', '2025-05-26'),
(20, 'Delivered', '2025-05-27'),
(21, 'In Transit', '2025-05-28'),
(22, 'Delivered', '2025-05-29'),
(23, 'Delayed', '2025-05-30'),
(24, 'Delivered', '2025-05-31'),
(25, 'Delivered', '2025-06-01'),
(26, 'Cancelled', '2025-06-02'),
(27, 'Delivered', '2025-06-03'),
(28, 'In Transit', '2025-06-04'),
(29, 'Delivered', '2025-06-05'),
(30, 'Delivered', '2025-06-06'),
(31, 'Delayed', '2025-06-07'),
(32, 'Delivered', '2025-06-08'),
(33, 'Delivered', '2025-06-09'),
(34, 'In Transit', '2025-06-10'),
(35, 'Delivered', '2025-06-11'),
(36, 'Cancelled', '2025-06-12'),
(37, 'Delivered', '2025-06-13'),
(38, 'In Transit', '2025-06-14'),
(39, 'Delivered', '2025-06-15'),
(40, 'Delivered', '2025-06-16'),
(41, 'In Transit', '2025-06-17'),
(42, 'Delivered', '2025-06-18'),
(43, 'Delivered', '2025-06-19'),
(44, 'Delayed', '2025-06-20'),
(45, 'Delivered', '2025-06-21'),
(46, 'In Transit', '2025-06-22'),
(47, 'Delivered', '2025-06-23'),
(48, 'Cancelled', '2025-06-24'),
(49, 'Delivered', '2025-06-25'),
(50, 'In Transit', '2025-06-26');

-- insert: new customer
insert into customers (name, email, phone, region)
values ('alice smith', 'alice@example.com', '1234567890', 'north');

-- insert: new order for new customer (assuming new customer_id is 71)
insert into orders (customer_id, order_date, delivery_date, amount, product_name)
values (71, '2025-06-01', '2025-06-05', 150.00, 'wireless mouse');

-- insert: delivery status for new order (assuming new order_id is 51)
insert into delivery_status (order_id, current_status)
values (51, 'delivered');

-- update: update delivery date for order_id = 1
update orders
set delivery_date = '2025-05-08'
where order_id = 1;

-- delete: remove delivery_status and order with order_id = 2 (if exists)
delete from delivery_status where order_id = 2;
delete from orders where order_id = 2;

-- select: get all orders with their delivery status and customer name
select c.name, o.order_id, o.order_date, o.delivery_date, o.product_name, ds.current_status
from customers c
join orders o on c.customer_id = o.customer_id
join delivery_status ds on o.order_id = ds.order_id;


--stored procedure to fetch delayed deliveried
-- drop if it exists
if object_id('getdelayeddeliveriesbycustomer', 'p') is not null
    drop procedure getdelayeddeliveriesbycustomer;

go

create procedure getdelayeddeliveriesbycustomer
    @customerid int
as
begin
    set nocount on;

    select o.order_id, o.order_date, o.delivery_date, ds.current_status
    from orders o
    inner join delivery_status ds on o.order_id = ds.order_id
    where o.customer_id = @customerid and ds.current_status = 'Delayed';
end;
go

-- example call
exec getdelayeddeliveriesbycustomer @customerid = 5;


