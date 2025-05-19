create database Inventory;
use Inventory;

create table ProductInventory(ProductID int primary key identity(1,1), ProductName varchar(100)
not null, Category varchar(100) not null,Quantity int not null, UnitPrice int not null,
Supplier varchar(100) not null,LastRestocked date not null);


insert into ProductInventory values
('Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
('Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
('Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
('Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
('Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');

--1
insert into ProductInventory values ('Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');

--2
update ProductInventory set Quantity=Quantity+20 where ProductName='Desk Lamp';

--3
delete from ProductInventory where ProductID=2;

--4
select * from ProductInventory order by ProductName asc;

--5
select * from ProductInventory order by Quantity desc;

--6
select * from ProductInventory where Category='Electronics';

--7
select * from ProductInventory where Category='Electronics' and Quantity>20;

--8
select * from ProductInventory where Category='Electronics' or UnitPrice<2000;

--9
select *,(Quantity*UnitPrice) as 'Total Value' from ProductInventory;

--10
select Category,avg(UnitPrice) from ProductInventory group by Category;

--11
select Supplier,count(*) as 'No of products' from ProductInventory where Supplier='GadgetHub' group by Supplier;

--12
select * from ProductInventory where ProductName like 'W%';

--13
select * from ProductInventory where Supplier='GadgetHub' and UnitPrice>10000;

--14
select * from ProductInventory where UnitPrice between 1000 and 20000;

--15
select top 3 * from ProductInventory order by UnitPrice desc;

--16
select * from ProductInventory where LastRestocked between GETDATE()-10 and GETDATE();

--17
select Supplier,sum(Quantity) as 'Total no of products supplied' from ProductInventory group by Supplier;

--18
select * from ProductInventory where Quantity<30;

--19
select top 1 Supplier,sum(Quantity) as 'Total no of products supplied' from ProductInventory group by Supplier order by [Total no of products supplied] desc ;

--20
select top 1 *,(Quantity*UnitPrice) as 'Total Value' from ProductInventory order by [Total Value] desc;

--database checking commnad
select * from ProductInventory;