--schema creation default as the given data
create database Products;
create table Product(ProductID int primary key identity(1,1),ProductName varchar(100) not null,
Category varchar(100) not null, price int not null ,StockQuantity int not null default 0, Supplier varchar(100) not null);

insert into Product values('Laptop', 'Electronics', 70000, 50, 'TechMart'),
('Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
('Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
('Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
('Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

--1
--Insert a product named &quot;Gaming Keyboard&quot; under the &quot;Electronics&quot; category, priced at 3500, with 150 units in stock, supplied by &quot;TechMart&quot;.
insert into Product values('Gaming Keyboard','Electronics',3500, 150,'TechMart');

--Increase the price of all Electronics products by 10%.
update Product set price = price*1.10 where Category='Electronics';

--Remove the product with the ProductID = 4 (Desk Lamp).
delete from Product where ProductID=4;

--Display all products sorted by Price in descending order.
select * from Product order by price desc;


--2
--Display the list of products sorted by StockQuantity in ascending order.
select * from Product order by StockQuantity asc;

--List all products belonging to the Electronics category.
select * from Product where Category='Electronics';

--Retrieve all Electronics products priced above 5000.
select * from Product where Category='Electronics' and price>5000;

--List all products that are either Electronics or priced below 2000.
select * from Product where Category='Electronics' and price<2000;


--3
--Find the total stock value (Price * StockQuantity) for all products.
select ProductID,ProductName,Category,price,StockQuantity,Supplier,(price * StockQuantity) as TotalStockValue from Product;

--Calculate the average price of products grouped by Category.
select Category,AVG(price) as AveragePrice from Product group by Category;

--Count the total number of products supplied by GadgetHub.
select count(ProductID) as 'Count Of Supplier GadgetHub' from Product where Supplier='GadgetHub';



--4
--Display all products whose ProductName contains the word &quot;Wireless&quot;.
select * from Product where ProductName like '%Wireless%';

--Retrieve all products supplied by either TechMart or GadgetHub.
select * from Product where Supplier='GadgetHub' or Supplier='TechMart';

--List all products with a price between 1000 and 20000.
select * from Product where price between 1000 and 20000;

--5
--Find products where StockQuantity is greater than the average stock quantity.
select * from Product where StockQuantity>(select avg(StockQuantity) from Product);

--Display the top 3 most expensive products in the table.
select top 3 * from Product order by price desc;

--Identify any duplicate supplier names in the table.
select Supplier,count(*) as 'Supplier count' from Product group by Supplier having count(*)>1;

--Generate a summary that shows each Category with the number of products and the total stock value.
select Category,count(*) as 'No of products',SUM(Price * StockQuantity) AS TotalStockValue from Product group by Category;

--6
--Find the supplier who provides the maximum number of products.
select top 1 Supplier,count(*) as ProductCount from Product group by Supplier order by ProductCount desc;
--List the most expensive product in each category.
select * from Product where price=(select max(price) from Product);