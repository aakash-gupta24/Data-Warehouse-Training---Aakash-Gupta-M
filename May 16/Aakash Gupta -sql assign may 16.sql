--databse creation
create database Books;
use Books;

--table and sample data creation
create table Book(BookID int primary key identity(1,1),Title varchar(100) not null,
Author varchar(100) not null,Genre varchar(100) not null, Price int not null,
PublishedYear varchar(4),Stock int default 0,constraint checkYear check(PublishedYear like '[1-9][0-9][0-9][0-9]'
and cast(PublishedYear as int )<= year(getdate())));

--data insertion
insert into Book values('The Alchemist','Paulo Coelho','Fiction',300,1988,50);
insert into Book values('Sapiens','Yuval Noah Harari','Non-Fiction',500,2011,30);
insert into Book values('Atomic Habits','James Clear','Self-Help',400,2018,25);
insert into Book values('Rich Dad Poor Dad','Robert Kiyosaki','Personal Finance',350,1997,20);
insert into Book values('The Lean Startup','Eric Ries','Business',450,2011,15);


--question no and solution
--1
insert into Book values('Deep Work','Cal Newport','Self-help',420,2016,35);

--2
update Book set Price=Price+50 where Genre='Self-help';

--3
delete from Book where BookID=4;

--4
select * from Book order by Title asc;

--5
select * from Book order by Price desc;

--6
select * from Book where Genre='Fiction';

--7
select * from Book where Genre='Self-Help' and Price>400;

--8
select * from Book where Genre='Fiction' or PublishedYear>2000;

--9
select *,(Price*Stock) as 'Total Stock Value' from Book;

--10
select Genre,avg(price) as 'Average Price' from Book group by Genre;

--11
select count(*) as 'No Of Books' from Book where Author='Paulo Coelho';

--12
select * from Book where Title like '%The%';

--13
select * from Book where Author='Yuval Noah Harari' and Price<600;

--14
select * from Book where Price between 300 and 500;

--15
select top 3 * from Book order by Price desc;

--16
select * from Book where PublishedYear<2000;

--17
select Genre,(count(*)) as 'Books Count' from Book group by Genre;

--18
select Title,count(*) as 'Count' from Book group by Title having count(*)>1;

--19
--adding a book because no author has pulished multiple books in the database
insert into Book values('The Alchemist II','Paulo Coelho','Fiction',600,1999,50);

--solution check
select top 1 Author,count(*) as 'No of Books' from Book group by Author order by count(*) desc;

--20
select * from Book where PublishedYear in (select min(PublishedYear) from Book group by Genre) order by Genre,PublishedYear;

--database data check query
select * from Book;