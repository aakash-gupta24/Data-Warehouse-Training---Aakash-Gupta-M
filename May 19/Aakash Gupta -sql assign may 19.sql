create database Attendence;

use Attendence;

create table EmployeeAttendance(AttendanceId int primary key identity(1,1),EmployeeName varchar(100) not null,
Department varchar(100) not null, Date  date not null,Status varchar(100) not null, HoursWorked int not null,
constraint check_status check(Status in ('Present','Absent','Late')));

insert into EmployeeAttendance values
('John Doe', 'IT', '2025-05-01', 'Present', 8),
('Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
('Ali Khan', 'IT', '2025-05-01', 'Present', 7),
('Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
('David Brown', 'Marketing', '2025-05-01', 'Present', 8);

--1
insert into EmployeeAttendance values('Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

--2
update EmployeeAttendance set Status='Present' where EmployeeName='Riya Patel';

--3
delete from EmployeeAttendance where EmployeeName='Priya Singh' and Date='2025-05-01';

--4
select * from EmployeeAttendance order by EmployeeName asc;

--5
select * from EmployeeAttendance order by HoursWorked desc;

--6
select * from EmployeeAttendance where Department='IT';

--7
select * from EmployeeAttendance where Department='IT' and Status='Present';

--8
select * from EmployeeAttendance where Status in ('Absent','Late');--NO absent employee and late so no output.

--9
select Department,sum(HoursWorked) as 'Hours Worked' from EmployeeAttendance group by Department;

--10
select Department,avg(HoursWorked) as 'Average Hours Worked' from EmployeeAttendance group by Department;

--11
select Status,count(Status) from EmployeeAttendance group by Status;

--12
select  EmployeeName from EmployeeAttendance where EmployeeName like 'R%';

--13
select EmployeeName, HoursWorked, Status from EmployeeAttendance where HoursWorked>6 and Status='Present';

--14
select * from EmployeeAttendance where HoursWorked between 6 and 8;

--15
select top 2 * from EmployeeAttendance order by HoursWorked desc;

--16
select * from EmployeeAttendance where HoursWorked<(Select avg(HoursWorked) from EmployeeAttendance);

--17
select Status,avg(HoursWorked) from EmployeeAttendance group by Status;

--18
insert into EmployeeAttendance values ('John Doe', 'IT', '2025-05-01', 'Present', 8); -- testing data

select EmployeeName,Date,count(*) as 'Count on date' from EmployeeAttendance group by Date,EmployeeName having count(*)>1;

--remove test data
delete from EmployeeAttendance where EmployeeName='John Doe';
insert into EmployeeAttendance values ('John Doe', 'IT', '2025-05-01', 'Present', 8);

--19
select top 1 Department,sum(HoursWorked) as 'Hours Worked' from EmployeeAttendance group by Department order by [Hours Worked] desc;

--20
select * from EmployeeAttendance t1 join (select Department,max(HoursWorked) as 'MaxWorked' from EmployeeAttendance group by Department) t2
on t1.Department=t2.Department and t1.HoursWorked=t2.MaxWorked;
select * from EmployeeAttendance;