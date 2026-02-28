###################################################################################################################################################
-- ===========================  100+ SQL interview queries from basic to advanced for Data Analyst preparation  ============================= -- 
#########################################################################################################################################################


SELECT * FROM users WHERE gender is null and contact is null;
 
 -- find customers who havent't placed any orders.
 select c.customer_id, c.name, o.order_id from customers c 
 left join Orders o 
 on c.customer_id = o.customer_id
 where o.order_id is null;
 
 -- find customers who haven't placed any orders. 
 select c.customer_id, c.name, o.order_id from customers c 
 left join  orders o
 on c.customer_id = o.customer_id
 where o.order_id is null;
  
-- retrieve the avg grade per course. 
select c.course_id, c.title, avg(e.grade) as avg_grade 
from courses c 
join enrollments e 
on c.course_id = e.course_id 
group by c.course_id, c.title; 
 
--  retrieve all employees with their department name and manager details. 
select e.*, d.*, m.* from employees e 
join department d 
on d.department_id = e.department_id 
join managers m 
 on m.manager_id = m.manager_id  ;
 
 -- find students who scored above the course average grade. 
 select s.student_id, s.name, e.course_id, e.grade, avg(e.grade) 
 from students s
 join enrollments e 
 on c.course_id = e.course_id 
 where e.grade > (select avg(e.grade) from enrollments e2 
                     where e.course_id = e.course_id ) 
order by e.course_id, s.student_id;
 
 -- find department that do not have a manager assigned. 
 select d.* from departments d 
 left join managers m 
 on m.manager_id = d.manager_id 
 where manager_name is null or manager_id is null; 
 
 -- retrieve employees who earn more than their department's average salary. 
 -- using windows func 
 SELECT
    e.emp_id,
    e.name AS employee_name,
    e.department_id,
    d.dept_name,
    e.salary,
AVG(e.salary) OVER (PARTITION BY e.department_id) AS dept_avg_salary
FROM employees e
JOIN departments d
    ON e.department_id = d.department_id
WHERE e.salary >
      AVG(e.salary) OVER (PARTITION BY e.department_id)
ORDER BY e.department_id, e.salary DESC;

-- another way

select e.emp_id, e.name as employee_name, e.department_id, 
d.dept_name, e.salary, avg(e.salary) over (partition by e.department_id) as 
dept_avg_salary from employee e 
join department d 
 on e.department_id = d.department_id 
 where e.salary > 
 avg(e.salary) over(partition by e.department_id) 
 order by e.department_id, e.salary desc;

-- another way 

SELECT *
FROM (
    SELECT
        e.emp_id,
        e.name AS employee_name,
        e.department_id,
        d.dept_name,
        e.salary,
        AVG(e.salary) OVER (PARTITION BY e.department_id) AS dept_avg_salary
    FROM employees e
    JOIN departments d
        ON e.department_id = d.department_id
) t
WHERE salary > dept_avg_salary
ORDER BY department_id, salary DESC;


-- retrieve all products that have never been sold. 
select p.* from products p 
left join order_items oi 
on oi.product_id = p.Product_ID 
where oi.product_id is null;


-- find the top 3 best selling products based on total quantity sold. 
select p.product_id, p.product_name, sum(oi.quantity) as total_quantity from product p 
join order_items oi 
on p.product_id = oi.product_id 
group by p.product_id, p.product_name  
order by total_quantity desc 
limit 3; 


-- find employees who are not assigned to any project 
select e.* from employees e 
left join employee_projects ep 
on ep.emp_id = e.emp_id
where ep.emp_id is null; 

-- get customers who haven't placed any orders in the last 6 months. 
select c.* from customers c 
left join orders o 
on c.customer_id = o.customer_id 
and o.order_date >= current_date - interval '6' month 
order by c.name; 


-- Find total revenue per region, but only for regions where revenue
-- exceeds $10, 000 
select region, sum(quantity * price) as total_revenue from sales 
group by region 
having sum(quantity * price) > 10000
order by total_revenue desc; 


-- Get the monthly revenue for the past 6 months 
select date_format(sale_date, '%Y-%m') as month,
 sum(quantity * price) as monthly_revenue 
from sales 
where sale_date >= date_sub(current_date(), interval 6 month) 
group by date_format(sale_date, '%Y-%m')
order by month desc; 

-- retrieve all employees with their department names and managers. 
select e.*, d.dept_name, m.* from employee e 
join departments d 
on d.department_id = e.department_id 
join managers m 
 on m.manager_id = d.manager_id; 
 
-- find employee who earn more than their department's avg salary. 
select e.emp_id, e.name, d.dept_name, e.salary, avg(e.salary) as 
avg_salary 
from employee e 
join departments d 
on d.department_id = e.department_id 
where e.salary > (select avg(e2.salary) from employees e2 
where e2.department_id = e.department_id);  

-- retrieve all students along with their enrolled courses. 
select s.*, c.* from students s 
join enrollments e 
on s.student_id = e.student_id 
join course c 
on c.course_id = e.course_id; 

-- get the highest and lowest grades for each course. 
select course_id, max(grade) as max_grade, 
min(grade) as min_grade from enrollments 
join courses c 
on e.course_id = c.course_id 
group by course_id, c.title; 
 
-- find the total revenue per customer, including product details. 
select c.customer_id, c.name, oi.quantity, oi.price, (oi.quantity * oi.price) as revenue 
from customers c 
join orders o on c.customer_id = o.customer_id 
join order_items oi on o.order_id = oi.order_id 
join products p on oi.product_id = p.product_id; 

-- get most profitable product category. 
select p.category, sum(oi.quantity * oi.price) as total_revenue 
from products p 
join order_items oi on p.product_id = oi.product_id 
group by p.category order by total_revenue desc 
limit 1; 

-- find customers who purchased products from at least 3 different 
-- categories. 
select c.customer_id, c.name as customer_name, count(distinct p.category) 
as category_count from customers c 
join orders o on c.customer_id = o.customer_id 
join order_items oi on c.customer_id = o.customer_id 
join products p on oi.product_id = p.product_id 
group by c.customer_id, c.name 
having count(distinct p.category) >= 3;   

--  write an sql query to find customers who placed an order with a non-null total amount. 
-- use the customers and orders tables. 
select c.name, o.orderId, o.totalamount from customers c 
join orders o 
on c.customerId = o.customerid 
where totalAmount is not null; 

-- => SELECT 1 is used with EXISTS because we only need to check whether a row exists, not fetch any data.
-- As soon as one matching row is found, EXISTS returns true.


-- write an sql query to retrieve customer names and cities for custoemrs 
-- who  have placed at least one order in the orders table. 
SELECT c.Name, c.City -- interview friendly 
FROM Customers c
WHERE EXISTS (
    SELECT 1
    FROM Orders o
    WHERE o.CustomerID = c.CustomerID
);

-- or simple 
SELECT DISTINCT c.Name, c.City
FROM Customers c
JOIN Orders o
ON c.CustomerID = o.CustomerID;

-- write an sql query to retrieve all customer and their order amount from the customers and orders table 
-- including customers who have not placed any orders  
select c.name, o.totalamount from customers c 
left join orders o 
on c.customerId = o.CustomerId; 

-- write an sql query to retrieve the customer names and their orders
-- ids for customers in new york who placed an order in 2024. 
select c.name, o.orderid from customers c 
join orders o 
on c.customerId = o.CustomerId 
where city = 'New York' and Year(OrderDate) = 2024; 

-- write an sql query to find the total number of orders by each customer, using the customers and orders tables. 
-- and make sure that customers who have placed no orders appear as well. 
Select c.name, count(o.orderId) from customers c 
left join orders o 
on c.customerId = o.CustomerId 
group by c.name; 


-- Write an sql query to retrieve the total order amount for each customer from the
-- orders table. ensure that customers who have not placed any orders are displayed. 
-- with a total order amount of 0. 
select c.customerId, c.name, c.city, coalesce(sum(o.totalAmount), 0) as total_order_amount 
from customers c 
left join orders on c.CustomerId = o.CustomerId 
group by c.CustomerId, c.Name, c.city; 


-- how do you retrieve the current date and time in sql? 
select now() as current_date_time; 

-- retrieve the transaction_date for all transactions but only show the
-- date part (withou tie) 
select date(transaction_date) from transactions;  

-- convert the transaction_date into a formatted string in the format 'YYYY-MM-DD'. 
select date_format(transaction_date, '%Y-%m-%d') as formatted_string 
from transactions; 

-- write an sql query to find all transactions that happend today. 
select * from transactions 
where date(transaction_date) = curdate(); 

-- for extracting year: year(col name) 

-- extract and list the day of the week for each transactions. 
select transaction_date, date_format(transaction_date, '%w') as 
formated_strings;  

-- write an sql query to find transactions that happend in the last 30 days. 
select * from transactions 
where transactions_date >= now() - interval 30 day; 


-- Retrieve all transactions where the transaction time is between 9 am and 5 pm 
select * from transactions 
where time(transactions_date) between '09:00:00' and '17:00:00'; 


-- convert the amount columns from a decimal number to an integer (remoing decimals) 
 select cast(amount as unsigned ) as amount_int from transactions ;
 
 
 -- convert the transaction_date col from datetime to unix timstamp format. 
select unix_timestamp(transaction_date) as unix_t from transactions; 

-- hard level 
-- find the difference in days between today's date and each transactions's transaction_date. 
select transaction_id, transaction_date, datediff(curdate(), date(transaction_date)) as 
diff_in_days from transactions; 

-- retrieve transaction and classify them into time slots: 
-- morning(5 am - 12 pm), afternoon ( 12 pm - 5 pm), evening (5 pm - 10 pm), night( 10 pm - 5 am) 
select transaction_id, transaction_date, 
case 
when time(transaction_DATE) >= '05:00:00' and 
time(transaction_date) < '12:00:00' then 'Morning' 
when time(transaction_date) >= '12:00:00' and 
time(transaction_date) < '17:00:00' then 'Afternoon' 
when time(transaction_date) >= '17:00:00' and 
time(transaction_date) < '22:00:00' then 'evening' 
else 'Night' 
end as time_slot from transactions;  


-- Retrieve employees whose experience is above the company's average and salary 
-- is below the company's average. 

select id, experience_years, Salary from employee_salary_dataset
where experience_years > ( select avg(experience_years) from employees_salary_dataset ) 
and salary < ( select avg(salary) from employee_salary_dataset); 


-- find employees with the second-lowest salary in the company. 
select id, salary from employee_salary_dataset 
where salary = ( select salary from employee_salary_dataset
order by salary asc 
limit 1, 1); 


-- find employees older than any employee who has the highest salary. 

select id, age from employee_salary_dataset 
where age > any ( 
select age from employee_salary_dataset 
where salary = (select max(salary) from employee_salary_dataset )); 


-- Find the employee(s) with the highest experience but earning less than the average
-- salary 

select id, experience_years, salary from employee_salary_dataset 
where experience_years > (select max(experience_years) from 
employee_salary_dataset) 
and Salary < ( select avg(salary) from employee_salary_dataset); 


-- Find the employee with the hightest salary among those who have experience less 
-- than the company's average experience. 

select id, experience_years, salary from employee_salary_datast 
where experience_years < ( select avg(experience_years) 
from employee_salary_datast) 
and salary = ( 
select max(salary) from employee_salary_datast 
where experience_years < ( 
select avg(experiene_years) from employee_salary_dataset) 
); 


-- find the employee with the hightest salary among those who have
-- experience less than the company's avg experience. 

select id, experience_years, salary 
from employee_salary_dataset 
where experience_years < ( 
select avg(experience_years) from employeee_salary_datast
 ) 
and salary = ( select max(salary)
from employee_salary_dataset 
where experience_years < ( select avg(experience_years) 
from employee_salary_dataset ) 
); 

-- another trick 

with avg_exp as (  
             select avg(experience_years) as avg_experience 
             from employee_salary_dataset 
             ),
filtered as ( 
select * from employee_salary_dataset e 
join avg_exp a 
on e.experience_year < a.avg_experience 
)
select id, experience_years, salary from filtered 
where salary = (select max(salary) from filtered); 

-- or, 
with avg_exp as ( 
          select avg(experience_year) as avg_experience 
          from employee_salary_dataset 
          ), 
filtered_employees as ( 
            select e.ID, e.Experience_years, e.Salary 
            from employee_salary_dataset e, avg_exp a 
            where e.experience_years < a.avg_experience 
            ) 
            
select id, experience_years, salary 
from filtered_employees 
where salary = (select max(salary) from filtered_employees); 


-- 1. Find the total revenue generated from each product, accounting for quantity 
-- and discount 

with product_sales_rev as ( 
  select od.productId,  (p.price * (1 - od.discount)) as original_price, 
  od.Quantity 
  from orderdetails od 
  join products p 
  on od.productId = p.productId 
  ) 
  select p.productName, sum(psr.original_price * psr.quantity) as totalrevenue 
  from product_sales_rev psr 
  join products p 
  on psr.productId = p.productId 
  group by p.productName; 
  
-- 2 retrieve the average order amount for each customer. 
with avg_amount_cust as ( 
select customerid, avg(totalamount) as avg_amount from orders 
group by customerid
) 
select customerId, avg_amount from avg_amount_cust; 

-- List customers who placed more than 2 orders along with their order
-- amount 
with order_count as ( 
select customerId, count(OrderId) as total_orders from orders 
group by customerid
) 
select customerId, total_orders 
from order_count 
where total_orders > 2; 


-- Hard level questions: 

--  Identify the top 3 customers who spent the most on 'electronics' products 
-- show their total spending, average spending per order, and the date of their 
-- first electronics purchase 

with elect_orders as ( 
  c.customername,
  o.customerid, 
  o.orderdate,
  (od.quantity * p.price * (1 - od.discount)) as revenue 
  from orders o 
  join orderdetails od on o.orderId = od.orderId 
  join products p on od.productId = p.productId 
  join Customer c on o.customerid = c.customerid 
  where p.category = 'Electronics'
  ) 
  select 
  customername, 
  sum(revenue) as totalspending, 
  avg(revenue) as avgspendingperorder, 
  min(orderdate) as firstpurchasedate
  from electronics_sales 
  group by customerid, customerName 
  order by totalspending desc 
  limit 3; 
  

  --  write a query to categorize campaigns as high budget if the budget is more 
  -- than 80, 000. medium budget if between 50, 000 and 80, 000 and low budget otherwise. 
  
  select campaignid, campaignname, budget,  
  case  
      when budget > 80000 then 'High Budget' 
      when Budget between 50000 and 80000 then 'Medium Budget'
      else 'low budget' 
      end as budgetcategory 
      from  campaigns; 
      
-- create a query to display each campaign's name along with a new column 
-- that says 'long duration' if the campaign lasted more than 30 days, otherwise. 
select campaignname, 
startdate, enddate, 
datediff(endDate, stardate) as durationdays, 
case 
 when datediff(endate, startdate) > 30 then 'long duration'
  end as durationcategory 
  from campaigns; 
  
  -- write a query to classify the leads generated as "high leads" if more than 200 leads were generated, 
  -- "Moderate Leads" if between 150 and 200, and "Low Leads" otherwise. 
  select leadId, campaignId, Date, 
  leadsgenerated, 
  Case  
      when leadsGenerated > 200 then "high Leads"
      when LeadesGenerated between 150 and 200 then 'Moderate Leads'
      else 'Low Leads' 
      end as leadcategory 
from Leads; 

-- Display each campaign name along with a column that labels the 
-- channel as 'digital' for 'social media', 'google ads', or 'influencer', 
-- and 'traditional' otherwise. 
select campaignname, 
channel, 
case  
    when channel in ('Social Media', 'Google Ads', 'Influencer') then 'Digital' 
    else 'Traditional'
    end as channel Category 
    from campaigns; 
    
    
-- Create a query that displays each campaign name, its budget, and a new column
-- "Roi Category" that categories campaigns as "High Roi" if the total revenue 
-- generated from leads is at least twice the budget, "Moderate Roi"
-- if revenue is between budget and twice the budget, and "low Roi" otherwise. 

select c.campaignName, 
c.budget, 
case 
	when sum(l.revenue) >= 2 * c.budget then 'High Roi'
    when sum(l.Revenue) >=  c.Budget then 'Moderate Roi'
    else 'Low Roi' 
end as roicategory 
from campaigns c 
join  leads l on c.campaignid = l.campaignid 
group by 
c.campaignid;

-- another safe method,

select c.campaignName, 
c.budget, 
case 
    when coalesce(sum(l.revenue), 0) >= 2 * c.budget then 'High Roi' 
    when coalesce(sum(l.revenue), 0) >= c.budget then 'Moderate Roi'
    else 'Low Roi' 
end as roicategory 
from campaigns c 
left join leads l 
on c.campaignid = l.campaignid 
group by c.campaignid; 

--  write a query to classify leads based on revenue as 'Preminu' 
-- if revenue is above 50, 000, 'standard' if between 30, 000 and 50, 000 and 'basic' otherwise. 
select leadId, campaignid, date, revenue, 
case  
    when revenue > 50000 then 'Premium' 
    when revenue between 30000 and 50000 then 'standard' 
    else 'Basic' 
end as revenuecategory 
from Leads; 

-- write a query to pivot the total number of leads generated for each 
-- campaign across different channels. the result should have campaign names as 
-- as rows and seperate columns for each channel. 

select c.campaignname, 
sum(case when c.channel = 'social media' then l.leadsgenerated 
else 0 end) as social_media, 
sum(case when c.channel = 'Email' then l.leadsgeneratd else 0 end) as email, 
sum(case when c.channel = 'TV Ads' then l.LeadsGenerated else 0 end) as Tv_ads, 
sum( case when c.channel = 'Google Ads' then l.LeadsGenerated else 0 end) as google_ADS, 
SUM(case when c.channel = 'Influencer' then l.Leadsgenerated else 0 end) as influencer 
from campaigns c 
join leads l on c.campaignId = l.campaignId 
group by 
c.campaignname; 


-- Create a query that pivots the revenue generated from leads for each campaign. 
-- The output should have campaign names as rows and separate columns for each month
-- month (june, august, september, october, november, dec, jan), showing the total 
-- revnue generated in that month. 

SELECT 
    c.campaignName,

    SUM(CASE WHEN MONTH(l.date) = 6  THEN l.revenue ELSE 0 END) AS June,
    SUM(CASE WHEN MONTH(l.date) = 8  THEN l.revenue ELSE 0 END) AS August,
    SUM(CASE WHEN MONTH(l.date) = 9  THEN l.revenue ELSE 0 END) AS September,
    SUM(CASE WHEN MONTH(l.date) = 10 THEN l.revenue ELSE 0 END) AS October,
    SUM(CASE WHEN MONTH(l.date) = 11 THEN l.revenue ELSE 0 END) AS November,
    SUM(CASE WHEN MONTH(l.date) = 12 THEN l.revenue ELSE 0 END) AS December,
    SUM(CASE WHEN MONTH(l.date) = 1  THEN l.revenue ELSE 0 END) AS January

FROM campaigns c
JOIN leads l 
    ON c.campaignID = l.campaignID
GROUP BY c.campaignName;

-- Write a query to display the number of leads generated per campaign, 
-- categorized by budget range. The output should have three columns: "Low Budget",
-- "medium Budget", and "High Budget", with the total leads generated under each category. 

select sum(case when c.budget < 50000 then l.LeadsGenerated else 0 end) as 
`Low Budget`, 
Sum(case when c.budget between 50000 and 80000 then l.LeadsGenerated else 0 end) as 
`Medium Budget`, 

sum(case c.budget > 80000 then l.LeadsGenerated else 0
 end) as `High Budget` 
from campaigns c 
join Leads l on c.campaignId = l.CampaignId; 

--  Write a query that displays each campaign's name, the total number of leads 
-- generated, and a new columns "Performance" that categorizes the campaign as "Excellent" 
-- if the average revenue per lead is above 300, 
-- "Good" if between 200 and 300, and "Needs improvement" otherwise. 

select c.campaignname, 
sum(l.LeadsGenerated) as TotalLeads, 
case when (sum(l.Revenue) / sum(l.LeadsGenerated)) > 300 then 'Excellent' 
when (sum(l.revenue)/ sum(l.LeadsGenerated)) between 200 and 300 then 'Good'
else 'Needs Improvement'
end as Performance 
from Campaigns c 
join leads l on c.CampaignID = l.CampaignID 
group by 
c.CampaignName; 


-- SQL RANKING 

--  write a query to display each student's Id, assessment Id, and their score. 
-- Additionally, include two columns: one showing the student's rank and another 
-- showing their dense rank within each assessment based on their scores in desc. 

select id_student, id_assessment, score, rank() over( partition by id_assessment order by score desc) as students_rank, 
dense_rank() over (partition by id_assessment order by score desc) as 
students_rank, 
dense_rank() over(partition by id_assessment order by score desc) as 
students_dense_rank
from studentsassessment; 

-- Identifying top performers per course 
-- create a query that lists each course Id, assessment Id, and the top three students 
-- based on their scores. Use the Rank() function to determine the top performers, ensuring that 
-- ties in scores are appropriately handled. 

with students_ranks as ( 
select a.code_presentation as course_id, 
sa.id_assessment, 
sa.score, 
rank() over (partition by a.code_presentation, sa.id_assessment order by 
sa.score desc) as student_rank
from studentAssessment sa 
join assessment a 
on a.id_assessment = sa.id_assessment 
) 
select course_id, 
id_assessment, 
score, 
student_rank from students_ranks 
where student_rank <= 3 
order by course_id, id_assessment, student_rank; 


-- Course Performance Analysis 
-- Generate a report that shows each curse id, the total number of students 
-- enrolled, and rank the courses based on enrollment size using both rank() and dense_rank() 

with courseenrollments as ( 
select sr.code_presentation as course_id, 
count(distinct sr.id_student) as total_students 
from studentRegistration sr 
group by 
   sr.code_presentation 
   ) 
select course_id, 
total_students, 
rank() over (order by total_students desc) as enrollment_rank, 
dense_rank() over ( order by total_students desc) as 
enrollment_dense_rank 
from courseEnrollments 
order by 
total_students desc; 

--  Write a query to complete the cumulative score for each student across all their 
-- all their assessments. Display the student's ID, assessment Id, individual 
-- assessment score, and their cumulative socre, ordered by student id and assessment date. 
with studentScores As ( 
select 
    sa.id_student, 
    sa.id_assessment, 
    sa.score, 
    sa.date_submitted 
    from studentAssessment sa 
    ) 
    select  
        id_student, 
        id_assessment, 
        score, 
        sum(score) over (partition by id_student order by date_submitted asc, 
        id_assessment asc) as cumulative_score 
        from studentScores 
        order by id_student, date_submitted asc, id_assessment asc; 
        
-- Determining avg scores with partition  
-- Create a query that calcuates the average assessment score for each course. display 
-- the course id, assessment id, student id, and the average score of the course. 
-- use the partion by clause to segment the data appropriately. 

with StudentCourseScores as ( 
select sa.id_student, 
sa.id_assessment, 
sa.score, 
a.code_presentation as course_id 
from 
   studentAssessment sa 
   join assessments a 
   on sa.id_assessment = a.id_assessment 
   ) 
select  course_id, 
id_assessment, 
id_student, 
avg(score) over (partition by course_id) as average_curse_score from 
studentCourseSocres 
order by course_id, id_assessment, id_student; 

--  Lagging Indicators of student performance 
-- Develop a query that shows each student's id, assessment id, assessment date, 
-- and their score. Include and additional column that displays the student's previous
-- assessment score using the LAG() function. this will help in analyzing changes in performance over time. 

with studentAssessments as ( 
select sa.id_student, 
sa.id_assessment, 
sa.score, 
sa.date_submitted 
from studentAssessment sa 
) 

select id_student, 
id_assessment, 
score, 
LAG(score) over (partition by id_student Order by date_submitted Asc, 
id_assessment asc) as previous_score 
from 
studentAssessments 
order by  
id_student, 
date_submitted asc, 
id_assessment asc; 

-- improving student retention rates 
-- increase student retention by identifying patterns that lead to course dropouts. 
-- student task: 
-- using he dataset, analyze the activity logs to identify students who have not accessed 
-- course matterials for an extended period. write a query to list student ids and the number 
-- days since their last activity. this information can be used to proactively reach out to 
--  at-risk students. 

with LastActivity as ( 
select id_student, 
max(date) as last_activity_date 
from studentAssessment 
group by id_student 
)  
select la.id_student, 
Datediff(current_date, la.last_activity_date) as 
days_since_last_activity 
from lastActivity la 
order by 
days_since_last_activity desc; 

-- enhancing course engagement 
-- business goal: 
-- Boost engagement in courses with low partition rates. 
-- student Task: 
-- Student Task: 
-- Identify courses with the lowest average number of student 
-- Interactions. Write a query that calculates the average number of 
-- interactions. Write a query that calculates the average number of 
-- interactions per student for each course and lists the courses with the 
-- lowest averages. provide recomendations on how to increases engagement in 
-- these courses. 

with studentCourseInteractions as ( 
select code_module || '_' || code_presentation as course_id, 
id_student, 
sum(sum_click) as total_interactions from studentVle 
group by code_module, code_presentation, id_student 
), 
CourseAverageInteractions As ( 
select course_id, 
Avg(total_interactions) as avg_interactions_per_student 
from studentCourseInteractions 
group by 
course_id 
) 
select course_id, 
avg_interactions_per_student from courseAverageInteractions,
order by avg_interactions_per_student asc; 

-- Optimizing assessment scheduling 
-- Determine the optimal timing for assessment to maximize student performance. 
-- Student Task: 
-- Analyze the dataset to find correlations between assessment 
-- submission dates and student scores. Write a query that calculates the average 
-- scores for assessments submitted on different days of the week. Based on 
-- your findings, suggest the best days to schedule assessments to enhance  
-- student performance. 

with studentAssessmentDates as ( 
select sa.id_student, 
sa.id_assessment, 
sa.score, 
sa.date_submitted 
from studentassessment sa 
), 
assessmentWithWeekday as ( 
  select 
  id_student, 
  id_assessment, 
  score, 
  date_submitted, 
  case 
     when extract(DOW from date_submitted) = 0 then 'sunday' 
     when weekday(date_submitted) = 1 then 'Monday' 
     when weekday(date_submitted) = 2 then 'Tuesday' 
     when weekday(date_submitted) = 3 then 'Wednesday' 
     when weekday(date_submitted) = 4 then 'Thursday' 
     when weekday(date_submitted) = 5 then 'Friday' 
     when weekday(date_submitted) = 6 then 'Saturday' 
     when weekday(date_submitted) = 7 then 'Sunday' 
	end as day_of_week 
    from studentAssessmentDates 
    ) 
    
select day_of_week, 
avg(score) as average_score 
from assessmentwithday 
group by day_of_week 
order by average_score_desc; 


-- what is data aggregation, and why is it important in sql? 
-- what are sql triggers, and how do they help automate tasks? 
-- explain the concept of stored procedures with a real-life ex. 
-- How can scheduled events in sql improve database management? 
-- table constraints 
-- what are table onstraints, and why are they necessary? 
-- explain the difference between primary key, unique, and foreign key constraints. 
-- what is the purpose of the check constraint, and how does it work? 

-- List all employees who never been benched and have more than 5 years of experience. 
select * from employees 
where is_benched = 0 and total_experience > 5; 

-- Find the percentage of employees who left the company, grouped by education level. 
select employee_id, education_level, 
round( 100.0 * sum(case when employment_status = 'Resigned' then 1  else 0 end) / count(*), 2) 
  as resignation_percentage 
  from employees 
  group by education_level; 
  
-- Identify the city with the highest average employee age. 
select city, avg(age) as average_age from employees 
group by city 
order by average_age desc 
limit 1; 

-- Retrieve the number of employees who joined each year, and order the result chronologically. 
select year(joining_date) as joined_year, 
count(*) as employee_count 
from employees 
group by year(joining_date) 
order by joined_year asc; 

--  list all employees who are above 30 years old and are in the highest 
-- payment tier. 
select * from employees 
where age > 30 
and payment_tier = ( 
select max(payment_tier) 
from employees 
); 

-- Find the total experience of employees grouped by gender 
select employee_id, gender, 
sum(total_experience) as total_experience_by_gender 
from employees 
group by gender; 

-- Identify employees who have over 10 years of experience but are in the 
-- lowest payment tier. 
Select * from employees 
where total_experience > 10 
and payment_tier = ( 
select min(payment_tier) 
from employees); 

-- Calculate the retention rate for employees who have been benched at least 
-- once. 
select (sum(case when employement_status = 'Active' then 1 else 0 end) * 
100.0 / count(*)) as retention_rate 
from employees 
where is_benched = 1; 











 









