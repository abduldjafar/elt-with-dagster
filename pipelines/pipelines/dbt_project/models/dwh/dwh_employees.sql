{{ config(materialized = 'view') }} 

select 
  e.emp_no as emp_no, 
  de.dept_no as dept_no, 
  e.birth_date as birth_date, 
  e.first_name as first_name, 
  e.last_name as last_name, 
  t.title as title 
from 
  employees_employees as e 
  inner join {{ ref('employees_cleanned_dept_emp') }} as de on e.emp_no = de.emp_no 
  inner join {{ ref('employees_cleanned_titles') }} as t on e.emp_no = t.emp_no