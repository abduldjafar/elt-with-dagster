{{ config(materialized = 'view') }} 

with cleanned_data as (
    SELECT
                e.emp_no AS emp_no,
                d.dept_no AS dept_no,
                dp.dept_name AS dept_name,
                s.salary AS salary,
                s.from_date AS from_date,
                s.to_date AS to_date
            FROM dwh.employees_employees AS e
            INNER JOIN {{ ref('employees_cleanned_dept_emp') }} AS d ON e.emp_no = d.emp_no
            INNER JOIN dwh.employees_departments AS dp ON d.dept_no = dp.dept_no
            INNER JOIN {{ ref('employees_cleanned_salaries') }} AS s ON e.emp_no = s.emp_no
            ORDER BY
                e.emp_no ASC,
                s.from_date ASC
),

total_get_paid as (
    SELECT
        emp_no,
        min(from_date) as from_date_work,
        max(to_date) as to_date_work,
        count(from_date) as count_increase_salary,
        abs(sum(date_diff('month', from_date, to_date) * salary)) as total_salary_during_work
    FROM cleanned_data AS t
    group by emp_no
    order by emp_no
),

avg_increase_salary as (
    SELECT
    emp_no,
    avg(increased_salary) AS avg_increased_salary_every_month
FROM
(
    SELECT
        emp_no,
        from_date,
        to_date,
        salary,
        if(row_numbers = 1, 0, neighbor(salary, -1)) AS previous_salary,
        if(previous_salary = 0, 0, salary - previous_salary) AS increased_salary
    FROM
    (
        SELECT
            row_number() OVER (PARTITION BY emp_no) AS row_numbers,
            emp_no,
            from_date,
            to_date,
            salary
        FROM cleanned_data
    )
)
GROUP BY emp_no
ORDER BY emp_no ASC

),

resume_increase_salary as (
  select 
    t.*, 
    ag.avg_increased_salary_every_month as avg_increase_in_year 
  from 
    total_get_paid as t 
    left join avg_increase_salary as ag on t.emp_no = ag.emp_no
)
select 
  * 
from 
  resume_increase_salary
