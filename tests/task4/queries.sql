-- Find Departments with Average Salary Greater than a Certain Amount
SELECT d.dept_id, d.dept_name, AVG(e.salary) AS avg_salary
FROM departments d
JOIN employees e ON d.dept_id = e.department_id
GROUP BY d.dept_id, d.dept_name
HAVING AVG(e.salary) > 50000;

-- List Employees with Salaries Above the Department Average and Working on More Than One Project
SELECT e.emp_id, e.full_name, e.salary, e.department_id
FROM employees e
JOIN (
    SELECT emp_id, COUNT(*) AS project_count
    FROM employee_projects
    GROUP BY emp_id
    HAVING COUNT(*) > 1
) ep ON e.emp_id = ep.emp_id
JOIN (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
) da ON e.department_id = da.department_id
WHERE e.salary > da.avg_salary;

-- Find Employees with the Highest Salary in Each Department and List Their Projects
WITH dept_max_salary AS (
    SELECT department_id, MAX(salary) AS max_salary
    FROM employees
    GROUP BY department_id
),
highest_paid_employees AS (
    SELECT e.emp_id, e.full_name, e.salary, e.department_id
    FROM employees e
    JOIN dept_max_salary dms ON e.department_id = dms.department_id AND e.salary = dms.max_salary
)
SELECT hpe.emp_id, hpe.full_name, hpe.salary, p.project_id, p.project_name
FROM highest_paid_employees hpe
LEFT JOIN employee_projects ep ON hpe.emp_id = ep.emp_id
LEFT JOIN projects p ON ep.project_id = p.project_id;
