SELECT department_name, AVG(max_salary) AS avg_max
FROM employees AS e
JOIN jobs AS j
    ON e.job_id = j.job_id
JOIN departments AS d
    ON e.department_id = d.department_id
GROUP BY department_name
HAVING avg_max > 8000;
