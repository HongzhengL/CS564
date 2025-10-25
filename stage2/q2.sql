SELECT d.department_name, COUNT(*) AS num_employees
FROM departments AS d
LEFT JOIN employees AS e
    ON d.department_id = e.department_id
GROUP BY d.department_name
ORDER BY num_employees DESC;
