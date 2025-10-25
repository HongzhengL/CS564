SELECT COUNT(*)
FROM employees AS e
JOIN departments AS d
    ON e.department_id = d.department_id
WHERE d.department_name = "Shipping";
