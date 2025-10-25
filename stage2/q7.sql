SELECT e1.first_name, e2.first_name
FROM employees AS e1, employees AS e2
WHERE e1.department_id = e2.department_id
    AND e1.manager_id = e2.manager_id
    AND e1.salary >= 10000
    AND e2.salary >= 10000
    AND e1.salary >= e2.salary
    AND e1.employee_id <> e2.employee_id;
