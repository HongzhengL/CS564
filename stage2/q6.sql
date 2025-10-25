SELECT COUNT(*)
FROM employees AS e
JOIN departments AS d ON e.department_id = d.department_id
JOIN locations AS l ON d.location_id = l.location_id
WHERE l.country_id IN (
    SELECT country_id
    FROM countries AS c
    JOIN regions AS r
        ON c.region_id = r.region_id
    WHERE r.region_name = "Europe"
);
