SELECT 
    printf('%ds', decade),
    COUNT(*)
FROM (
    SELECT 
        DISTINCT(p.person_id),
        p.born / 10 * 10 AS decade
    FROM 
        people AS p 
        INNER JOIN
        crew AS c 
        ON p.person_id == c.person_id
    WHERE c.category == 'director'
        AND p.born >= 1900
)
GROUP BY decade
ORDER BY decade;