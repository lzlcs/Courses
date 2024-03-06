SELECT 
    actor.name, 
    printf('%.2f', AVG(ratings.rating)) AS avg
FROM 
    (
        SELECT 
            DISTINCT(people.person_id) AS person_id, 
            people.name AS name
        FROM people
            INNER JOIN crew ON people.person_id == crew.person_id
        WHERE crew.category == 'actor'
            AND crew.characters LIKE '%"Batman"%'
    ) AS actor
    INNER JOIN people ON crew.person_id == people.person_id
    INNER JOIN ratings ON ratings.title_id == crew.title_id
GROUP BY actor.person_id
ORDER BY avg DESC
LIMIT 10;