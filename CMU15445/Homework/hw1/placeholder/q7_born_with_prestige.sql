WITH filtered_people AS (
    SELECT people.person_id
    FROM people
    WHERE people.born IN 
        (    
            SELECT premiered
            FROM titles
            WHERE primary_title == 'The Prestige'
        )
)
SELECT COUNT(DISTINCT(crew.person_id))
FROM filtered_people
    INNER JOIN crew ON 
        crew.category IN ('actor', 'actress')
        AND crew.person_id == filtered_people.person_id
