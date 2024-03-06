WITH 
    actress AS (
        SELECT 
            crew.title_id AS id,
            people.name AS name
        FROM crew
            INNER JOIN people ON 
                crew.category == 'actress'
                AND people.person_id == crew.person_id
                AND people.name LIKE 'Rose%'
    ),
    director AS (
        SELECT 
            crew.title_id AS id,
            people.name AS name
        FROM crew
            INNER JOIN people ON 
                crew.category == 'director'
                AND people.person_id == crew.person_id
    )

SELECT DISTINCT(director.name)
FROM director 
    INNER JOIN actress ON 
        actress.id == director.id
ORDER BY director.name;

