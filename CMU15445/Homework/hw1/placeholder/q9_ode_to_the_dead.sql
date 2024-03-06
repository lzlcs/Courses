WITH death_rank (
    category,
    name,
    died,
    title,
    runtime_minutes,
    died_rank,
    runtime_rank
) AS (
    SELECT c.category AS category,
        p.name,
        p.died,
        t.primary_title,
        t.runtime_minutes,
        DENSE_RANK() OVER (
            PARTITION BY c.category
            ORDER BY p.died ASC,
                p.name
        ),
        DENSE_RANK() OVER (
            PARTITION BY c.category,
            p.person_id
            ORDER BY t.runtime_minutes DESC,
                t.title_id ASC
        )
    FROM people AS p
        INNER JOIN crew AS c ON p.person_id = c.person_id
        INNER JOIN titleS AS t ON 
            c.title_id = t.title_id
            AND t.runtime_minutes IS NOT NULL
    WHERE p.died IS NOT NULL
)
SELECT category,
    name,
    died,
    title,
    runtime_minutes,
    died_rank
FROM death_rank
WHERE died_rank <= 5
    AND runtime_rank = 1
ORDER BY category,
    died_rank;