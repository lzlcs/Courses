WITH split(word, csv) AS (
    SELECT '',
        -- here you can SELECT FROM e.g. another table: col_name||',' FROM X
        (
            SELECT group_concat(
                    REPLACE(REPLACE(crew.characters, '[', ''), ']', '')
                ) || ','
            FROM people
                INNER JOIN crew ON people.person_id = crew.person_id
            WHERE name = 'Leonardo DiCaprio'
                AND born = 1974
                AND crew.characters IS NOT NULL
        )
        -- 'recursive query'
    UNION ALL
    SELECT substr(csv, 0, instr(csv, ',')),
        -- each word contains text up to next ','
        substr(csv, instr(csv, ',') + 1) -- next recursion parses csv after this ','
    FROM split -- recurse
    WHERE csv != '' -- break recursion once no more csv words exist
),
words AS(
    SELECT DISTINCT(word)
    FROM split
    WHERE word != ''
        AND word NOT LIKE "%Self%"
    ORDER BY word
)
SELECT group_concat(REPLACE(word, '"', ''))
FROM words;