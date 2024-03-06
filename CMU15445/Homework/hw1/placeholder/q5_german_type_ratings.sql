SELECT 
    t.type,
    printf('%.2f', AVG(r.rating)) AS avg,
    MIN(r.rating),
    MAX(r.rating)
FROM 
    akas AS a
    INNER JOIN ratings AS r ON a.title_id == r.title_id
    INNER JOIN titles as t ON a.title_id == t.title_id
WHERE language IS 'de'
    AND a.types IN ('imdbDisplay', 'original')
GROUP BY t.type
ORDER BY avg;
