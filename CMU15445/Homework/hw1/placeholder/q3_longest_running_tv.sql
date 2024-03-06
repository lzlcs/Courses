SELECT 
    primary_title,
    CASE
        WHEN ended IS NULL THEN (2023 - premiered)
        ELSE ended - premiered
    END AS running
FROM titles
WHERE premiered IS NOT NULL 
    AND type IS 'tvSeries'
ORDER BY 
    running DESC, 
    primary_title
limit 20;

-- SELECT DISTINCT type
-- FROM titles
