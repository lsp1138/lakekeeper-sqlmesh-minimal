MODEL (
    name demo.city_stats,
    kind FULL
);

SELECT
    continent,
    COUNT(*) AS city_count,
    SUM(population) AS total_population,
    ROUND(AVG(population)) AS avg_population,
    MAX(population) AS largest_city_pop
FROM demo.raw_cities
GROUP BY continent
ORDER BY total_population DESC;
