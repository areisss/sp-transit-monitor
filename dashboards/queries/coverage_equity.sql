-- Dashboard: Coverage Equity
-- Analyzes whether transit service is equitably distributed across income levels

-- Service equity overview by income bracket
SELECT
  income_bracket,
  COUNT(DISTINCT h3_index) AS hex_count,
  ROUND(AVG(unique_vehicles), 1) AS avg_vehicles_per_hour,
  ROUND(AVG(unique_routes), 1) AS avg_routes,
  ROUND(AVG(vehicles_per_1k_population), 2) AS avg_vehicles_per_1k_pop,
  ROUND(SUM(population), 0) AS total_population,
  service_equity_score
FROM transit_monitor.gold.coverage_equity
WHERE _event_date = current_date()
  AND hour_of_day BETWEEN 6 AND 22
GROUP BY income_bracket, service_equity_score
ORDER BY
  CASE income_bracket
    WHEN 'low' THEN 1
    WHEN 'medium' THEN 2
    WHEN 'high' THEN 3
    WHEN 'very_high' THEN 4
  END;

-- Service frequency scatter: income vs buses per capita (for scatter plot)
SELECT
  h3_index,
  avg_income_brl,
  population,
  population_density,
  unique_vehicles,
  unique_routes,
  vehicles_per_1k_population,
  income_bracket,
  service_equity_score
FROM transit_monitor.gold.coverage_equity
WHERE _event_date = current_date()
  AND time_period = 'morning_peak'
  AND population > 0;

-- Underserved areas (low income, low service)
SELECT
  h3_index,
  income_bracket,
  population,
  ROUND(avg_income_brl, 0) AS avg_income_brl,
  unique_vehicles AS vehicles_per_hour,
  unique_routes,
  ROUND(vehicles_per_1k_population, 2) AS vehicles_per_1k_pop,
  service_equity_score
FROM transit_monitor.gold.coverage_equity
WHERE _event_date = current_date()
  AND time_period = 'morning_peak'
  AND service_equity_score = 'underserved'
ORDER BY vehicles_per_1k_population ASC
LIMIT 50;

-- Service frequency by time of day, split by income bracket
SELECT
  hour_of_day,
  income_bracket,
  ROUND(AVG(unique_vehicles), 1) AS avg_vehicles,
  ROUND(AVG(vehicles_per_1k_population), 2) AS avg_vehicles_per_1k_pop
FROM transit_monitor.gold.coverage_equity
WHERE _event_date = current_date()
GROUP BY hour_of_day, income_bracket
ORDER BY hour_of_day, income_bracket;
