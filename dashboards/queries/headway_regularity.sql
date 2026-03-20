-- Dashboard: Headway Regularity
-- Identifies bus bunching and irregular service patterns

-- Routes with worst bunching (highest headway CV)
SELECT
  line_code,
  route_id,
  time_period,
  ROUND(AVG(avg_headway_seconds) / 60, 1) AS avg_headway_minutes,
  ROUND(AVG(headway_cv), 2) AS avg_headway_cv,
  bunching_severity,
  SUM(arrival_count) AS total_arrivals
FROM transit_monitor.gold.headway_regularity
WHERE _event_date >= date_sub(current_date(), 7)
  AND is_weekday = true
GROUP BY line_code, route_id, time_period, bunching_severity
HAVING SUM(arrival_count) >= 20
ORDER BY avg_headway_cv DESC
LIMIT 20;

-- Bunching severity distribution
SELECT
  bunching_severity,
  COUNT(DISTINCT line_code) AS route_count,
  SUM(arrival_count) AS total_arrivals,
  ROUND(AVG(avg_headway_seconds) / 60, 1) AS avg_headway_minutes
FROM transit_monitor.gold.headway_regularity
WHERE _event_date = current_date()
  AND time_period = 'morning_peak'
GROUP BY bunching_severity
ORDER BY
  CASE bunching_severity
    WHEN 'critical' THEN 1
    WHEN 'severe' THEN 2
    WHEN 'moderate' THEN 3
    WHEN 'regular' THEN 4
  END;

-- Headway trend over the day for a specific route
SELECT
  window_start,
  ROUND(avg_headway_seconds / 60, 1) AS headway_minutes,
  ROUND(headway_cv, 2) AS headway_cv,
  bunching_severity,
  arrival_count
FROM transit_monitor.gold.headway_regularity
WHERE _event_date = current_date()
  AND line_code = :line_code_param
ORDER BY window_start;
