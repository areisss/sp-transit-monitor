-- Dashboard: Fleet Utilization
-- Compares actual fleet deployment against GTFS planned service

-- Overall fleet utilization summary
SELECT
  status,
  COUNT(*) AS route_count,
  SUM(actual_vehicles) AS total_active_vehicles,
  SUM(planned_trips) AS total_planned_trips,
  ROUND(AVG(utilization_ratio), 2) AS avg_utilization_ratio
FROM transit_monitor.gold.fleet_utilization
WHERE _event_date = date_sub(current_date(), 1)
GROUP BY status
ORDER BY
  CASE status
    WHEN 'no_service' THEN 1
    WHEN 'severely_underserved' THEN 2
    WHEN 'underserved' THEN 3
    WHEN 'normal' THEN 4
    WHEN 'over_capacity' THEN 5
  END;

-- Most under-served routes
SELECT
  line_code,
  line_number,
  route_long_name,
  actual_vehicles,
  planned_trips,
  ROUND(utilization_ratio, 2) AS utilization_ratio,
  status,
  total_observations,
  first_seen,
  last_seen
FROM transit_monitor.gold.fleet_utilization
WHERE _event_date = date_sub(current_date(), 1)
  AND status IN ('severely_underserved', 'underserved', 'no_service')
ORDER BY utilization_ratio ASC NULLS FIRST
LIMIT 30;

-- Fleet utilization trend (last 7 days)
SELECT
  _event_date,
  SUM(actual_vehicles) AS total_active_vehicles,
  SUM(planned_trips) AS total_planned_trips,
  ROUND(SUM(actual_vehicles) * 1.0 / NULLIF(SUM(planned_trips), 0), 2) AS overall_utilization,
  COUNT(CASE WHEN status = 'no_service' THEN 1 END) AS routes_no_service,
  COUNT(CASE WHEN status = 'severely_underserved' THEN 1 END) AS routes_severely_underserved
FROM transit_monitor.gold.fleet_utilization
WHERE _event_date >= date_sub(current_date(), 7)
GROUP BY _event_date
ORDER BY _event_date;

-- Routes with full or excess service
SELECT
  line_code,
  line_number,
  route_long_name,
  actual_vehicles,
  planned_trips,
  ROUND(utilization_ratio, 2) AS utilization_ratio,
  total_observations
FROM transit_monitor.gold.fleet_utilization
WHERE _event_date = date_sub(current_date(), 1)
  AND status IN ('normal', 'over_capacity')
ORDER BY utilization_ratio DESC
LIMIT 20;
