-- Dashboard: Speed & Congestion
-- Maps congestion hotspots and analyzes speed patterns

-- Current congestion map (latest hour, for map visualization)
SELECT
  h3_index,
  congestion_level,
  ROUND(avg_speed_kmh, 1) AS avg_speed_kmh,
  ROUND(median_speed_kmh, 1) AS median_speed_kmh,
  observation_count,
  line_number
FROM transit_monitor.gold.speed_congestion
WHERE _event_date = current_date()
  AND window_start >= date_add(current_timestamp(), -1)
ORDER BY avg_speed_kmh ASC;

-- Congestion distribution by time period
SELECT
  time_period,
  congestion_level,
  COUNT(*) AS segment_count,
  ROUND(AVG(avg_speed_kmh), 1) AS avg_speed_kmh,
  SUM(observation_count) AS total_observations
FROM transit_monitor.gold.speed_congestion
WHERE _event_date >= date_sub(current_date(), 7)
  AND is_weekday = true
GROUP BY time_period, congestion_level
ORDER BY time_period, congestion_level;

-- Worst congestion corridors (slowest routes)
SELECT
  line_number,
  line_code,
  time_period,
  ROUND(AVG(avg_speed_kmh), 1) AS avg_speed_kmh,
  ROUND(AVG(p10_speed_kmh), 1) AS p10_speed_kmh,
  SUM(observation_count) AS total_observations,
  MODE(congestion_level) AS predominant_congestion
FROM transit_monitor.gold.speed_congestion
WHERE _event_date >= date_sub(current_date(), 7)
  AND time_period = 'morning_peak'
  AND is_weekday = true
GROUP BY line_number, line_code, time_period
HAVING SUM(observation_count) >= 50
ORDER BY avg_speed_kmh ASC
LIMIT 20;

-- Speed trend throughout the day
SELECT
  window_start,
  ROUND(AVG(avg_speed_kmh), 1) AS avg_speed_kmh,
  ROUND(AVG(p10_speed_kmh), 1) AS p10_speed_kmh,
  ROUND(AVG(p90_speed_kmh), 1) AS p90_speed_kmh,
  SUM(observation_count) AS total_observations
FROM transit_monitor.gold.speed_congestion
WHERE _event_date = current_date()
GROUP BY window_start
ORDER BY window_start;
