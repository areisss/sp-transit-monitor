-- Dashboard: On-Time Performance
-- Shows percentage of buses arriving on schedule by route and time period

-- Overall on-time performance trend (hourly)
SELECT
  window_start,
  time_period,
  SUM(total_arrivals) AS total_arrivals,
  SUM(on_time_count) AS on_time_arrivals,
  ROUND(SUM(on_time_count) * 100.0 / NULLIF(SUM(total_arrivals), 0), 1) AS on_time_pct,
  ROUND(SUM(early_count) * 100.0 / NULLIF(SUM(total_arrivals), 0), 1) AS early_pct,
  ROUND(SUM(late_count) * 100.0 / NULLIF(SUM(total_arrivals), 0), 1) AS late_pct,
  ROUND(AVG(avg_delay_seconds), 0) AS avg_delay_seconds
FROM transit_monitor.gold.on_time_performance
WHERE _event_date >= date_sub(current_date(), 7)
GROUP BY window_start, time_period
ORDER BY window_start;

-- On-time by route (worst performing)
SELECT
  line_code,
  time_period,
  SUM(total_arrivals) AS total_arrivals,
  ROUND(SUM(on_time_count) * 100.0 / NULLIF(SUM(total_arrivals), 0), 1) AS on_time_pct,
  ROUND(AVG(avg_delay_seconds), 0) AS avg_delay_seconds,
  ROUND(AVG(p95_delay_seconds), 0) AS p95_delay_seconds
FROM transit_monitor.gold.on_time_performance
WHERE _event_date = current_date()
  AND is_weekday = true
GROUP BY line_code, time_period
HAVING SUM(total_arrivals) >= 10
ORDER BY on_time_pct ASC
LIMIT 20;

-- Peak vs off-peak comparison
SELECT
  time_period,
  is_weekday,
  SUM(total_arrivals) AS total_arrivals,
  ROUND(SUM(on_time_count) * 100.0 / NULLIF(SUM(total_arrivals), 0), 1) AS on_time_pct,
  ROUND(AVG(median_delay_seconds), 0) AS median_delay_seconds
FROM transit_monitor.gold.on_time_performance
WHERE _event_date >= date_sub(current_date(), 7)
GROUP BY time_period, is_weekday
ORDER BY time_period;
