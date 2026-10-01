--1. Which equipment is showing the most abnormal sensor conditions and how many?
SELECT machine_id,equipment_status,COUNT(*) as abnormal_count
FROM telemetry_data
WHERE equipment_status IN ('WARNING', 'CRITICAL')
GROUP BY machine_id,equipment_status
ORDER BY abnormal_count DESC;


--2. What is the average temperature, humidity,vibration, and pressure by equipment type?
SELECT 
    machine_id, 
    ROUND(AVG(temperature_c),2) as avg_temperature_c,
    ROUND(AVG(humidity_percent),2) as avg_humidity_percent,
    ROUND(AVG(vibration_mm_s),2) as avg_vibration_mm_s,
    ROUND(AVG(pressure_bar),2) as avg_pressure_bar
FROM telemetry_data
GROUP BY machine_id
ORDER BY machine_id ASC;


--3. Which equipment has the highest temperature? 
SELECT machine_id,ROUND(MAX(temperature_c),2) as max_temperature_c
FROM telemetry_data
GROUP BY machine_id
ORDER BY max_temperature_c DESC;


--4. Which equipment has the highest humidity? 
SELECT machine_id,ROUND(MAX(humidity_percent),2) as max_humidity_percent
FROM telemetry_data
GROUP BY machine_id
ORDER BY max_humidity_percent DESC;


--5. Which equipment has the highest vibration?
SELECT machine_id, ROUND(MAX(vibration_mm_s),2) as max_vibration_mm_s
FROM telemetry_data
GROUP BY machine_id
ORDER BY max_vibration_mm_s DESC;


--6. Which equipment has the highest pressure?
SELECT machine_id, ROUND(MAX(pressure_bar),2) as max_pressure_bar
FROM telemetry_data
GROUP BY machine_id
ORDER BY max_pressure_bar DESC;


--7. Which abnormal condition rate is the highest?
WITH machine_stats AS (
    SELECT 
        t1.machine_id, 
        (
            SELECT COUNT(t2.equipment_status) 
            FROM telemetry_data t2
            WHERE t2.equipment_status IN ('CRITICAL', 'WARNING') 
                AND t2.machine_id = t1.machine_id
        ) AS warning_count, 
        COUNT(t1.equipment_status) as total_readings
    FROM telemetry_data t1
    GROUP BY t1.machine_id
)
SELECT 
    machine_id, 
    warning_count, 
    total_readings, 
    ROUND(100.00 * warning_count / NULLIF(total_readings,0),2) AS warning_rate_percent
FROM machine_stats
ORDER BY warning_rate_percent DESC;


