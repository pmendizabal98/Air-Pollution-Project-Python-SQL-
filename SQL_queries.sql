-- 1. Return the date/time, station name and the highest recorded value of nitrogen oxide (NOx) found in the dataset for the year 2022.

SELECT r.date_time, s.location, MAX(r.nox)
FROM readings r
JOIN station s ON r.station_stationid = s.stationid
WHERE YEAR(r.date_time) = 2022
GROUP BY r.station_stationid;

-- 2. Return the mean values of PM2.5 (particulate matter <2.5 micron diameter) & VPM2.5 (volatile particulate matter <2.5 micron diameter)
-- by each station for the year 2019 for readings taken at 08:00 hours (peak traffic intensity).

SELECT s.location, AVG(r.`pm2.5`) AS pm25_mean, AVG(r.`vpm2.5`) AS vpm25_mean
FROM `station` s
JOIN `readings` r ON s.stationid = r.station_stationid
WHERE YEAR(r.`date_time`) = 2019 AND TIME(r.`date_time`) = '08:00:00'
GROUP BY s.stationid;

-- 3. Extend the previous query to show these values for all stations in the years 2010 to 2022.

SELECT 
    station.location AS 'station_location',
    YEAR(readings.date_time) AS 'Year',
    AVG(readings.`pm2.5`) AS 'mean_PM2.5',
    AVG(readings.`vpm2.5`) AS 'mean_VPM2.5'
FROM 
    readings
    INNER JOIN station ON readings.station_stationid = station.stationid
WHERE 
    HOUR(readings.date_time) = 8
    AND YEAR(readings.date_time) BETWEEN 2010 AND 2022
GROUP BY 
    station.stationid,
    YEAR(readings.date_time)
ORDER BY 
    station.stationid ASC,
    YEAR(readings.date_time) ASC;
