CREATE TABLE IF NOT EXISTS airquality (
  location TEXT,
  report_time TIMESTAMP,
  pm10 FLOAT,
  pm2_5 FLOAT,
  us_aqi INT,
  european_aqi INT,
  PRIMARY KEY (location, report_time)
);