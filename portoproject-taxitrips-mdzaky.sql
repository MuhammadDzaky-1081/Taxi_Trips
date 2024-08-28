SELECT * FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` LIMIT 10000

-- 1. Count average, median, and standard deviation of trip duration for trips made on Monday and Saturday, comparing the results of the two weekdays.
-- Create a Common Table Expression (CTE) to extract the weekday and trip duration (in seconds)
WITH trip_stats AS (
    SELECT
        EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS weekdays, -- Extract the weekday from the trip start timestamp
        trip_seconds -- Duration of the trip in seconds
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    WHERE trip_seconds IS NOT NULL -- Ensure trip_seconds is not null
)

-- Select the average, median, and standard deviation of trip durations for Monday and Saturday
SELECT
    CASE
        WHEN weekdays = 2 THEN 'Monday' -- Convert weekdays = 2 to Monday
        WHEN weekdays = 7 THEN 'Saturday' -- Convert weekdays = 7 to Saturday
    END AS weekday, -- Use 'weekday' instead of 'day_name' for readability
    AVG(trip_seconds) AS avg_seconds, -- Calculate the average trip duration
    APPROX_QUANTILES(trip_seconds, 2)[OFFSET(1)] AS median_seconds, -- Approximate the median of trip duration
    STDDEV(trip_seconds) AS stddev_seconds -- Calculate the standard deviation of trip duration
FROM trip_stats
WHERE weekdays IN (2, 7) -- Filter the days to only include Monday and Saturday
GROUP BY weekday -- Group by the weekday to calculate statistics separately for Monday and Saturday
ORDER BY weekday; -- Order the results by the weekday

-- 2. Discover the total number of trips for the 5 routes with the highest number of trips in 2023.
-- Select the pickup and dropoff community areas and count the number of trips between them.
SELECT
    pickup_community_area, -- The community area where the trip started.
    dropoff_community_area, -- The community area where the trip ended.
    COUNT(*) AS num_trips -- Count the total number of trips for each unique route (combination of pickup and dropoff community areas).
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2023 -- Filter the trips to include only those that occurred in the year 2023.
  AND pickup_community_area IS NOT NULL -- Ensure the pickup community area is not null.
  AND dropoff_community_area IS NOT NULL -- Ensure the dropoff community area is not null.
GROUP BY pickup_community_area, dropoff_community_area -- Group the trips by the combination of pickup and dropoff community areas to identify unique routes.
ORDER BY num_trips DESC -- Order the results by the total number of trips in descending order to show the most frequent routes first.
LIMIT 5; -- Limit the results to the top 5 routes with the highest number of trips.

-- Select the total number of trips for the top 5 routes.
WITH top_routes AS (
    SELECT
        pickup_community_area, -- The community area where the trip started.
        dropoff_community_area, -- The community area where the trip ended.
        COUNT(*) AS num_trips -- Count the total number of trips for each unique route.
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2023 -- Filter the trips to include only those that occurred in the year 2023.
      AND pickup_community_area IS NOT NULL -- Ensure the pickup community area is not null.
      AND dropoff_community_area IS NOT NULL -- Ensure the dropoff community area is not null.
    GROUP BY pickup_community_area, dropoff_community_area -- Group the trips by the combination of pickup and dropoff community areas.
    ORDER BY num_trips DESC -- Order the results by the total number of trips in descending order.
    LIMIT 5 -- Limit the results to the top 5 routes with the highest number of trips.
)
SELECT
    SUM(num_trips) AS total_num_trips -- Calculate the total number of trips for the top 5 routes.
FROM top_routes;

---3. compare the average cost of taxi trips, based on payment method in 2019.
-- Compare the average cost of taxi trips by payment method in 2019
SELECT
    payment_type, -- The payment method used for the trip
    AVG(fare) AS average_fare, -- Calculate the average fare
    AVG(tips) AS average_tips, -- Calculate the average tips
    AVG(tolls) AS average_tolls -- Calculate the average tolls
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2019 -- Filter for trips in the year 2019
GROUP BY payment_type -- Group the results by payment method
ORDER BY average_fare DESC; -- Order the results by average fare in descending order

---4. use several machine learning methods to compare the results of taxi trips cost, based on the nearest location for the last 3 years.
-- Logistic regression to classify trips into long or short based on median trip duration
CREATE OR REPLACE MODEL `portoproject-taxitrips-mdzaky.chicago_taxi_trips.taxi_trips_trip_duration_classification`
OPTIONS(
  model_type='logistic_reg',  -- Specifies logistic regression as the model type
  input_label_cols=['is_long_trip']  -- The target column to classify
) AS
WITH data AS (
  SELECT
    trip_seconds,
    -- Calculate the trip distance using geographical coordinates
    ST_DISTANCE(
      ST_GEOGPOINT(pickup_longitude, pickup_latitude), 
      ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)
    ) AS trip_distance,
    EXTRACT(HOUR FROM trip_start_timestamp) AS pickup_hour,  -- Extract the hour from the trip start timestamp
    payment_type,  -- Payment method used for the trip
    -- Classify the trip as long or short based on the median trip duration
    IF(trip_seconds > (
      SELECT APPROX_QUANTILES(trip_seconds, 2)[OFFSET(1)]  -- Approximate median trip duration
      FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
      WHERE trip_seconds IS NOT NULL
    ), 1, 0) AS is_long_trip  -- 1 if long trip, 0 otherwise
  FROM
    `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE
    -- Ensure that all necessary fields are non-NULL and have valid data
    trip_seconds IS NOT NULL
    AND trip_seconds > 0
    AND pickup_latitude IS NOT NULL
    AND pickup_longitude IS NOT NULL
    AND dropoff_latitude IS NOT NULL
    AND dropoff_longitude IS NOT NULL
  LIMIT 10000 -- Limit the number of rows to avoid exceeding quota
)
SELECT
  trip_distance,  -- Feature: distance of the trip
  trip_seconds,  -- Feature: duration of the trip in seconds
  pickup_hour,  -- Feature: hour of pickup time
  payment_type,  -- Feature: payment method
  is_long_trip  -- Label: long or short trip classification
FROM
  data;

-- Linear regression for fare prediction based on distance
CREATE OR REPLACE MODEL `portoproject-taxitrips-mdzaky.chicago_taxi_trips.taxi_trip_fare_linear_model`
OPTIONS(
  model_type='linear_reg',  -- Specifies linear regression as the model type
  input_label_cols=['fare']  -- The target column for prediction (taxi fare)
) AS
SELECT
    fare,  -- Label: taxi fare
    ST_DISTANCE(ST_GEOGPOINT(pickup_longitude, pickup_latitude), ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)) AS trip_distance  -- Feature: distance of the trip
FROM
    `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE
    -- Ensure that necessary fields are non-NULL and have valid data
    fare IS NOT NULL
    AND pickup_latitude IS NOT NULL
    AND pickup_longitude IS NOT NULL
    AND dropoff_latitude IS NOT NULL
    AND dropoff_longitude IS NOT NULL
LIMIT 10000;  -- Limit the data to reduce storage usage

-- Boosted tree regression for fare prediction based on distance
CREATE OR REPLACE MODEL `portoproject-taxitrips-mdzaky.chicago_taxi_trips.taxi_trip_fare_boosted_tree`
OPTIONS(
  model_type='boosted_tree_regressor',  -- Specifies boosted tree regressor as the model type
  max_iterations=50,  -- Limits the number of iterations to prevent overfitting
  input_label_cols=['fare']  -- The target column for prediction (taxi fare)
) AS
SELECT
    fare,  -- Label: taxi fare
    ST_DISTANCE(ST_GEOGPOINT(pickup_longitude, pickup_latitude), ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)) AS trip_distance  -- Feature: distance of the trip
FROM
    `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE
    -- Ensure that necessary fields are non-NULL and have valid data
    fare IS NOT NULL
    AND pickup_latitude IS NOT NULL
    AND pickup_longitude IS NOT NULL
    AND dropoff_latitude IS NOT NULL
    AND dropoff_longitude IS NOT NULL
LIMIT 10000;  -- Limit the data to reduce storage usage

-- Comparing model performance with machine learning using RMSE (Root Mean Squared Error)
WITH predictions AS (
    SELECT
        'linear_regression' AS model_type,  -- Identify the model type
        fare,  -- Actual fare
        predicted_fare  -- Predicted fare
    FROM
        ML.PREDICT(MODEL `portoproject-taxitrips-mdzaky.chicago_taxi_trips.taxi_trip_fare_linear_model`,
                   (SELECT
                        fare,
                        ST_DISTANCE(ST_GEOGPOINT(pickup_longitude, pickup_latitude), 
                                    ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)) AS trip_distance  -- Feature for prediction
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE fare IS NOT NULL
                      AND pickup_latitude IS NOT NULL
                      AND pickup_longitude IS NOT NULL
                      AND dropoff_latitude IS NOT NULL
                      AND dropoff_longitude IS NOT NULL))
    UNION ALL
    SELECT
        'boosted_tree' AS model_type,  -- Identify the model type
        fare,  -- Actual fare
        predicted_fare  -- Predicted fare
    FROM
        ML.PREDICT(MODEL `portoproject-taxitrips-mdzaky.chicago_taxi_trips.taxi_trip_fare_boosted_tree`,
                   (SELECT
                        fare,
                        ST_DISTANCE(ST_GEOGPOINT(pickup_longitude, pickup_latitude), 
                                    ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)) AS trip_distance  -- Feature for prediction
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE fare IS NOT NULL
                      AND pickup_latitude IS NOT NULL
                      AND pickup_longitude IS NOT NULL
                      AND dropoff_latitude IS NOT NULL
                      AND dropoff_longitude IS NOT NULL))
)

SELECT
    model_type,  -- Model type used
    SQRT(AVG(POW(predicted_fare - fare, 2))) AS rmse  -- Root Mean Squared Error for the model
FROM
    predictions
GROUP BY
    model_type  -- Group by model type to compare RMSE
ORDER BY
    rmse;  -- Sort the results by RMSE in ascending order
