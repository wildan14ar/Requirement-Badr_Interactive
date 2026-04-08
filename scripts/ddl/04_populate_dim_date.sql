-- =====================================================
-- Populate Date Dimension
-- Database: datamart_badr_interactive
-- Purpose: Generate date records for years 2020-2030
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Generate Date Records using a stored procedure
-- =====================================================
DELIMITER $$

DROP PROCEDURE IF EXISTS populate_date_dimension$$

CREATE PROCEDURE populate_date_dimension(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    DECLARE current_date DATE;
    DECLARE day_of_week INT;
    
    SET current_date = start_date;
    
    WHILE current_date <= end_date DO
        -- Calculate day of week (1=Monday, 7=Sunday)
        SET day_of_week = DAYOFWEEK(current_date);
        -- Adjust to make Monday=1, Sunday=7
        SET day_of_week = IF(day_of_week = 1, 7, day_of_week - 1);
        
        INSERT INTO dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            month_short,
            day,
            day_of_week,
            day_name,
            day_short,
            is_weekend,
            is_holiday,
            week_of_year
        ) VALUES (
            DATE_FORMAT(current_date, '%Y%m%d'),  -- date_key in YYYYMMDD format
            current_date,                          -- full_date
            YEAR(current_date),                    -- year
            QUARTER(current_date),                 -- quarter
            MONTH(current_date),                   -- month
            MONTHNAME(current_date),               -- month_name
            LEFT(MONTHNAME(current_date), 3),      -- month_short
            DAY(current_date),                     -- day
            day_of_week,                           -- day_of_week (1=Monday, 7=Sunday)
            DAYNAME(current_date),                 -- day_name
            LEFT(DAYNAME(current_date), 3),        -- day_short
            (day_of_week >= 6),                    -- is_weekend (Saturday=6, Sunday=7)
            FALSE,                                 -- is_holiday (can be updated later)
            WEEK(current_date, 1)                  -- week_of_year (ISO week)
        );
        
        SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
    END WHILE;
    
    SELECT CONCAT('Date dimension populated from ', start_date, ' to ', end_date) AS status;
END$$

DELIMITER ;

-- Execute the procedure for date range 2020-01-01 to 2030-12-31
CALL populate_date_dimension('2020-01-01', '2030-12-31');

-- =====================================================
-- Verify date dimension
-- =====================================================
SELECT 
    COUNT(*) AS total_records,
    MIN(full_date) AS start_date,
    MAX(full_date) AS end_date,
    COUNT(DISTINCT year) AS year_count,
    COUNT(DISTINCT quarter) AS quarter_count,
    COUNT(DISTINCT month) AS month_count
FROM dim_date;

-- Sample records
SELECT * FROM dim_date 
WHERE full_date BETWEEN '2024-01-01' AND '2024-01-07'
ORDER BY full_date;

-- Check weekend flags
SELECT 
    day_name,
    is_weekend,
    COUNT(*) AS day_count
FROM dim_date
GROUP BY day_name, is_weekend
ORDER BY day_of_week;

SELECT 'Date dimension population completed!' AS status;
