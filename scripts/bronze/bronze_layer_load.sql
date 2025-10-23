DELIMITER $$

DROP PROCEDURE IF EXISTS load_bronze $$

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS msg;
        SELECT CONCAT('Error Message: ', @@error_message) AS msg;
        ROLLBACK;
    END;

    SET batch_start_time = NOW();
    SELECT '================================================' AS msg;
    SELECT 'Loading Bronze Layer' AS msg;
    SELECT '================================================' AS msg;

    -- CRM Tables -------------------------------------------------
    SELECT '------------------------------------------------' AS msg;
    SELECT 'Loading CRM Tables' AS msg;
    SELECT '------------------------------------------------' AS msg;

    -- bronze.crm_cust_info
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze_crm_cust_info' AS msg;
    TRUNCATE TABLE bronze_crm_cust_info;

    SELECT '>> Inserting Data Into: bronze_crm_cust_info' AS msg;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze_crm_cust_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- bronze.crm_prd_info
    SET start_time = NOW();
    TRUNCATE TABLE bronze_crm_prd_info;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze_crm_prd_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- bronze.crm_sales_details
    SET start_time = NOW();
    TRUNCATE TABLE bronze_crm_sales_details;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze_crm_sales_details
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- ERP Tables -------------------------------------------------
    SELECT '------------------------------------------------' AS msg;
    SELECT 'Loading ERP Tables' AS msg;
    SELECT '------------------------------------------------' AS msg;

    -- bronze.erp_loc_a101
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_loc_a101;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze_erp_loc_a101
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- bronze.erp_cust_az12
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_cust_az12;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_erp/cust_az12.csv'
    INTO TABLE bronze_erp_cust_az12
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- bronze.erp_px_cat_g1v2
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_px_cat_g1v2;
    LOAD DATA LOCAL INFILE '/path/to/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze_erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- Wrap up ----------------------------------------------------
    SET batch_end_time = NOW();
    SELECT '==========================================' AS msg;
    SELECT 'Loading Bronze Layer Completed' AS msg;
    SELECT CONCAT('Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS msg;
    SELECT '==========================================' AS msg;
END $$

DELIMITER ;
