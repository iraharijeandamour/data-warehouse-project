/*CREATING STORED PROCEDURE TO RUN DAILY IF CHANGE IN SOURCES DATA HAPPEN*/
/*DECLRARING 4 VARIABLES FOR GETTING EXECUTION STARTING TIME
AND EXECUTIOND ENDING TIME*/
--This will get all data from bronze but cleaned, standarized and transformed data
CREATE OR REPLACE PROCEDURE LOAD_SILVER AUTHID CURRENT_USER AS
    V_START_T TIMESTAMP;
    V_END_T TIMESTAMP;
    V_DURATION INTERVAL DAY TO SECOND;
    V_DURATION_IN_SEC NUMBER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_cust_info';
    DBMS_OUTPUT.PUT_LINE('DELETING ALL DATA FROM silver.crm_cust_info');
    V_START_T := SYSTIMESTAMP;
    /*INSERTING DATA FROM BRONZE TO SILVER LAYER */
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname ,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        CST_ID,
        CST_KEY,
        TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME,
        TRIM(CST_LASTNAME) AS CST_LASTNAME,
        CASE WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
             ELSE 'N/A'
        END CST_MARITAL_STATUS,
        CASE WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
             ELSE 'N/A'
        END CST_MARITAL_STATUS,
        CST_CREATE_DATE
        FROM    
            (SELECT P.*, ROW_NUMBER() 
            OVER(PARTITION BY cst_id ORDER BY CST_CREATE_DATE DESC) 
            AS FLAG_LAST
                FROM (SELECT * FROM bronze.crm_cust_info WHERE CST_ID 
                IN
            (SELECT cst_id FROM bronze.crm_cust_info 
                GROUP BY CST_ID )) P)
                WHERE FLAG_LAST = 1;    
     V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info ENDED AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info TOOKS'|| V_DURATION_IN_SEC);
    /* INSERTING INTO SILVER.CRM_PRD_INFO AFTER DATA PREPARATION */
    DBMS_OUTPUT.PUT_LINE('Deleting All Rows Before Insert Any Data');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_prd_info';
    DBMS_OUTPUT.PUT_LINE('Inserting Data');
    V_START_T := SYSTIMESTAMP;
    INSERT INTO SILVER.CRM_PRD_INFO
        (
            prd_id,
            CAT_ID,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
    SELECT PRD_ID,
           REPLACE(SUBSTR(PRD_KEY, 1, 5), '-', '_') AS CAT_ID,
           REPLACE(SUBSTR(PRD_KEY, 7, LENGTH(PRD_KEY)), '-', '_') AS PRD_KEY,
           prd_nm,
           NVL(prd_cost, 0) AS PRD_COST,
           CASE TRIM(UPPER(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'   
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'TouriNg'
                ELSE 'N/A'
           END AS PRD_LINE     ,
           prd_start_dt,
           LEAD(prd_START_dt) OVER (PARTITION BY PRD_KEY ORDER BY PRD_START_DT)-1 AS prd_end_dt
        FROM 
        BRONZE.CRM_PRD_INFO;
     V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info ENDED AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_PRD_info TOOKS'|| V_DURATION_IN_SEC);   
        
    /* TRANSFORM, NORMALIZE, CLEAN AND INSERT INTO SILVER LAYER */
    DBMS_OUTPUT.PUT_LINE('Deleting All Rows Before Insert Any Data silver.crm_sales_details');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_sales_details';
    DBMS_OUTPUT.PUT_LINE('Inserting Data silver.crm_sales_details');
    V_START_T := SYSTIMESTAMP;
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        SLS_PRD_KEY,
        SLS_CUST_ID,
        CASE WHEN sls_ORDER_DT=0 OR LENGTH(sls_ORDER_DT) !=8 THEN Null
            ELSE TO_DATE(sls_ORDER_DT, 'YYYY-MM-DD')
        END AS sls_ORDER_DT,
        CASE WHEN SLS_ship_DT=0 OR LENGTH(SLS_ship_DT) !=8 THEN Null
            ELSE TO_DATE(SLS_ship_DT, 'YYYY-MM-DD')
        END AS SLS_ship_DT ,
        CASE WHEN sls_due_dt=0 OR LENGTH(sls_due_dt) !=8 THEN Null
            ELSE TO_DATE(sls_due_dt, 'YYYY-MM-DD')
        END AS sls_due_dt ,
        CASE WHEN sls_sales IS NULL OR sls_sales <=0 
                    OR sls_sales != SLS_QUANTITY * ABS(SLS_PRICE)
                    THEN SLS_QUANTITY * ABS(SLS_PRICE) 
             ELSE sls_sales
        END sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0 
                    OR sls_price != SLS_SALES/SLS_QUANTITY
                    THEN SLS_SALES/NULLIF(SLS_QUANTITY, 0)
             ELSE SLS_PRICE
        END SLS_PRICE
    FROM BRONZE.CRM_SALES_DETAILS;    
     V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_SALES_DETAILS STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_SALES_DETAILS AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.crm_SALES_DETAILS TOOKS'|| V_DURATION_IN_SEC);
    
    
    /* PREPARE AND INSERT DATA INTO SILVER.ERP_CUST_AZ12 FROM BRONZE.ERP_CUST_AZ12 */
    DBMS_OUTPUT.PUT_LINE('Deleting All Rows Before Insert Any Data INTO silver.ERP_CUST_AZ12');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.ERP_CUST_AZ12';
    DBMS_OUTPUT.PUT_LINE('Inserting Data silver.ERP_CUST_AZ12');
    V_START_T := SYSTIMESTAMP;
    INSERT INTO SILVER.ERP_CUST_AZ12
        (
            CID,
            BDATE,
            GEN
        )
    SELECT CASE WHEN CID LIKE 'NAS%' THEN SUBSTR(CID, 4, LENGTH(CID))
                ELSE CID
           END CID,
           CASE WHEN BDATE > CURRENT_DATE THEN NULL
                ELSE BDATE
           END BDATE,
           CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
           END GEN      
        FROM BRONZE.ERP_CUST_AZ12;
     V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_cust_AZ12 STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_CUST_AZ12 ENDED AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP-CUST_AZ12 TOOKS'|| V_DURATION_IN_SEC);
    /*VIEW DATA FROM SILVER.ERP_CUST_AZ12 */
    EXECUTE IMMEDIATE 'SELECT * FROM SILVER.ERP_CUST_AZ12';
    
    /*LOAD, CLEAN AND INSERT DATA INTO SILVER.ERP_LOC_AZ12 FROM BRONZE.ERP_LOC_AZ12 */
    DBMS_OUTPUT.PUT_LINE('Deleting All Rows Before Insert Any Data INTO silver.ERP_LOC_A101');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.ERP_LOC_A101';
    DBMS_OUTPUT.PUT_LINE('Inserting Data INTO silver.ERP_LOC_A101');
    V_START_T := SYSTIMESTAMP;
    INSERT INTO SILVER.ERP_LOC_A101
        (CID, CNTRY)
    SELECT 
        REPLACE(CID, '-', '') AS CID,
        CASE WHEN TRIM(CNTRY) IN ('USA', 'US') THEN 'United States'
             WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
             WHEN TRIM(CNTRY) IS NULL THEN 'N/A'
             ELSE TRIM(CNTRY)
        END AS CNTRY
        FROM BRONZE.ERP_LOC_A101;
    V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_LOC_A101 STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_LOC_A101 ENDED AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_LOC_A101 TOOKS'|| V_DURATION_IN_SEC);
        
    /* VIEW DATA FROM SILVER.ERP_LOC_A101 */
    EXECUTE IMMEDIATE 'SELECT * FROM SILVER.ERP_LOC_A101';
    
    /*LOAD, CLEAN AND INSERT DATA INTO SILVER.ERP_LOC_AZ12 FROM BRONZE.ERP_LOC_AZ12 */
    /*THIS TABLE DATA IS FRIENDLY */
    DBMS_OUTPUT.PUT_LINE('Deleting All Rows Before Insert Any Data');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.ERP_PX_CAT_G1V2';
    DBMS_OUTPUT.PUT_LINE('Inserting Data INTO SILVER.ERP_PX_CAT_G1V2');
    V_START_T := SYSTIMESTAMP;
    INSERT INTO SILVER.ERP_PX_CAT_G1V2
        (ID, CAT, SUBCAT, MAINTENANCE)
    SELECT * FROM BRONZE.ERP_PX_CAT_G1V2;
    
     V_END_T := SYSTIMESTAMP; 
     V_DURATION := V_END_T - V_START_T;
     V_DURATION_IN_SEC := EXTRACT(DAY FROM V_DURATION)* 86400 + 
                   EXTRACT(HOUR FROM V_DURATION)* 3600 +
                   EXTRACT(MINUTE FROM V_DURATION)* 60 +
                   EXTRACT(SECOND FROM V_DURATION) ;
                   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_PX_CAT_G1V2 STARTED AT'|| V_START_T)   ;   
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_PX_CAT_G1V2 ENDED AT'|| V_END_T);
     DBMS_OUTPUT.PUT_LINE('INSERTING INTO silver.ERP_PX_CAT_G1V2 TOOKS'|| V_DURATION_IN_SEC);
    /*VIEW DATA FROM SILVER.ERP_PX_CAT_G1V2 */
    EXECUTE IMMEDIATE 'SELECT * FROM SILVER.ERP_PX_CAT_G1V2';
    
END;
/*EXECUTE LOAD_SILVER AFTER RUNNING WHOLE PROCEDURE USE BELOW COMMAND*/
--EXEC LOAD_SILVER;
