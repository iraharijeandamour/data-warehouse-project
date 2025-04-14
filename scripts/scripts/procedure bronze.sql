/*Creating Procedure to store whole codes So that this Data pipeline Can be Executed When Scheduled, 
 we start from creating virtual tables which can't be stored in user tables, after we will write 
query to drop them, if we try to create them when are already created, they are dropped first, and 
full load and trucnate, we truncate all records from table before we insert new data to avoid data redudancy.
*/
CREATE OR REPLACE PROCEDURE LOAD_ALL_DATA AUTHID CURRENT_USER AS 
    v_sql VARCHAR2(4000);
    v_count INTEGER;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL DAY TO SECOND;
    
BEGIN
-- Drop and Create External Tables
    SELECT COUNT(*) INTO v_count 
    FROM all_tables 
    WHERE owner = 'BRONZE' AND table_name IN 
    (SELECT TABLE_NAME FROM ALL_TABLES WHERE 
        owner = 'BRONZE' AND TABLE_NAME LIKE 'UN_%');

    IF v_count > 0 THEN
        FOR table_delete IN
            (
                SELECT TABLE_NAME FROM ALL_TABLES WHERE 
                owner = 'BRONZE' AND TABLE_NAME LIKE 'UN_%'
            )LOOP
                v_sql := 'DROP TABLE BRONZE.' || table_delete.TABLE_NAME;
            EXECUTE IMMEDIATE v_sql;
            dbms_output.put('Dropping Table=>'||table_delete.TABLE_NAME);
        END LOOP;
    END IF;
    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_crm_cust_info(
            cst_id NUMBER,
            cst_key VARCHAR(100),
            cst_firstname VARCHAR(100),
            cst_lastname VARCHAR(100),
            cst_marital_status VARCHAR(100),
            cst_gndr VARCHAR(10),
            cst_create_date DATE
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_crm
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date DATE "YYYY-MM-DD")
            )
            LOCATION (''cust_info.csv'')
        )
        REJECT LIMIT UNLIMITED';    
    DBMS_OUTPUT.PUT_LINE('External crm_cust_info created successfully!');

    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_crm_prd_info(
            prd_id NUMBER,
            prd_key VARCHAR(100),
            prd_nm VARCHAR(100),
            prd_cost INT,
            prd_line VARCHAR(100),
            prd_start_dt DATE,
            prd_end_dt DATE
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_crm
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt DATE ''YYYY-MM-DD'', prd_end_dt DATE ''YYYY-MM-DD'')
            )
            LOCATION (''prd_info.csv'')
        )
        REJECT LIMIT UNLIMITED';
    DBMS_OUTPUT.PUT_LINE('External table prd_info created');

    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_crm_sales_details(
            sls_ord_num VARCHAR(100),
            sls_prd_key VARCHAR(100),
            sls_cust_id VARCHAR(100),
            sls_order_dt VARCHAR(100),
            sls_ship_dt VARCHAR(100),
            sls_due_dt VARCHAR(100),
            sls_sales INT,
            sls_quantity INT,
            sls_price INT
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_crm
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
            )
            LOCATION (''sales_details.csv'')
        )
        REJECT LIMIT UNLIMITED';
    DBMS_OUTPUT.PUT_LINE('External table sales_details created!');

    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_erp_cust_az12(
            cid VARCHAR(50),
            bdate DATE,
            gen VARCHAR(10)
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_erp
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (cid, bdate DATE ''YYYY-MM-DD'', gen)
            )
            LOCATION (''CUST_AZ12.csv'')
        )
        REJECT LIMIT UNLIMITED';
    DBMS_OUTPUT.PUT_LINE('External Table CUST_AZ12 Created Successfully!');

    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_loc_a101(
            cid VARCHAR(50),
            cntry  VARCHAR(50)
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_erp
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (cid, cntry)
            )
            LOCATION (''LOC_A101.csv'')
        )
        REJECT LIMIT UNLIMITED';
    DBMS_OUTPUT.PUT_LINE('External Table LOC_A101 Created Successfully!');

    EXECUTE IMMEDIATE '
        CREATE TABLE bronze.un_erp_px_cat_g1v2(
            id VARCHAR(50),
            cat VARCHAR(50),
            subcat VARCHAR(50),
            maintenance VARCHAR(5)
        )
        ORGANIZATION EXTERNAL(
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY source_erp
            ACCESS PARAMETERS(
                RECORDS DELIMITED BY NEWLINE
                SKIP=1
                FIELDS TERMINATED BY '',''
                OPTIONALLY ENCLOSED BY ''"''
                MISSING FIELD VALUES ARE NULL
                (id, cat, subcat, maintenance)
            )
            LOCATION (''PX_CAT_G1V2.csv'')
        )
        REJECT LIMIT UNLIMITED';
    DBMS_OUTPUT.PUT_LINE('External Table PX_CAT_G1V2 Created Successfully!');

    -- Truncate and Insert Data
    -- Getting Loading Starting Time
    v_start_time := SYSTIMESTAMP;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_cust_info';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.crm_cust_info SELECT * FROM bronze.un_crm_cust_info';
    DBMS_OUTPUT.PUT_LINE('Customer Information Inserted');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_prd_info';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.crm_prd_info SELECT * FROM bronze.un_crm_prd_info';
    DBMS_OUTPUT.PUT_LINE('Product Information Inserted');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_sales_details';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.crm_sales_details SELECT * FROM bronze.un_crm_sales_details';
    DBMS_OUTPUT.PUT_LINE('Sales Details Information Inserted');
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_cust_az12';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.erp_cust_az12 SELECT * FROM bronze.un_erp_cust_az12';
    DBMS_OUTPUT.PUT_LINE('Customer More Information Inserted');
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_loc_a101';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.erp_loc_a101 SELECT * FROM bronze.un_loc_a101';
    DBMS_OUTPUT.PUT_LINE('Customer Location Information Inserted');
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
    EXECUTE IMMEDIATE 'INSERT INTO bronze.erp_px_cat_g1v2 SELECT * FROM bronze.un_erp_px_cat_g1v2';
    DBMS_OUTPUT.PUT_LINE('Cart Information Inserted');
    
    -- Getting Loading Starting Time
    v_end_time := SYSTIMESTAMP;
    
    v_duration := v_end_time - v_start_time;
    DBMS_OUTPUT.PUT_LINE('Loading Process Started At '||v_start_time);
    DBMS_OUTPUT.PUT_LINE('Loading Process Ended At '||v_end_time);
    DBMS_OUTPUT.PUT_LINE('Loading Process Executed In '||v_duration);
END;
-- After Running Whole Script Uncoment The Following Line And Execute It so Changes can be applied
-- EXEC LOAD_ALL_DATA;

