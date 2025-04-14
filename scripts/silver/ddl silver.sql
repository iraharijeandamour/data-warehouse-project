--DDL SILVER
/*Create Silver Tables With Same Name As in Bronze*/
CREATE TABLE silver.crm_cust_info(
    cst_id NUMBER,
    cst_key VARCHAR(100),
    cst_firstname VARCHAR(100),
    cst_lastname VARCHAR(100),
    cst_marital_status VARCHAR(100),
    cst_gndr VARCHAR(10),
    cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

----Create Table for Products Information
CREATE TABLE silver.crm_prd_info(
    prd_id NUMBER,
    CAT_ID VARCHAR(100),
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(100),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Create Table for sales details Information

CREATE TABLE silver.crm_sales_details(
    sls_ord_num VARCHAR(100),
    sls_prd_key VARCHAR(100),
    sls_cust_id VARCHAR(100),
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Create Table For Customer Personal Information
CREATE TABLE silver.erp_cust_az12(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(10),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
--Create Table For Location Information
CREATE TABLE silver.erp_loc_a101(
    cid VARCHAR(50),
    cntry  VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
--Create table Cart Information
CREATE TABLE silver.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(5),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


