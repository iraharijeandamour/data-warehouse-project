/*
    Creating Three Different Schemas, bronze fo Bronze layer,
    silver  for Silver Layer and gold for Gold Layer
*/
/*creating 3 diffrent user as schemas in oracle*/
CREATE USER bronze IDENTIFIED BY mmyjdm11;
GRANT DBA TO bronze;

CREATE USER silver IDENTIFIED BY mmyjdm11;
GRANT DBA TO silver;

CREATE USER gold IDENTIFIED BY mmyjdm11;
GRANT DBA TO gold;

--Create Table for Customers Information
CREATE TABLE bronze.crm_cust_info(
    cst_id NUMBER,
    cst_key VARCHAR(100),
    cst_firstname VARCHAR(100),
    cst_lastname VARCHAR(100),
    cst_marital_status VARCHAR(100),
    cst_gndr VARCHAR(10),
    cst_create_date DATE
);

----Create Table for Products Information
CREATE TABLE bronze.crm_prd_info(
    prd_id NUMBER,
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(100),
    prd_start_dt DATE,
    prd_end_dt DATE
);

--Create Table for sales details Information

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num VARCHAR(100),
    sls_prd_key VARCHAR(100),
    sls_cust_id VARCHAR(100),
    sls_order_dt VARCHAR(100),
    sls_ship_dt VARCHAR(100),
    sls_due_dt VARCHAR(100),
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

--Create Table For Customer Personal Information
CREATE TABLE bronze.erp_cust_az12(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(10)
);

--Create Table For Location Information

CREATE TABLE bronze.loc_a101(
    cid VARCHAR(50),
    cntry  VARCHAR(50)
);
--Create table Cart Information
CREATE TABLE bronze.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(5)
);
