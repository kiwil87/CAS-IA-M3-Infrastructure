-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Silver layer
-- MAGIC
-- MAGIC ## Connecting to the Silver layer (Target)

-- COMMAND ----------

USE CATALOG levkiwi_lakehouse;
USE DATABASE silver;

-- COMMAND ----------

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of address

-- COMMAND ----------

MERGE INTO silver.address AS tgt
USING (
    SELECT
        AddressID       AS address_id,
        AddressLine1    AS address_line1,
        AddressLine2    AS address_line2,
        City            AS city,
        StateProvince   AS state_province,
        CountryRegion   AS country_region,
        PostalCode      AS postal_code,
        rowguid         AS rowguid,
        ModifiedDate    AS modified_date
    FROM bronze.address
) AS src
ON tgt.address_id = src.address_id
  AND tgt._tf_valid_to IS NULL   -- Only match against 'active' records in silver
  
WHEN MATCHED AND (
       tgt.address_line1    != src.address_line1
    OR tgt.address_line2    != src.address_line2
    OR tgt.city             != src.city
    OR tgt.state_province   != src.state_province
    OR tgt.country_region   != src.country_region
    OR tgt.postal_code      != src.postal_code
    OR tgt.rowguid          != src.rowguid
    OR tgt.modified_date    != src.modified_date
    -- etc. for any columns you want to track changes on
) AND tgt._tf_valid_to IS NULL THEN
  -- 1) Close the old record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date
  
WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
  -- 2) Close the deleted record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date
;

-- COMMAND ----------

MERGE INTO silver.address AS tgt
USING (
    SELECT
        AddressID       AS address_id,
        AddressLine1    AS address_line1,
        AddressLine2    AS address_line2,
        City            AS city,
        StateProvince   AS state_province,
        CountryRegion   AS country_region,
        PostalCode      AS postal_code,
        rowguid         AS rowguid,
        ModifiedDate    AS modified_date
    FROM bronze.address
) AS src
ON tgt.address_id = src.address_id
  AND tgt._tf_valid_to IS NULL   -- Only match against 'active' records in silver
  
WHEN NOT MATCHED THEN
  -- 3) Insert NEW records (either truly new address_id or a new version if the old one was just closed)
  INSERT (
    address_id,
    address_line1,
    address_line2,
    city,
    state_province,
    country_region,
    postal_code,
    rowguid,
    modified_date,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.address_id,
    src.address_line1,
    src.address_line2,
    src.city,
    src.state_province,
    src.country_region,
    src.postal_code,
    src.rowguid,
    src.modified_date,
    load_date,        -- _tf_valid_from
    NULL,             -- _tf_valid_to
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of customer 

-- COMMAND ----------

MERGE INTO silver.customer AS tgt
USING (
    SELECT
        CustomerID       AS customer_id,
        NameStyle        AS name_style,
        Title            AS title,
        FirstName        AS first_name,
        MiddleName       AS middle_name,
        LastName         AS last_name,
        Suffix           AS suffix,
        CompanyName      AS company_name,
        SalesPerson      AS sales_person,
        EmailAddress     AS email_address,
        Phone            AS phone,
        PasswordHash     AS password_hash,
        PasswordSalt     AS password_salt,
        rowguid          AS rowguid,
        ModifiedDate     AS modified_date
    FROM bronze.customer
) AS src
ON tgt.customer_id = src.customer_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN MATCHED AND (
       tgt.name_style        != src.name_style
    OR tgt.title             != src.title
    OR tgt.first_name        != src.first_name
    OR tgt.middle_name       != src.middle_name
    OR tgt.last_name         != src.last_name
    OR tgt.suffix            != src.suffix
    OR tgt.company_name      != src.company_name
    OR tgt.sales_person      != src.sales_person
    OR tgt.email_address     != src.email_address
    OR tgt.phone             != src.phone
    OR tgt.password_hash     != src.password_hash
    OR tgt.password_salt     != src.password_salt
    OR tgt.rowguid           != src.rowguid
    OR tgt.modified_date     != src.modified_date
) AND tgt._tf_valid_to IS NULL THEN
  -- 1) Close the old record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date

WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
  -- 2) Close the deleted record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date
;

-- COMMAND ----------

MERGE INTO silver.customer AS tgt
USING (
    SELECT
        CustomerID       AS customer_id,
        NameStyle        AS name_style,
        Title            AS title,
        FirstName        AS first_name,
        MiddleName       AS middle_name,
        LastName         AS last_name,
        Suffix           AS suffix,
        CompanyName      AS company_name,
        SalesPerson      AS sales_person,
        EmailAddress     AS email_address,
        Phone            AS phone,
        PasswordHash     AS password_hash,
        PasswordSalt     AS password_salt,
        rowguid          AS rowguid,
        ModifiedDate     AS modified_date
    FROM bronze.customer
) AS src
ON tgt.customer_id = src.customer_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN NOT MATCHED THEN
  -- 3) Insert NEW records (new customer_id or new version of existing record)
  INSERT (
    customer_id,
    name_style,
    title,
    first_name,
    middle_name,
    last_name,
    suffix,
    company_name,
    sales_person,
    email_address,
    phone,
    password_hash,
    password_salt,
    rowguid,
    modified_date,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.customer_id,
    src.name_style,
    src.title,
    src.first_name,
    src.middle_name,
    src.last_name,
    src.suffix,
    src.company_name,
    src.sales_person,
    src.email_address,
    src.phone,
    src.password_hash,
    src.password_salt,
    src.rowguid,
    src.modified_date,
    load_date,        -- _tf_valid_from
    NULL,             -- _tf_valid_to
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of sales_order_detail

-- COMMAND ----------

MERGE INTO silver.sales_order_detail AS tgt
USING (
    SELECT
        SalesOrderID          AS sales_order_id,
        SalesOrderDetailID    AS sales_order_detail_id,
        OrderQty              AS order_qty,
        ProductID             AS product_id,
        UnitPrice             AS unit_price,
        UnitPriceDiscount     AS unit_price_discount,
        LineTotal             AS line_total,
        rowguid               AS rowguid,
        ModifiedDate          AS modified_date
    FROM bronze.salesorderdetail
) AS src
ON tgt.sales_order_id = src.sales_order_id
   AND tgt.sales_order_detail_id = src.sales_order_detail_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN MATCHED AND (
       tgt.order_qty           != src.order_qty
    OR tgt.product_id          != src.product_id
    OR tgt.unit_price          != src.unit_price
    OR tgt.unit_price_discount != src.unit_price_discount
    OR tgt.line_total          != src.line_total
    OR tgt.rowguid             != src.rowguid
    OR tgt.modified_date       != src.modified_date
    -- etc. for any additional columns to track changes
) AND tgt._tf_valid_to IS NULL THEN
  -- 1) Close the old record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date

WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
  -- 2) Close the deleted record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date
;

-- COMMAND ----------

MERGE INTO silver.sales_order_detail AS tgt
USING (
    SELECT
        SalesOrderID          AS sales_order_id,
        SalesOrderDetailID    AS sales_order_detail_id,
        OrderQty              AS order_qty,
        ProductID             AS product_id,
        UnitPrice             AS unit_price,
        UnitPriceDiscount     AS unit_price_discount,
        LineTotal             AS line_total,
        rowguid               AS rowguid,
        ModifiedDate          AS modified_date
    FROM bronze.salesorderdetail
) AS src
ON tgt.sales_order_id = src.sales_order_id
   AND tgt.sales_order_detail_id = src.sales_order_detail_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN NOT MATCHED THEN
  -- 3) Insert NEW records (new sales_order_id or new version of existing record)
  INSERT (
    sales_order_id,
    sales_order_detail_id,
    order_qty,
    product_id,
    unit_price,
    unit_price_discount,
    line_total,
    rowguid,
    modified_date,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.sales_order_id,
    src.sales_order_detail_id,
    src.order_qty,
    src.product_id,
    src.unit_price,
    src.unit_price_discount,
    src.line_total,
    src.rowguid,
    src.modified_date,
    load_date,        -- _tf_valid_from
    NULL,             -- _tf_valid_to
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of sales_order_header

-- COMMAND ----------

MERGE INTO silver.sales_order_header AS tgt
USING (
    SELECT
        SalesOrderID          AS sales_order_id,
        RevisionNumber        AS revision_number,
        OrderDate             AS order_date,
        DueDate               AS due_date,
        ShipDate              AS ship_date,
        Status                AS status,
        OnlineOrderFlag       AS online_order_flag,
        SalesOrderNumber      AS sales_order_number,
        PurchaseOrderNumber   AS purchase_order_number,
        AccountNumber         AS account_number,
        CustomerID            AS customer_id,
        ShipToAddressID       AS ship_to_address_id,
        BillToAddressID       AS bill_to_address_id,
        ShipMethod            AS ship_method,
        CreditCardApprovalCode AS credit_card_approval_code,
        SubTotal              AS sub_total,
        TaxAmt                AS tax_amt,
        Freight               AS freight,
        TotalDue              AS total_due,
        Comment               AS comment,
        rowguid               AS rowguid,
        ModifiedDate          AS modified_date
    FROM bronze.salesorderheader
) AS src
ON tgt.sales_order_id = src.sales_order_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN MATCHED AND (
       tgt.revision_number        != src.revision_number
    OR tgt.order_date             != src.order_date
    OR tgt.due_date               != src.due_date
    OR tgt.ship_date              != src.ship_date
    OR tgt.status                 != src.status
    OR tgt.online_order_flag      != src.online_order_flag
    OR tgt.sales_order_number     != src.sales_order_number
    OR tgt.purchase_order_number  != src.purchase_order_number
    OR tgt.account_number         != src.account_number
    OR tgt.customer_id            != src.customer_id
    OR tgt.ship_to_address_id     != src.ship_to_address_id
    OR tgt.bill_to_address_id     != src.bill_to_address_id
    OR tgt.ship_method            != src.ship_method
    OR tgt.credit_card_approval_code != src.credit_card_approval_code
    OR tgt.sub_total              != src.sub_total
    OR tgt.tax_amt                != src.tax_amt
    OR tgt.freight                != src.freight
    OR tgt.total_due              != src.total_due
    OR tgt.comment                != src.comment
    OR tgt.rowguid                != src.rowguid
    OR tgt.modified_date          != src.modified_date
) AND tgt._tf_valid_to IS NULL THEN
  -- 1) Close the old record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date

WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
  -- 2) Close the deleted record by setting _tf_valid_to
  UPDATE SET 
    tgt._tf_valid_to    = load_date,
    tgt._tf_update_date = load_date
;

-- COMMAND ----------

MERGE INTO silver.sales_order_header AS tgt
USING (
    SELECT
        SalesOrderID          AS sales_order_id,
        RevisionNumber        AS revision_number,
        OrderDate             AS order_date,
        DueDate               AS due_date,
        ShipDate              AS ship_date,
        Status                AS status,
        OnlineOrderFlag       AS online_order_flag,
        SalesOrderNumber      AS sales_order_number,
        PurchaseOrderNumber   AS purchase_order_number,
        AccountNumber         AS account_number,
        CustomerID            AS customer_id,
        ShipToAddressID       AS ship_to_address_id,
        BillToAddressID       AS bill_to_address_id,
        ShipMethod            AS ship_method,
        CreditCardApprovalCode AS credit_card_approval_code,
        SubTotal              AS sub_total,
        TaxAmt                AS tax_amt,
        Freight               AS freight,
        TotalDue              AS total_due,
        Comment               AS comment,
        rowguid               AS rowguid,
        ModifiedDate          AS modified_date
    FROM bronze.salesorderheader
) AS src
ON tgt.sales_order_id = src.sales_order_id
   AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver

WHEN NOT MATCHED THEN
  -- 3) Insert NEW records (new sales_order_id or new version of existing record)
  INSERT (
    sales_order_id,
    revision_number,
    order_date,
    due_date,
    ship_date,
    status,
    online_order_flag,
    sales_order_number,
    purchase_order_number,
    account_number,
    customer_id,
    ship_to_address_id,
    bill_to_address_id,
    ship_method,
    credit_card_approval_code,
    sub_total,
    tax_amt,
    freight,
    total_due,
    comment,
    rowguid,
    modified_date,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.sales_order_id,
    src.revision_number,
    src.order_date,
    src.due_date,
    src.ship_date,
    src.status,
    src.online_order_flag,
    src.sales_order_number,
    src.purchase_order_number,
    src.account_number,
    src.customer_id,
    src.ship_to_address_id,
    src.bill_to_address_id,
    src.ship_method,
    src.credit_card_approval_code,
    src.sub_total,
    src.tax_amt,
    src.freight,
    src.total_due,
    src.comment,
    src.rowguid,
    src.modified_date,
    load_date,        -- _tf_valid_from
    NULL,             -- _tf_valid_to
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )
