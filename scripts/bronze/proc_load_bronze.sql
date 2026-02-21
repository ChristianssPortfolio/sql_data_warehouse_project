
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME;
    DECLARE @EndTime DATETIME;

    BEGIN TRY

        PRINT '===== CRM DATA LOADING STARTED =====';


        /* =========================
           CRM Customer Info
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'crm_cust_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        /* =========================
           CRM Product Info
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'crm_prd_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        /* =========================
           CRM Sales Details
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'crm_sales_details loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        PRINT '===== ERP DATA LOADING STARTED =====';


        /* =========================
           ERP Customer AZ12
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'erp_cust_az12 loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        /* =========================
           ERP Location A101
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'erp_loc_a101 loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        /* =========================
           ERP Product Category G1V2
           ========================= */
        SET @StartTime = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\xrist\Documents\SQL_projects\sql_project_for_cv\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @EndTime = GETDATE();
        PRINT 'erp_px_cat_g1v2 loaded in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) 
              + ' seconds';


        PRINT '===== BRONZE LOAD FINISHED SUCCESSFULLY =====';

    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO

EXEC bronze.load_bronze;
GO
