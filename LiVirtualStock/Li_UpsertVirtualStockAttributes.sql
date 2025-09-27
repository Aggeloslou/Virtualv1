USE [Lvision]
GO

/****** Object:  StoredProcedure [dbo].[Li_UpsertVirtualStockAttributes]    Script Date: 27/9/2025 2:05:31 μμ ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[Li_UpsertVirtualStockAttributes]
    @StockID   INT,        -- το stk_ID του virtual stock
    @ProductID INT,        -- το προϊόν στο οποίο βασίζονται τα stock attributes
    @DomainID  INT = 1     -- προεπιλογή: 1
AS
BEGIN
    SET NOCOUNT ON;

    -- Φόρτωσε ΜΙΑ φορά τα attributes του προϊόντος με τις default τιμές για Virtual
    DROP TABLE IF EXISTS #AttrRows;
    ;WITH Attrs AS (
        SELECT DISTINCT
            sa.sat_ID AS AttributeID,
            CASE 
                WHEN at.att_MessageCode = 'ATTR_NUMERIC'  THEN CAST(0 AS NVARCHAR(50))
                WHEN at.att_MessageCode = 'ATTR_TEXT'     THEN '0'
                WHEN at.att_MessageCode = 'ATTR_DATE'     THEN CONVERT(NVARCHAR(50), '2000-01-01', 120)
                WHEN at.att_MessageCode = 'ATTR_BOOLEAN'  THEN CAST(0 AS NVARCHAR(50))
                WHEN at.att_MessageCode = 'ATTR_LIST'     THEN '01'
                WHEN at.att_MessageCode = 'ATTR_DATETIME' THEN CONVERT(NVARCHAR(50), '2000-01-01 01:00:00', 120)
                WHEN at.att_MessageCode = 'ATTR_TIME'     THEN CONVERT(NVARCHAR(50), '01:00:00', 108)
            END AS VirtualStockValue
        FROM LV_ProductStockAttributes psa WITH (NOLOCK)
        INNER JOIN LV_StockAttributes   sa  WITH (NOLOCK) ON sa.sat_ID = psa.psa_AttributeID
        INNER JOIN LV_AttributeTypes    at  WITH (NOLOCK) ON at.att_ID = sa.sat_TypeID
        WHERE psa.psa_ProductID = @ProductID
    )
    SELECT 
        A.AttributeID,
        A.VirtualStockValue,
        @DomainID AS DomainID
    INTO #AttrRows
    FROM Attrs A;

    -- Row-by-row UPSERT (χρειάζεται επειδή το sav_ID παράγεται από sp_GetNextID ανά γραμμή)
    DECLARE @AttributeID INT, @Value NVARCHAR(MAX), @sav_ID INT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT AttributeID, VirtualStockValue
        FROM #AttrRows
        ORDER BY AttributeID;

    OPEN cur;
    FETCH NEXT FROM cur INTO @AttributeID, @Value;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Αν υπάρχει ήδη (sav_StockID, sav_attributeID) -> UPDATE
        IF EXISTS (
            SELECT 1
            FROM LV_StockAttributesValues WITH (UPDLOCK, HOLDLOCK)
            WHERE sav_StockID = @StockID
              AND sav_attributeID = @AttributeID
        )
        BEGIN
            UPDATE LV_StockAttributesValues
            SET sav_Value    = @Value,
                sav_DomainID = @DomainID
            WHERE sav_StockID = @StockID
              AND sav_attributeID = @AttributeID;
        END
        ELSE
        BEGIN
            -- Αλλιώς -> INSERT με νέο sav_ID
            EXEC dbo.sp_GetNextID 
                 @seqfield = N'sav_ID',
                 @total    = 1,
                 @result   = @sav_ID OUTPUT;

            INSERT INTO LV_StockAttributesValues (sav_ID, sav_StockID, sav_attributeID, sav_Value, sav_DomainID)
            VALUES (@sav_ID, @StockID, @AttributeID, @Value, @DomainID);
        END

        FETCH NEXT FROM cur INTO @AttributeID, @Value;
    END

    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE IF EXISTS #AttrRows;
END
GO


