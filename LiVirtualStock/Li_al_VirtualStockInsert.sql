USE [Lvision]
GO

/****** Object:  StoredProcedure [dbo].[Li_al_VirtualStockInsert]    Script Date: 27/9/2025 2:04:08 μμ ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







ALTER   PROCEDURE [dbo].[Li_al_VirtualStockInsert]
AS
BEGIN
   DROP TABLE IF EXISTS #StockData;

 ;with STOCKDATA as (
   SELECT  
       stock.stk_id							as stk_id,
	   dbo.Li_GetVstock(stock.stk_id)		as vStockID,
	   stock.stk_ProductID					as stk_productID,
	   
	   stock.stk_CUQuantity					as stk_CUQuantity,
	   stock.stk_CUQuantityFree				as stk_CuQuantityFree,
	   stock.Stk_CUItemUnitID				as Stk_CUItemUnitID,
	   
	   stockPackType.spt_Quantity			as spt_Quantity,
	   stockPackType.spt_QuantityFree		as spt_QuantityFree,
	   stockPackType.spt_ItemUnitID			as spt_ItemUnitID,
	   
	   stockPackType.spt_CUQuantity			as spt_CUQuantity,
	   stockPackType.spt_CUQuantityFree		as spt_CUQuantityFree,
	   
	   stock.stk_WeightQty					as stk_WeightQty,
	   stock.Stk_WeightUnitID				as Stk_WeightUnitID,
	   
	   stock.stk_LengthQty					as stk_LengthQty,
	   stock.Stk_LengthUnitID				as Stk_LengthUnitID,
	   
	   stock.Stk_AreaQty					as Stk_AreaQty,
	   stock.Stk_AreaUnitID					as Stk_AreaUnitID,
	   
	   stock.Stk_VolumeQty					as Stk_VolumeQty,
	   stock.Stk_VolumeUnitID 				as Stk_VolumeUnitID,
	   
	   spt_ID								as spt_ID,
	   spt_parentID							as spt_ParentID,
	   stockPackType.spt_PackTypeRatio		as spt_PackTypeRatio,
	   
		ROW_NUMBER() OVER (PARTITION BY stock.stk_ID  
									ORDER BY stockPackType.spt_PackTypeRatio asc, stockPackType.spt_ParentID )	as StockPackHierarchyLevel,


		MAX(CASE WHEN spt_parentID IS NULL THEN spt_ItemUnitID END)
        OVER (PARTITION BY stock.stk_ID)  AS ParentItemUnitID							
    
    FROM LV_Stock stock WITH (NOLOCK)
    INNER JOIN LV_StockPackType stockPackType WITH (NOLOCK)  ON stock.stk_ID = stockPackType.spt_StockID
    WHERE stock.stk_LocationID NOT IN (593,605)           -- όχι virtual, ούτε οι άλλες εξαιρέσεις
      AND stock.stk_UnsuitReasonID IS NULL
      AND stock.stk_ReserveReasonID IS NULL
	  AND dbo.Li_GetVstock(stock.stk_id) is null
	)
	select STOCKDATA.*,
			DENSE_RANK() OVER (ORDER BY ParentItemUnitID )AS StockGroup
	INTO #StockData
	from 
	STOCKDATA


	/*STOCK*/
	IF NOT EXISTS (SELECT 1 FROM #StockData) RETURN;

	DROP TABLE IF EXISTS #InsertLV_Stock;

	SELECT 
	ROW_NUMBER() OVER (ORDER BY stk_productID) AS rn	,
	stk_productID				as    stk_productID		,
	sum(stk_CUQuantity)			as    stk_CUQuantity	,
	sum(stk_CuQuantityFree)		as    stk_CuQuantityFree,
	Stk_CUItemUnitID			as    Stk_CUItemUnitID  ,
	sum(stk_WeightQty)			as    stk_WeightQty		,
	Stk_WeightUnitID			as    Stk_WeightUnitID  ,
	sum(stk_LengthQty)			as    stk_LengthQty		,
	Stk_LengthUnitID			as    Stk_LengthUnitID  ,
	sum(Stk_AreaQty)			as    Stk_AreaQty		,
	Stk_AreaUnitID				as    Stk_AreaUnitID	,
	sum(Stk_VolumeQty)			as    Stk_VolumeQty		,
	Stk_VolumeUnitID			as	  Stk_VolumeUnitID  ,
	StockGroup					 as StockGroup

		INTO #InsertLV_Stock
	
	From #StockData
	where StockPackHierarchyLevel=1
	group by stk_productID,Stk_CUItemUnitID,Stk_WeightUnitID,Stk_LengthUnitID,Stk_AreaUnitID,Stk_VolumeUnitID,StockGroup
	order by rn,StockGroup

	
			--select * from #InsertLV_Stock

	DECLARE @rowsStock INT = (SELECT COUNT(*) FROM #InsertLV_Stock),
			@stockcount INT = 1, /*EXEC STOCKID*/@st int , @stkID int

	WHILE @stockcount <= @rowsStock
	Begin
		
		DECLARE 
				@stockGroup			int,
				@stk_productID		INT ,
				@stk_CUQuantity		DECIMAL(18,6),
				@stk_CuQuantityFree DECIMAL(18,6),
				@Stk_CUItemUnitID	INT,										
				@stk_WeightQty		DECIMAL(18,6),
				@Stk_WeightUnitID	INT,
				@stk_LengthQty		DECIMAL(18,6),
				@Stk_LengthUnitID	INT,
				@Stk_AreaQty		DECIMAL(18,6),
				@Stk_AreaUnitID		INT,
				@Stk_VolumeQty		DECIMAL(18,6),
				@Stk_VolumeUnitID	INT;

		
		SELECT 
			@stockGroup			= StockGroup		,
			@stk_productID		= stk_productID		 ,
			
			@stk_CUQuantity		= stk_CUQuantity	 ,	
			@stk_CuQuantityFree = stk_CuQuantityFree , 
			@Stk_CUItemUnitID	= Stk_CUItemUnitID	 ,
			
			@stk_WeightQty		= stk_WeightQty		 ,
			@Stk_WeightUnitID	= Stk_WeightUnitID	 ,
			
			@stk_LengthQty		= stk_LengthQty		 ,
			@Stk_LengthUnitID	= Stk_LengthUnitID	 ,
			
			@Stk_AreaQty		= Stk_AreaQty		 ,	
			@Stk_AreaUnitID		= Stk_AreaUnitID	 ,	
			
			@Stk_VolumeQty		= Stk_VolumeQty		 ,
			@Stk_VolumeUnitID	= Stk_VolumeUnitID	
		FROM #InsertLV_Stock
		WHERE rn = @stockcount;



			/*	select 
			@stockGroup			as StockGroup			,
			@stk_productID		as stk_productID		,
			@stk_CUQuantity		as stk_CUQuantity		,
			@stk_CuQuantityFree as stk_CuQuantityFree ,
			@Stk_CUItemUnitID	as Stk_CUItemUnitID	,
			@stk_WeightQty		as stk_WeightQty		,
			@Stk_WeightUnitID	as Stk_WeightUnitID	,
			@stk_LengthQty		as stk_LengthQty		,
			@Stk_LengthUnitID	as Stk_LengthUnitID	,
			@Stk_AreaQty		as Stk_AreaQty		,
			@Stk_AreaUnitID		as Stk_AreaUnitID		,
			@Stk_VolumeQty		as Stk_VolumeQty		,
			@Stk_VolumeUnitID	as Stk_VolumeUnitID	*/

		
		EXEC  @st  = dbo.sp_GetNextID
                 @seqfield = N'stk_ID',
                 @total    = 1,
                 @result   = @stkID OUTPUT;


		INSERT INTO LV_Stock 
			(   stk_ID,
				stk_LocationID,
				stk_ProductID,
				stk_DepositorID,
				stk_ContainerID,
				stk_CUItemUnitID,
				stk_CUQuantity,
				stk_CUQuantityFree,
				stk_UnsuitReasonID,
				stk_ReserveReasonID,
				stk_LockedForDAFALED,
				stk_LengthQty,
				stk_LengthUnitID,
				stk_AreaQty,
				stk_AreaUnitID,
				stk_VolumeQty,
				stk_VolumeUnitID,
				stk_WeightQty,
				stk_WeightUnitID,
				stk_SplitStockInCUsLED,
				stk_LocationSequence,
				stk_LogisticUnitID,
				stk_DomainID,
				stk_UnitDeadWeight)
		VALUES (
				@stkID,
				605, -- LocationVirtual
				@stk_productID,
				1	,	--Depositor,
				18176,	-- ContainerVirtual
				@Stk_CUItemUnitID,
				@stk_CUQuantity,
				@stk_CuQuantityFree,
				NULL,	--stk_UnsuitReasonID,   
				NULL,	--stk_ReserveReasonID,  
				NULL,	--stk_LockedForDAFALED,
				@stk_LengthQty,		     
				@Stk_LengthUnitID,	    
				@Stk_AreaQty,		     
				@Stk_AreaUnitID,	    
				@Stk_VolumeQty,		     
				@Stk_VolumeUnitID,	    
				@stk_WeightQty,
				@Stk_WeightUnitID,
				
				0,	  --stk_SplitStockInCUsLED
				NULL, --stk_LocationSequence,
				7,	  --stk_LogisticUnitID
				1,	  --DomainID
				NULL  --stk_UnitDeadWeight 
				);

					
		/*StockPackType*/

		DROP TABLE IF EXISTS #InsertLV_StockPackType;


		SELECT
			
            StockPackHierarchyLevel                        AS StockPackHierarchyLevel,
            spt_ItemUnitID                                 AS spt_ItemUnitID,
			SUM(spt_Quantity)							   AS spt_Quantity,
            SUM(spt_QuantityFree)						   AS spt_QuantityFree,
            spt_PackTypeRatio							   AS spt_PackTypeRatio,

		
            SUM(spt_CUQuantity)							   AS spt_CUQuantity,

            SUM(spt_CUQuantityFree)						   AS spt_CUQuantityFree
        
			INTO #InsertLV_StockPackType
        
		FROM #StockData 
		where StockGroup=@stockGroup
        GROUP BY
            StockPackHierarchyLevel,
            spt_ItemUnitID,
            spt_PackTypeRatio
		order by StockPackHierarchyLevel
		
			
		declare @maxStockPackHierarchyLevel int = (select max(StockPackHierarchyLevel) from #InsertLV_StockPackType),
				@curentStockPackHieranchuLevel int=1 ;
		
			DECLARE @prevNewSPTID INT = NULL;

		WHILE @curentStockPackHieranchuLevel<=@maxStockPackHierarchyLevel
				Begin
					DECLARE 
							@spt_ID					int,
							@spt_ItemUnitID			int,
							@spt_Quantity			DECIMAL(18,6),
							@spt_QuantityFree		DECIMAL(18,6),
							@spt_PackTypeRatio		DECIMAL(18,6),
							@spt_ParentID			int,
							@spt_CUQuantity			DECIMAL(18,6),
							@spt_CUQuantityFree		DECIMAL(18,6),
							@newSPTID				INT;
							

					SELECT 
						@spt_ItemUnitID =	    spt_ItemUnitID,
						@spt_PackTypeRatio =	spt_PackTypeRatio,
						@spt_Quantity =			spt_Quantity,
						@spt_QuantityFree =		spt_QuantityFree,
						@spt_CUQuantity =		spt_CUQuantity,
						@spt_CUQuantityFree =	spt_CUQuantityFree
					FROM #InsertLV_StockPackType
					WHERE StockPackHierarchyLevel = @curentStockPackHieranchuLevel;

						
						
					SET @spt_ParentID = CASE WHEN @curentStockPackHieranchuLevel = 1 THEN NULL 
											ELSE @prevNewSPTID  END; --END CASE WHEN


					EXEC dbo.sp_GetNextID
							@seqfield = N'spt_ID',
							@total    = 1,
							@result   = @newSPTID OUTPUT;

						/*select 
							@newSPTID			as spt_id,			
							@spt_ItemUnitID		as spt_ItemUnitID,
							@spt_PackTypeRatio	as spt_PackTypeRatio,
							@spt_ParentID		as spt_parentId,
							@spt_Quantity		as spt_Quantity,
							@spt_QuantityFree	as spt_QuantityFree,
							@spt_CUQuantity		as spt_CUQuantity,
							@spt_CUQuantityFree as spt_CUQuantityFree*/

					INSERT INTO LV_StockPackType 
					( spt_ID, 
					  spt_StockID, 
					  spt_ItemUnitID, 
					  spt_Quantity, 
					  spt_QuantityFree,
					  spt_PackTypeRatio, 
					  spt_ParentID, 
					  spt_CUQuantity, 
					  spt_CUQuantityFree, 
					  spt_DomainID
					  )
					VALUES
					( @newSPTID, 
					  @stkID, 
					  @spt_ItemUnitID, 
					  @spt_Quantity, 
					  @spt_QuantityFree,
					  @spt_PackTypeRatio, 
					  @spt_ParentID, 
					  @spt_CUQuantity, 
					  @spt_CUQuantityFree, 
					  1 --spt_DomainID
					);
					
					SET @prevNewSPTID = @newSPTID;
					set @curentStockPackHieranchuLevel+=1
				End

		
		/*StockAttributes*/
                EXEC dbo.Li_InsertVirtualStockAttributes
                     @StockID   = @stkID,
                     @ProductID = @stk_productID;
            

		set @stockcount+=1
		
	END

	
END
GO


