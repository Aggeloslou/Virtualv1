USE [Lvision]
GO

/****** Object:  StoredProcedure [dbo].[Li_al_UpdateInsertVirtual]    Script Date: 27/9/2025 2:01:46 μμ ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO












ALTER procedure [dbo].[Li_al_UpdateInsertVirtual](@LogID INT)
AS
BEGIN
	--DECLARE @LogID INT=1542116;
	
	DECLARE @MaxRowLoopData INT,@RowLoopData INT = 1;
	
	DROP TABLE if exists #LoopData;
	
	SELECT 
	   ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) 	AS RowNum,
	    lsk_LogID,
	    lsk_ToStockID,
	    lsk_ProductID,
		DENSE_RANK() OVER (ORDER BY ParentItemUnitID ) AS StockGroup,
		ParentItemUnitID
	INTO #LoopData
	FROM ( SELECT 
				DISTINCT 
				lsk_LogID,
				lsk_ToStockID,
				lsk_ProductID,
				MAX(CASE WHEN lsp_parentID IS NULL THEN lsp_ItemUnitID END)
								OVER (PARTITION BY lsk_id)  AS ParentItemUnitID	
			FROM LV_LogStock WITH(NOLOCK)
			inner join LV_LogStockPackType  WITH(NOLOCK) ON lsp_LogStockID = lsk_ID
			WHERE lsk_LogID = @LogID and lsk_LocationID not in (/*3,*/593,605) ) a   --01RE01 01RIP01 VIRTUAL--
	
	 
	SET @MaxRowLoopData = (SELECT  COUNT(*) FROM #LoopData);
			
	WHILE @RowLoopData <= @MaxRowLoopData
	 
	  BEGIN --LoopData
		 
		DROP TABLE IF EXISTS #LOGSTOCKDATA

		DECLARE @curLogID INT,@curToStockID INT,@prdID INT,@ExistingStock int , @StockGroup INT,@ParentItemUnitID INT,
				
				@lsk_Qty DECIMAL(18,6),@lsk_QtyFree DECIMAL(18,6),@TranType int;
	
		SELECT 
			@curLogID		=	lsk_LogID,
			@curToStockID	=	lsk_ToStockID,
			@prdID			=	lsk_ProductID,
			@StockGroup		=	StockGroup,
			@ParentItemUnitID = ParentItemUnitID
		FROM #LoopData
		WHERE RowNum = @RowLoopData;
	
	

		;with STOCKDATA as (
		    SELECT 		
				log_id																			as log_id,								
				lsk_ID																			as lsk_ID,
				
				lsk_ProductID																	as ProductID,
				lsk_CUQuantity																	as lsk_CUQuantity,
				lsk_CUQuantityFree																as lsk_CUQuantityFree,
				lsk_ItemUnitID																	as lsk_ItemUnitID,
				
				lsk_ToStockID																	as StkID,
				
				lsk_WeightQty																	as lsk_WeightQty,
				lsk_WeightUnitID																as lsk_WeightUnitID,
		
				lsk_LengthQty																	as lsk_LengthQty,
				lsk_LengthUnitID																as lsk_LengthUnitID,
		
				lsk_AreaQty																		as lsk_AreaQty,
				lsk_AreaUnitID																	as lsk_AreaUnitID,
		
				lsk_VolumeQty																	as lsk_VolumeQty,
				lsk_VolumeUnitID																as lsk_VolumeUnitID,
		
				lsp_ID																			as lsp_ID,
				Lsp_ItemUnitID																	as Lsp_ItemUnitID,
				Lsp_Quantity																	as Lsp_Quantity,
				Lsp_QuantityFree																as Lsp_QuantityFree,
				lsp_CUQuantity																	as lsp_CUQuantity,
				lsp_PackTypeRatio																as lsp_PackTypeRatio,
				lsp_ParentID																	as lsp_ParentID,
		
				ROW_NUMBER() OVER (PARTITION BY log_id,lsk_id  
					ORDER BY lsp_PackTypeRatio asc, lsp_ParentID )								as StockPackHierarchyLevel	,
		
				MAX(CASE WHEN lsp_parentID IS NULL THEN lsp_ItemUnitID END)
								OVER (PARTITION BY lsk_id)  AS ParentItemUnitID	
					
					
			FROM LV_Log							with(nolock)  
				inner join  LV_LogStock			with(nolock) on log_ID = lsk_LogID
				inner join  LV_LogStockPackType with(nolock) on lsp_LogStockID = lsk_ID --and lsp_ParentID is null
			WHERE 		
				log_TransactionTypeID in (1,3,4,5,6,7,8,10,11,12,25,27) and 
				lsk_LocationID not in (/*3,*/593,605)  --01RE01 01RIP01 VIRTUAL--
				and lsk_UnsuitReasonID is null 
				and log_id=@curLogID
				and lsk_ToStockID=@curToStockID
				and lsk_ProductID=@prdID
				AND (
					(log_TransactionTypeID = 3 AND lsk_originalLed = 1)
					OR
					(log_TransactionTypeID <> 3 AND lsk_originalLed IN (0,1))	)
					
			)SELECT 
				ROW_NUMBER() OVER (ORDER BY ProductID) AS rn,
				dbo.Li_GetVstockForLog(log_ID,StkID,ParentItemUnitID,ProductID)						as VirtualStkID,
				ProductID				as ProductID,
				sum(lsk_CUQuantity)		as lsk_CUQuantity,
				sum(lsk_CUQuantityFree) as lsk_CUQuantityFree,
				lsk_ItemUnitID			as lsk_ItemUnitID,
		
				sum(lsk_WeightQty)		as lsk_WeightQty,
				lsk_WeightUnitID		as lsk_WeightUnitID,
				
				sum(lsk_LengthQty)		as lsk_LengthQty,
				lsk_LengthUnitID		as lsk_LengthUnitID,
				
				sum(lsk_AreaQty)		as lsk_AreaQty,
				lsk_AreaUnitID			as lsk_AreaUnitID,
		
				sum(lsk_VolumeQty)		as lsk_VolumeQty,
				lsk_VolumeUnitID		as lsk_VolumeUnitID,
				
				
				sum(Lsp_Quantity)		as Lsp_Quantity,
				sum(Lsp_QuantityFree)	as Lsp_QuantityFree,
				Lsp_ItemUnitID			as Lsp_ItemUnitID,
				sum(lsp_CUQuantity)		as lsp_CUQuantity,
				lsp_PackTypeRatio		as lsp_PackTypeRatio,
				StockPackHierarchyLevel as StockPackHierarchyLevel,
				DENSE_RANK() OVER (ORDER BY ParentItemUnitID) AS StockGroup,
				ParentItemUnitID
				
				INTO #LOGSTOCKDATA
			FROM 
			STOCKDATA 
			WHERE  ParentItemUnitID =@ParentItemUnitID
			GROUP BY 
			dbo.Li_GetVstockForLog(log_ID,StkID,ParentItemUnitID,ProductID),
		    ProductID,
		    lsk_ItemUnitID,
		    lsk_WeightUnitID,
		    lsk_LengthUnitID,
		    lsk_AreaUnitID,
		    lsk_VolumeUnitID,
		    Lsp_ItemUnitID,
		    lsp_PackTypeRatio,
		    StockPackHierarchyLevel,
			ParentItemUnitID
			order by StockPackHierarchyLevel


			SET  @ExistingStock= (SELECT top 1 isnull(VirtualStkID,0) from #LOGSTOCKDATA)

			SET @TranType=(select top 1 log_transactionTypeID from LV_Log where log_ID=@curLogID)
			
			SET  @lsk_Qty =(SELECT top 1 lsk_CUQuantity from #LOGSTOCKDATA)

			SET  @lsk_QtyFree =(SELECT top 1  lsk_CUQuantityFree from #LOGSTOCKDATA)

			IF /*@lsk_Qty=0 or*/ (@lsk_Qty<0 and @lsk_QtyFree<0 and @ExistingStock=0) 
			or(@lsk_Qty=0 and @lsk_QtyFree=0)  RETURN

			
			--select * from #LOGSTOCKDATA
			--order by StockPackHierarchyLevel

			DECLARE 
				@vstk_ID			INT,
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
				--@stockGroup			INT;

			DECLARE 
				@spt_ItemUnitID		INT,	
				@spt_Quantity		DECIMAL(18,6),	
				@spt_QuantityFree	DECIMAL(18,6),	
				@spt_CUQuantity		DECIMAL(18,6),
				@spt_CUQuantityFree	DECIMAL(18,6),
				@spt_StockID		INT,
				@spt_PackTypeRatio	INT,
				@spt_ParentID		INT;

				--select  @ExistingStock

			IF @ExistingStock<>0
			BEGIN --EXISTINGSTOCK
			
				/* =========================================================
		           1.UPDATE ΥΠΑΡΧΩΝ LV_STOCK 
		           ========================================================= */
				   DROP TABLE IF EXISTS #STOCKUPDATE
				
				SELECT TOP(1)
					@stockGroup			= StockGroup,
					@vstk_ID			= VirtualStkID,
					@stk_productID		= ProductID,
					@stk_CUQuantity		= case when @TranType=3 then 0				else lsk_CUQuantity		end,
					@stk_CuQuantityFree	= case when @TranType=3 then lsk_CUQuantity else lsk_CUQuantityFree end,
					@Stk_CUItemUnitID	= lsk_ItemUnitID,
					@stk_WeightQty		= lsk_WeightQty,
					@Stk_WeightUnitID	= lsk_WeightUnitID,
					@stk_LengthQty		= lsk_LengthQty,
					@Stk_LengthUnitID	= lsk_LengthUnitID,
					@Stk_AreaQty		= lsk_AreaQty,
					@Stk_AreaUnitID		= lsk_AreaUnitID,
					@Stk_VolumeQty		= lsk_VolumeQty,
					@Stk_VolumeUnitID	 = lsk_VolumeUnitID
				FROM  #LOGSTOCKDATA
				where StockPackHierarchyLevel=1 and ParentItemUnitID=@ParentItemUnitID
				order by rn,StockGroup

				/*SELECT
					@vstk_ID			as vstk_ID,
					@stk_productID		as stk_productID,
					@stk_CUQuantity		as stk_CUQuantity,
					@stk_CuQuantityFree	as stk_CuQuantityFree,
					@Stk_CUItemUnitID	as Stk_CUItemUnitID,
					@stk_WeightQty		as stk_WeightQty,
					@Stk_WeightUnitID	as Stk_WeightUnitID,
					@stk_LengthQty		as stk_LengthQty,
					@Stk_LengthUnitID	as Stk_LengthUnitID,
					@Stk_AreaQty		as Stk_AreaQty,
					@Stk_AreaUnitID		as Stk_AreaUnitID,
					@Stk_VolumeQty		as Stk_VolumeQty,
					@Stk_VolumeUnitID	as Stk_VolumeUnitID*/

					

					IF	( SELECT stk_CUQuantity+@stk_CUQuantity			FROM LV_Stock WHERE stk_id=@vstk_ID)<=0 
							AND 
						( SELECT stk_CUQuantityFree+@stk_CuQuantityFree FROM LV_Stock WHERE stk_id=@vstk_ID)<=0
						
						BEGIN --DELETESTOCK
							DELETE LV_Stock					WHERE stk_ID=@vstk_ID
						
							DELETE LV_StockPackType			WHERE spt_StockID=@vstk_ID
							
							DELETE LV_StockAttributesValues WHERE sav_StockID=@vstk_ID
						END --DELETESTOCK

				UPDATE LV_Stock
				set 
					stk_CUQuantity		= stk_CUQuantity		+		@stk_CUQuantity,
					stk_CuQuantityFree	= stk_CuQuantityFree	+		@stk_CuQuantityFree,
					stk_WeightQty		= stk_WeightQty			+		@stk_WeightQty,
					stk_LengthQty		= stk_LengthQty			+		@stk_LengthQty,
					Stk_AreaQty			= Stk_AreaQty			+		@Stk_AreaQty,
					Stk_VolumeQty		= Stk_VolumeQty			+		@Stk_VolumeQty

				WHERE 
						stk_ID=@vstk_ID	
					and stk_ProductID	 = 	@stk_productID
					and stk_CUItemUnitID =	@Stk_CUItemUnitID
					and stk_WeightUnitID =	@Stk_WeightUnitID
					and stk_LengthUnitID =	@Stk_LengthUnitID
					and Stk_AreaUnitID   =	@Stk_AreaUnitID
					and Stk_VolumeUnitID =	@Stk_VolumeUnitID

				
				/* =========================================================
		           1.UPDATE ΥΠΑΡΧΩΝ LV_STOCKPACKTYPE
		           ========================================================= */

				   DROP TABLE IF EXISTS #UpdateLV_StockPackType;
				   
				   		select  VirtualStkID		as spt_StockID,
								Lsp_ItemUnitID		as spt_ItemUnitID,
								Lsp_Quantity		as spt_Quantity,
								Lsp_QuantityFree	as spt_QuantityFree,
								lsp_CUQuantity		as spt_CuQuantity,
								lsk_CUQuantityFree	as spt_CuQuantityFree,
								StockPackHierarchyLevel
							
							INTO #UpdateLV_StockPackType
						
						FROM #LOGSTOCKDATA
						WHERE ParentItemUnitID=@ParentItemUnitID
						ORDER BY StockPackHierarchyLevel

					DECLARE @maxStockPackHierarchyLevel int = (select max(StockPackHierarchyLevel) from #UpdateLV_StockPackType),
							@curentStockPackHieranchuLevel int=1 ;

					WHILE @curentStockPackHieranchuLevel<=@maxStockPackHierarchyLevel
						BEGIN --WHILESTOCKPACKTYPE

							SELECT
								@spt_StockID		=	spt_stockID,
								@spt_ItemUnitID		=	spt_ItemUnitID,
								@spt_Quantity		=	case when @TranType=3 then 0				else spt_Quantity		end,
								@spt_QuantityFree	=	case when @TranType=3 then spt_Quantity		else spt_QuantityFree	end,
								@spt_CUQuantity		=	case when @TranType=3 then 0				else spt_CUQuantity		end,
								@spt_CUQuantityFree =	case when @TranType=3 then spt_CUQuantity	else spt_CuQuantityFree end 
							
							FROM #UpdateLV_StockPackType
							WHERE StockPackHierarchyLevel = @curentStockPackHieranchuLevel;


							/*select 
								@spt_StockID		as spt_StockID,
								@spt_ItemUnitID		as spt_ItemUnitID,
								@spt_Quantity		as spt_Quantity,
								@spt_QuantityFree	as spt_QuantityFree,
								@spt_CUQuantity		as spt_CUQuantity,
								@spt_CUQuantityFree	as spt_CUQuantityFree*/


							UPDATE LV_StockPackType
							
							SET	spt_Quantity		=	spt_Quantity + @spt_Quantity,
								spt_QuantityFree	=	spt_QuantityFree + @spt_QuantityFree,
								spt_CuQuantity		=	case when spt_CUQuantity	 is null then null else spt_CUQuantity		+  @spt_CUQuantity	   end ,
								spt_CUQuantityFree	=	case when spt_CUQuantityFree is null then null else spt_CUQuantityFree  +  @spt_CUQuantityFree end 

							WHERE spt_StockID=@spt_StockID and spt_ItemUnitID=@spt_ItemUnitID

							

							set @curentStockPackHieranchuLevel+=1
						END --WHILESTOCKPACKTYPE

						
						


			END--EXISTINGSTOCK


			--select  @ExistingStock

			IF @ExistingStock=0 and @lsk_Qty>0
			BEGIN -- NOT EXISTING STOCK
				SELECT TOP(1)
					@stockGroup			= StockGroup,
					@vstk_ID			= VirtualStkID,
					@stk_productID		= ProductID,
					@stk_CUQuantity		= lsk_CUQuantity,
					@stk_CuQuantityFree	= lsk_CUQuantityFree,
					@Stk_CUItemUnitID	= lsk_ItemUnitID,
					@stk_WeightQty		= lsk_WeightQty,
					@Stk_WeightUnitID	= lsk_WeightUnitID,
					@stk_LengthQty		= lsk_LengthQty,
					@Stk_LengthUnitID	= lsk_LengthUnitID,
					@Stk_AreaQty		= lsk_AreaQty,
					@Stk_AreaUnitID		= lsk_AreaUnitID,
					@Stk_VolumeQty		= lsk_VolumeQty,
					@Stk_VolumeUnitID	 = lsk_VolumeUnitID
				FROM  #LOGSTOCKDATA
				where StockPackHierarchyLevel=1 and ParentItemUnitID=@ParentItemUnitID
				order by rn,StockGroup

				/*SELECT 
					@stockGroup			,
					@vstk_ID			,
					@stk_productID		,
					@stk_CUQuantity		,
					@stk_CuQuantityFree	,
					@Stk_CUItemUnitID	,
					@stk_WeightQty		,
					@Stk_WeightUnitID	,
					@stk_LengthQty		,
					@Stk_LengthUnitID	,
					@Stk_AreaQty		,
					@Stk_AreaUnitID		,
					@Stk_VolumeQty		,
					@Stk_VolumeUnitID	*/

				DECLARE @st INT,@stkID int

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
						NULL,	--stk_LockedForDAFALED
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
				  DROP TABLE IF EXISTS #LogInsertLV_StockPackType;
				   
				   		select  VirtualStkID			as spt_StockID,
								Lsp_ItemUnitID			as spt_ItemUnitID,
								Lsp_Quantity			as spt_Quantity,
								Lsp_QuantityFree		as spt_QuantityFree,
								lsp_PackTypeRatio		as spt_PackTypeRatio,
								lsp_CUQuantity			as spt_CuQuantity,
								lsk_CUQuantityFree		as spt_CuQuantityFree,
								StockPackHierarchyLevel as StockPackHierarchyLevel
							INTO #LogInsertLV_StockPackType
						FROM #LOGSTOCKDATA
						
						WHERE ParentItemUnitID=@ParentItemUnitID
						ORDER BY StockPackHierarchyLevel


				DECLARE @ImaxStockPackHierarchyLevel	int = (select max(StockPackHierarchyLevel) from #LogInsertLV_StockPackType),
						@IcurentStockPackHieranchuLevel int	=	1 ;

				DECLARE @prevNewSPTID INT = NULL,@newSPTID int;

				WHILE @IcurentStockPackHieranchuLevel<=@ImaxStockPackHierarchyLevel
					BEGIN --WhileInsertStockPackType
					
						SELECT 
							@spt_ItemUnitID =	    spt_ItemUnitID,
							@spt_PackTypeRatio =	spt_PackTypeRatio,
							@spt_Quantity =			spt_Quantity,
							@spt_QuantityFree =		spt_QuantityFree,
							@spt_CUQuantity =		case when StockPackHierarchyLevel=1 then spt_CUQuantity else null end,
							@spt_CUQuantityFree =	case when StockPackHierarchyLevel=1 then spt_CUQuantityFree else null end
						FROM #LogInsertLV_StockPackType
						WHERE StockPackHierarchyLevel = @IcurentStockPackHieranchuLevel;

						SET @spt_ParentID = CASE WHEN @IcurentStockPackHieranchuLevel = 1 THEN NULL 
												ELSE @prevNewSPTID  END; --END CASE WHEN
						
						/*select 
						@spt_ItemUnitID 	,    
						@spt_PackTypeRatio 	,
						@spt_Quantity 	,		
						@spt_QuantityFree 	,	
						@spt_CUQuantity 	,	
						@spt_CUQuantityFree	*/
						
						EXEC dbo.sp_GetNextID
								@seqfield = N'spt_ID',
								@total    = 1,
								@result   = @newSPTID OUTPUT;
						

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
						SET @IcurentStockPackHieranchuLevel+=1

					END --WhileInsertStockPackType

					/*StockAttributes*/
                EXEC dbo.Li_InsertVirtualStockAttributes
                     @StockID   = @stkID,
                     @ProductID = @stk_productID;

			END -- NOT EXISTING STOCK



			DROP TABLE if exists #LOGSTOCKDATA;
		  SET @RowLoopData+= 1;
		END --LoopData
END

		
GO


