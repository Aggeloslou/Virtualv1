using LiVirtualStock.Models;
using System;
using System.Data;
using System.Globalization;
using Mantis.LVision.Interfaces;

namespace LiVirtualStock
{
    public static class VDbExecution
    {


        public static void UpdateLvStock(StockData stk)
        {
            

            string sql = $@"
                            UPDATE LV_Stock WITH (ROWLOCK, UPDLOCK)
                            SET stk_CUQuantity     = stk_CUQuantity     + {ToSqlValue(stk.CUQuantity)},
                                stk_CUQuantityFree = stk_CUQuantityFree + {ToSqlValue(stk.CUQuantityFree)},
                                stk_WeightQty      = stk_WeightQty      + {ToSqlValue(stk.WeightQty ?? 0m)},
                                stk_LengthQty      = stk_LengthQty      + {ToSqlValue(stk.LengthQty ?? 0m)},
                                Stk_AreaQty        = Stk_AreaQty        + {ToSqlValue(stk.AreaQty ?? 0m)},
                                Stk_VolumeQty      = Stk_VolumeQty      + {ToSqlValue(stk.VolumeQty ?? 0m)}
                            
                            WHERE stk_ID =            {ToSqlValue(stk.VirtualStkID)} 
                              AND stk_ProductID    =  {ToSqlValue(stk.ProductID)} 
                              AND stk_CUItemUnitID =  {ToSqlValue(stk.CUItemUnitID)} 
                              AND isnull(stk_WeightUnitID,0) =  {ToSqlValue(stk.WeightUnitID ?? 0m)} 
                              AND isnull(stk_LengthUnitID,0) =  {ToSqlValue(stk.LengthUnitID ?? 0m)} 
                              AND isnull(Stk_AreaUnitID  ,0) =  {ToSqlValue(stk.AreaUnitID ?? 0m)} 
                              AND isnull(Stk_VolumeUnitID,0) =  {ToSqlValue(stk.VolumeUnitID ?? 0m)} ;";

            VDatabaseHelper.ExecuteQuery(sql);
        }


        public static void UpdateLvStockPackType(StockPackTypeData spt)
        {
          

            string sql = $@"
                            UPDATE LV_StockPackType WITH (ROWLOCK)
                            SET spt_Quantity       = spt_Quantity + {ToSqlValue(spt.SptQuantity)} ,
                                spt_QuantityFree   = spt_QuantityFree +  {ToSqlValue(spt.SptQuantityFree)} ,
                                spt_CUQuantity     = CASE WHEN spt_CUQuantity     IS NULL THEN NULL ELSE spt_CUQuantity     +  {ToSqlValue(spt.SptCuQuantity)}       END,
                                spt_CUQuantityFree = CASE WHEN spt_CUQuantityFree IS NULL THEN NULL ELSE spt_CUQuantityFree +  {ToSqlValue(spt.SptCuQuantityFree)}   END
                            WHERE spt_StockID  =  {ToSqlValue(spt.SptStockID)} 
                              AND isnull(spt_ItemUnitID,0) =  {ToSqlValue(spt.SptItemUnitID ?? 0m)};";

            VDatabaseHelper.ExecuteQuery(sql);
        }


        public static void DeleteStock(StockData stk)
        {
            
            string sql = $@"DELETE FROM LV_Stock WITH (ROWLOCK)  WHERE isnull(stk_ID,0) =  {ToSqlValue(stk.VirtualStkID ?? 0m)}";

            VDatabaseHelper.ExecuteQuery(sql);
          
        }


        public static int InsertLvStock(StockData stk)
        {
            var sql = $@"
                
                DECLARE @result int,@newID int
                                         EXEC  dbo.Li_GetNextID2
                                        		@seqfield = N'stk_ID',
                                                @total    = 1,
                                        		@result   = @result OUTPUT;
                                       -- select @result;
                
                INSERT INTO LV_Stock WITH (ROWLOCK)
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
                		@result,
                		605, -- LocationVirtual
                		{ToSqlValue(stk.ProductID)},
                		1	,	--Depositor,
                		18176,	-- ContainerVirtual
                		
                        {ToSqlValue(stk.CUItemUnitID)},
                		{ToSqlValue(stk.CUQuantity)},
                		{ToSqlValue(stk.CUQuantityFree)},
                		
                        NULL,	--stk_UnsuitReasonID, 
                		NULL,	--stk_ReserveReasonID,
                		NULL,	--stk_LockedForDAFALED
                		
                        {ToSqlValue(stk.LengthQty)},		     
                		{ToSqlValue(stk.LengthUnitID)},	    
                		{ToSqlValue(stk.AreaQty)},		     
                		{ToSqlValue(stk.AreaUnitID)},	    
                		{ToSqlValue(stk.VolumeQty)},		     
                		{ToSqlValue(stk.VolumeUnitID)},	    
                		{ToSqlValue(stk.WeightQty)},
                		{ToSqlValue(stk.WeightUnitID)},
                		
                		0,	  --stk_SplitStockInCUsLED
                		NULL, --stk_LocationSequence,
                		7,	  --stk_LogisticUnitID
                		1,	  --DomainID
                		NULL  --stk_UnitDeadWeight 
                		);
                select @result";
            var result=VDatabaseHelper.ExecuteScalar(sql);

            return Convert.ToInt32(result);
        }


        public static int InsertLvStockPackType(int stkID,  int? SptParentID, StockPackTypeData spt)
        {
            string sql = $@"
                             DECLARE @result int,@newID int 
                                         EXEC  dbo.Li_GetNextID2
                                        		@seqfield = N'spt_ID',
                                                @total    = 1,
                                        		@result   = @result OUTPUT;
                                       -- select @result;

                        INSERT INTO LV_StockPackType WITH (ROWLOCK)
                                                        (spt_ID,
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
                                                        ( @result,
                                                          {stkID},
                                                          {ToSqlValue(spt.SptItemUnitID)},
                                                          {ToSqlValue(spt.SptQuantity)},
                                                          {ToSqlValue(spt.SptQuantityFree)},
                                                          {ToSqlValue(spt.SptTypeRatio)},
                                                          {ToSqlValue(SptParentID)},
                                                          {ToSqlValue(spt.SptCuQuantity)},
                                                          {ToSqlValue(spt.SptCuQuantityFree)},
                                                          1
                                                        );
                                            select @result";

            var result = VDatabaseHelper.ExecuteScalar(sql);

            return Convert.ToInt32(result);
        }

        public static void InsertStockAttributes(StockAttributes sv) 
        {
            string sql = $@"INSERT INTO LV_StockAttributesValues WITH (ROWLOCK) (sav_ID, sav_StockID, sav_attributeID, sav_Value, sav_DomainID)
                             VALUES                                             ({ToSqlValue(sv.sav_ID)},
                                                                                 {ToSqlValue(sv.sav_StockID)},
                                                                                 {ToSqlValue(sv.sav_attributeID)},
                                                                                 {sv.sav_Value},1)";

            VDatabaseHelper.ExecuteQuery(sql);
        }

        private static string ToSqlValue(decimal? value)
        {
            return value?.ToString(CultureInfo.InvariantCulture) ?? "NULL";
        }

        private static string ToSqlValue(int? value)
        {
            return value?.ToString() ?? "NULL";
        }

      
    }
}
