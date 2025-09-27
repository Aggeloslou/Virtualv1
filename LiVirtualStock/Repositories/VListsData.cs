using LiVirtualStock.Models;
using Mantis.LVision.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;


namespace LiVirtualStock.Repositories
{
    public  class VListsData
    {

        public List<LogStockData> LogStockData(int logID)
        {
            var listLogStockData = new List<LogStockData>();


            var sql = $@" SELECT 
	                            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) 	AS RowNum,
	                            lsk_LogID,
	                            lsk_ToStockID,
	                            lsk_ProductID,
		                        DENSE_RANK() OVER (PARTITION BY lsk_ToStockID ORDER BY ParentItemUnitID ) AS StockGroup,
                                ParentItemUnitID
	                    FROM ( SELECT DISTINCT 
	                    			lsk_LogID,
	                    			lsk_ToStockID,
	                    			lsk_ProductID,
	                    			MAX(CASE WHEN lsp_parentID IS NULL THEN lsp_ItemUnitID END)
	                    							OVER (PARTITION BY lsk_ToStockID,lsk_ID)  AS ParentItemUnitID	
	                    		FROM        LV_LogStock WITH(NOLOCK)
	                    		INNER  JOIN LV_LogStockPackType  WITH(NOLOCK) ON lsp_LogStockID = lsk_ID
	                    		WHERE lsk_LogID ={logID} and lsk_LocationID not in (/*3,*/593,605) ) A  --01RE01 01RIP01 VIRTUAL-- ";

            var TableTT = VDatabaseHelper.ExecuteSelectTable(sql);

            foreach (DataRow r in TableTT.Rows)
            {
                listLogStockData.Add(new LogStockData
                {
                    RowNum = Convert.ToInt32(r["RowNum"]),
                    LskLogID = Convert.ToInt32(r["lsk_LogID"]),
                    LskToStockID = Convert.ToInt32(r["lsk_ToStockID"]),
                    LskProductID = Convert.ToInt32(r["lsk_ProductID"]),
                    StockGroup = Convert.ToInt32(r["StockGroup"]),
                    ParentItemUnitID= Convert.ToInt32(r["ParentItemUnitID"])
                });
            }

            return listLogStockData;
        }

        public List<StockData> StockData(int curLogID, int curToStockID, int prdID, int ParentItemUnitID)
        {
            var listStockData = new List<StockData>();

            var sql = $@" SELECT         VirtualStkID                                                                 as VirtualStkID ,
                                         ProductID                                                                    as ProductID,
                                         case when TranTypeID=3 then 0	           else lsk_CUQuantity	end           as lsk_CUQuantity,
                                         case when TranTypeID=3 then lsk_CUQuantity else lsk_CUQuantityFree end       as lsk_CUQuantityFree,

                                         lsk_ItemUnitID                                                               as lsk_ItemUnitID,
                                         lsk_WeightQty                                                                as lsk_WeightQty,
                                         lsk_WeightUnitID                                                             as lsk_WeightUnitID,
                                         lsk_LengthQty                                                                as lsk_LengthQty,
                                         lsk_LengthUnitID                                                             as lsk_LengthUnitID,
                                         lsk_AreaQty                                                                  as lsk_AreaQty,
                                         lsk_AreaUnitID                                                               as lsk_AreaUnitID,
                                         lsk_VolumeQty                                                                as lsk_VolumeQty,
                                         lsk_VolumeUnitID                                                             as lsk_VolumeUnitID,
                                         ParentItemUnitID                                                             as ParentItemUnitID,
                                            forDelete                                                                 as forDelete,
                                          case 
                                                when exists (select 1 from LV_ProductStockAttributes where psa_ProductID=ProductID)
                                                then 1
                                                else 0  end as HasStockAttributes
                         FROM dbo.Li_al_StockData({curLogID},{curToStockID},{ParentItemUnitID},{prdID})
                         WHERE StockPackHierarchyLevel = 1 and ParentItemUnitID={ParentItemUnitID}
                         ORDER BY StockGroup,StockPackHierarchyLevel";

            var TableSTK = VDatabaseHelper.ExecuteSelectTable(sql);

            foreach (DataRow r in TableSTK.Rows)
            {
                listStockData.Add(new StockData
                {
                    VirtualStkID = r.Field<int?>("VirtualStkID"),
                    ProductID = r.Field<int?>("ProductID"),
                    HasStockAttributes = r.Field<int?>("HasStockAttributes"),

                    CUQuantity = r.Field<decimal?>("lsk_CUQuantity"),
                    CUQuantityFree = r.Field<decimal?>("lsk_CUQuantityFree"),
                    CUItemUnitID = r.Field<int?>("lsk_ItemUnitID"),

                    WeightQty = r.Field<decimal?>("lsk_WeightQty"),
                    WeightUnitID = r.Field<int?>("lsk_WeightUnitID"),

                    LengthQty = r.Field<decimal?>("lsk_LengthQty"),
                    LengthUnitID = r.Field<int?>("lsk_LengthUnitID"),

                    AreaQty = r.Field<decimal?>("lsk_AreaQty"),
                    AreaUnitID = r.Field<int?>("lsk_AreaUnitID"),

                    VolumeQty = r.Field<decimal?>("lsk_VolumeQty"),
                    VolumeUnitID = r.Field<int?>("lsk_VolumeUnitID"),
                    ParentItemUnitID= r.Field<int?>("ParentItemUnitID"),
                    forDeleted = r.Field<int?>("forDelete")
                });
            }

            return listStockData;
        }

        public List<StockPackTypeData> StockPackTypeData(int curLogID, int curToStockID, int prdID, int ParentItemUnitID)
        {

            var list = new List<StockPackTypeData>();

            var sql = $@"SELECT       VirtualStkID                                                          as spt_StockID,
                                      Lsp_ItemUnitID                                                        as spt_ItemUnitID,
                                      lsp_PackTypeRatio                                                     as spt_PackTypeRatio,
                                      case when TranTypeID=3 then 0 else Lsp_Quantity                 end   as spt_Quantity,
                                      case when TranTypeID=3 then Lsp_Quantity else Lsp_QuantityFree  end   as spt_QuantityFree,
                                      case when TranTypeID=3 then 0 else lsp_CUQuantity               end   as spt_CuQuantity,
                                      
                                      case when StockPackHierarchyLevel=1 then 
                                        case when TranTypeID=3 then lsp_CUQuantity else  lsk_CUQuantityFree end  
                                           else null  end    as spt_CuQuantityFree,

                                      StockPackHierarchyLevel ,
                                        ParentItemUnitID
                     FROM dbo.Li_al_StockData({curLogID},{curToStockID},{ParentItemUnitID},{prdID}) 
                     where ParentItemUnitID={ParentItemUnitID}
                     ORDER BY StockGroup,StockPackHierarchyLevel";

            var TableStockPackType = VDatabaseHelper.ExecuteSelectTable(sql);

            foreach (DataRow r in TableStockPackType.Rows)
            {
                
                var data = new StockPackTypeData
                {
                    SptStockID = r.Field<int?>("spt_StockID"),
                    SptItemUnitID = r.Field<int?>("spt_ItemUnitID"),
                    SptQuantity = r.Field<decimal?>("spt_Quantity"),
                    SptQuantityFree = r.Field<decimal?>("spt_QuantityFree"),
                    SptCuQuantity = r.Field<decimal?>("spt_CuQuantity"),
                    SptCuQuantityFree = r.Field<decimal?>("spt_CuQuantityFree"),
                    SptTypeRatio = r.Field<decimal?>("spt_PackTypeRatio"),
                    StockPackHierarchyLevel = (int?)r.Field<long?>("StockPackHierarchyLevel"),
                    ParentItemUnitID = r.Field<int?>("ParentItemUnitID")
                };
                list.Add(data);
                
               
            }


            return list;
        }

        public List<StockAttributes> StockAttributesData(int NewstockID, int prdID)
        {

            var list = new List<StockAttributes>();

            var sql = $@"  EXEC dbo.Li_InsertVirtualStockAttributes2 @sav_StockID={NewstockID}, @ProductID={prdID}";

            var TableStockPackType = VDatabaseHelper.ExecuteSelectTable(sql);

            foreach (DataRow r in TableStockPackType.Rows)
            {

                var data = new StockAttributes
                {
                    sav_ID = r.Field<int?>("sav_ID"),
                    sav_StockID = r.Field<int?>("sav_StockID"),
                    sav_attributeID = r.Field<int?>("sav_attributeID"),
                    sav_Value = r.Field<String>("VirtualStockValue")
                    
                };
                list.Add(data);


            }


            return list;
        }
    }
}
