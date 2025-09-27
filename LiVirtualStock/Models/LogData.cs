
using System.Data.SqlTypes;

namespace LiVirtualStock.Models
{
    public class LogStockData
    {
        public int? RowNum { get; set; }
        public int? LskLogID { get; set; }
        public int? LskToStockID { get; set; }
        public int? LskProductID { get; set; }
        public int? StockGroup { get; set; }

        public int? ParentItemUnitID { get; set; }
    }

    public class StockData
    {   
        public int? VirtualStkID { get; set; }
        public int? ProductID { get; set; }

        public int? HasStockAttributes { get; set; }

        public int? ParentItemUnitID { get; set; }

        public decimal? CUQuantity { get; set; }
        public decimal? CUQuantityFree { get; set; }
        public int? CUItemUnitID { get; set; }

        public decimal? WeightQty { get; set; }
        public int? WeightUnitID { get; set; }

        public decimal? LengthQty { get; set; }
        public int? LengthUnitID { get; set; }

        public decimal? AreaQty { get; set; }
        public int? AreaUnitID { get; set; }

        public decimal? VolumeQty { get; set; }
        public int? VolumeUnitID { get; set; }

        public int? forDeleted { get; set; }
    }

    public class StockPackTypeData
    {
        public int? SptStockID { get; set; }
        
        public decimal? SptQuantity { get; set; }
        
        public decimal? SptQuantityFree { get; set; }
        
        public int? SptItemUnitID { get; set; }

        public decimal? SptTypeRatio { get; set; }

        public decimal? SptCuQuantity { get; set; }

        public decimal? SptCuQuantityFree { get; set; }

        public int? StockPackHierarchyLevel { get; set; }

        public int? ParentItemUnitID { get; set; }

    }

    public class StockAttributes
    {
        public int? sav_ID { get; set; }

        public int? sav_StockID { get; set; }

        public int? sav_attributeID { get; set; }

        public string sav_Value { get; set; }

    }

}
