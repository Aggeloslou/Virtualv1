using LiVirtualStock.DatabaseHelper;
using LiVirtualStock.Models;
using LiVirtualStock.Repositories;
using Mantis.LVision.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;


namespace LiVirtualStock
{
    public static class VirtualStockService
    {
        public static object VDbExecutionInsertStockAttributes { get; private set; }

        public static (bool shouldExecute, int logID) ShouldExecuteTrigger(SchedulerCode.SchedulerTableChangedInfo e)
        {
            if (e?.Datasource == null || e.Datasource.Tables.Count == 0 || e.Datasource.Tables[0].Rows.Count == 0)
            {
                return (false, 0);
            }


            var m_dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();

            //var logStockID = e.Datasource.Tables[0].Rows[0]["lsp_logStockID"].ToString();

            //string sql = $@"
            //                SELECT  DISTINCT
            //                        lsk_logID as LogID,
            //                        log_transactionTypeID as TranType
            //                FROM lv_logStock with (nolock) 
            //                INNER JOIN Lv_Log with (nolock) ON log_id = lsk_logID
            //                WHERE lsk_id = {logStockID}";

            //var resultTable = m_dbr.SelectTable(sql, VILivionForm.Args);

            //var row = resultTable.Tables[0].Rows[0];

            var row = e.Datasource.Tables[0].Rows[0];
           // var logID = e.Datasource.Tables[0].Rows[0]["log_id"].ToString();


            if (!int.TryParse(row["log_id"]?.ToString(), out var logID))
                return (false, 0);

            if (!int.TryParse(row["log_transactionTypeID"]?.ToString(), out var tranType))
                return (false, 0);

            int[] validTranTypes = { 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 25, 27 };

            if (!validTranTypes.Contains(tranType))
                return (false, 0);

            return (true, logID);
        }


        public static void ExecuteVirtualStock(int logID)
        {
            var exec = new VListsData();

            var loopData = exec.LogStockData(Convert.ToInt32(logID));


            foreach (var row in loopData)
            {

                var LvStock = exec.StockData(Convert.ToInt32(row.LskLogID),
                                                     Convert.ToInt32(row.LskToStockID),
                                                     Convert.ToInt32(row.LskProductID),
                                                     Convert.ToInt32(row.ParentItemUnitID));
                foreach (var stk in LvStock)
                {


                    if (ShouldSkipStock(stk)==true)
                    {
                        continue;
                    }
                    else
                    {
                        HandleStock(row, stk);
                    }
                }
            }

        }



        private static void HandleStock(LogStockData row, StockData stk)
        {
            int ExistingStock = stk.VirtualStkID ?? 0;

            if (ExistingStock != 0) //Αν δεν υπάρχει το stock τότε insert 
            {
                ExistingStockExec(row, stk);
            }

            else // υπάρχει το stock τότε κάνε update
            {
                NonExistingStockExec(row, stk);
            }

        }

        private static void ExistingStockExec(LogStockData row, StockData stk)
        {

            if (stk.forDeleted == 1)
            {
                VDbExecution.DeleteStock(stk);

            }
            else
            {


                VDbExecution.UpdateLvStock(stk);



                var LvStockPackType = new VListsData().StockPackTypeData(Convert.ToInt32(row.LskLogID),
                                                                        Convert.ToInt32(row.LskToStockID),
                                                                        Convert.ToInt32(row.LskProductID),
                                                                        Convert.ToInt32(row.ParentItemUnitID));

                foreach (var spt in LvStockPackType.OrderBy(x => x.StockPackHierarchyLevel ?? int.MaxValue))
                {
                    VDbExecution.UpdateLvStockPackType(spt);
                }

            }

        }


        private static void NonExistingStockExec(LogStockData row, StockData stk)
        {

            var newStkId = VDbExecution.InsertLvStock(stk);



            int? parentId = null;

            var LvStockPackType = new VListsData().StockPackTypeData(Convert.ToInt32(row.LskLogID),
                                                                    Convert.ToInt32(row.LskToStockID),
                                                                    Convert.ToInt32(row.LskProductID),
                                                                    Convert.ToInt32(row.ParentItemUnitID));
            foreach (var inspt in LvStockPackType)
            {


                var newSptId = VDbExecution.InsertLvStockPackType(newStkId, parentId, inspt);

                parentId = newSptId;
            }

            var LvStockValues = new VListsData().StockAttributesData(newStkId, Convert.ToInt32(stk.ProductID));

            if (stk.HasStockAttributes == 1)
            {
                foreach (var sv in LvStockValues)
                {
                    VDbExecution.InsertStockAttributes(sv);
                }

            }
            else { return; }


        }


        private static bool ShouldSkipStock(StockData stk)
        {
            var qty = stk.CUQuantity ?? 0;
            var qtyFree = stk.CUQuantityFree ?? 0;
            var existing = stk.VirtualStkID ?? 0;

            return (qty < 0 && qtyFree < 0 && existing == 0)
                   || (qty == 0 && qtyFree == 0);
        }



    }
}
