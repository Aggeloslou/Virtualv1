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

            var row = e.Datasource.Tables[0].Rows[0];


            int logID = Convert.ToInt32(e.Datasource.Tables[0].Rows[0]["log_id"].ToString());

            int TranTypeID = Convert.ToInt32(e.Datasource.Tables[0].Rows[0]["log_transactionTypeID"].ToString());


            int[] ValidTranTypeID = { 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 25, 27 }; //Παραλαβή,Συλλογη,Τροποποίηση Συλλογής,Διαμόρφωση Αποθέματος,
                                                                                 //Διαγραφή Αποθέματος,Ενδοδιακίνηση,ΔΑΦΑ,Φορτωση,
                                                                                 //Εξαγωγή,Επιστροφή,Αποφορτωσή,Ακύρωση Δεσμέυσης

            if (!ValidTranTypeID.Contains(TranTypeID))
            {
                return (false, 0);
            }

            return (true, logID);
        }


        public static void ExecuteVirtualStock(ILVisionForm args, int logID)
        {


            var exec = new VListsData();

            var LogStockData = exec.LogStockData(Convert.ToInt32(logID));


            int index = 0;
            while (index < LogStockData.Count)
            {
                //var m_dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();

                //string sql = $@" select dbo.Li_GetVstockForLog({ls.LskLogID},{ls.LskToStockID},{ls.ParentItemUnitID},{ls.LskProductID})";

                //object virtualStockObj = m_dbr.SelectSingleValue(sql, args);

                //int? virtualStock = virtualStockObj as int?;

                var ls = LogStockData[index];

                if ((ls.virtualStock ?? 0) != 0) //Υπαρχει το Stock  Η Delete ή Update
                {
                    if ((ls.StockforDelete ?? 0) == 1)
                    {
                        VDbExecution.DeleteStock(Convert.ToInt32(ls.LskLogID),
                                                Convert.ToInt32(ls.LskToStockID),
                                                Convert.ToInt32(ls.virtualStock));
                    }
                    else
                    {
                        VDbExecution.UpdateStockData(Convert.ToInt32(ls.LskLogID),
                                                Convert.ToInt32(ls.LskToStockID),
                                                Convert.ToInt32(ls.ParentItemUnitID),
                                                Convert.ToInt32(ls.LskProductID));

                    }

                }
                else
                {

                    if ((ls.virtualStock ?? 0) == 0 && ls.CUQuantity > 0) //Δεν υπαρχεί το Stock και πρεπει να γίνει insert
                    {
                        VDbExecution.InsertStockData(Convert.ToInt32(ls.LskLogID),
                                                    Convert.ToInt32(ls.LskToStockID),
                                                    Convert.ToInt32(ls.ParentItemUnitID),
                                                    Convert.ToInt32(ls.LskProductID));


                    }
                }
                LogStockData = exec.LogStockData(Convert.ToInt32(logID));
                index++;
            }

        }


        public static void ExecuteVirtualStock2(ILVisionForm args, int logID)
        {


            var exec = new VListsData();

            var LogStockData = exec.LogStockData(Convert.ToInt32(logID));

            

            //int index = 0;
            foreach (var ls in LogStockData)
            {
                LogStockData = exec.LogStockData(Convert.ToInt32(logID));
                //var m_dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();

                //string sql = $@" select dbo.Li_GetVstockForLog({ls.LskLogID},{ls.LskToStockID},{ls.ParentItemUnitID},{ls.LskProductID})";

                //object virtualStockObj = m_dbr.SelectSingleValue(sql, args);

                //int? virtualStock = virtualStockObj as int?;

                //var ls = LogStockData[index];

                if ((ls.virtualStock ?? 0) != 0) //Υπαρχει το Stock  Η Delete ή Update
                {
                    if ((ls.StockforDelete ?? 0) == 1)
                    {
                        VDbExecution.DeleteStock(Convert.ToInt32(ls.LskLogID),
                                                Convert.ToInt32(ls.LskToStockID),
                                                Convert.ToInt32(ls.virtualStock));
                    }
                    else
                    {
                        VDbExecution.UpdateStockData(Convert.ToInt32(ls.LskLogID),
                                                Convert.ToInt32(ls.LskToStockID),
                                                Convert.ToInt32(ls.ParentItemUnitID),
                                                Convert.ToInt32(ls.LskProductID));

                    }

                }
                else
                {

                    if ((ls.virtualStock ?? 0) == 0 && ls.CUQuantity > 0) //Δεν υπαρχεί το Stock και πρεπει να γίνει insert
                    {
                        VDbExecution.InsertStockData(Convert.ToInt32(ls.LskLogID),
                                                    Convert.ToInt32(ls.LskToStockID),
                                                    Convert.ToInt32(ls.ParentItemUnitID),
                                                    Convert.ToInt32(ls.LskProductID));


                    }
                }
                
                //index++;
            }

        }




        public static void ExecuteVirtualStock3(ILVisionForm args, int logID)
        {
            var exec = new VListsData();

            // Βρίσκω το μέγιστο RowNum από την πρώτη λίστα
            var logStockData = exec.LogStockData(logID);
            int maxRow = (int)logStockData.Max(x => x.RowNum);

            for (int row = 1; row <= maxRow; row++)
            {
                // Φρεσκάρουμε πάντα τη λίστα
                logStockData = exec.LogStockData(logID);

                // Βρίσκουμε το τρέχον row
                var ls = logStockData.FirstOrDefault(x => x.RowNum == row);
                if (ls == null) continue; // μπορεί να έχει σβηστεί ή αλλάξει

                if ((ls.virtualStock ?? 0) != 0) // Υπάρχει Stock → Delete ή Update
                {
                    if ((ls.StockforDelete ?? 0) == 1)
                    {
                        VDbExecution.DeleteStock(Convert.ToInt32(ls.LskLogID),
                                            Convert.ToInt32(ls.LskToStockID),
                                            Convert.ToInt32(ls.virtualStock));
                    }
                    else
                    {
                        VDbExecution.UpdateStockData(Convert.ToInt32(ls.LskLogID),
                                                Convert.ToInt32(ls.LskToStockID),
                                                Convert.ToInt32(ls.ParentItemUnitID),
                                                Convert.ToInt32(ls.LskProductID));
                    }
                }
                else
                {
                    if ((ls.virtualStock ?? 0) == 0 && ls.CUQuantity > 0)
                    {
                        VDbExecution.InsertStockData(Convert.ToInt32(ls.LskLogID),
                                                    Convert.ToInt32(ls.LskToStockID),
                                                    Convert.ToInt32(ls.ParentItemUnitID),
                                                    Convert.ToInt32(ls.LskProductID));

                    }
                }
            }
        }


    }
}
