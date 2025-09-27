using LiVirtualStock.DatabaseHelper;
using LiVirtualStock.Repositories;
using Mantis.LVision.Interfaces;
using System.Collections.Generic;

namespace LiVirtualStock
{
    public class VirtualStock : Mantis.LVision.Interfaces.SchedulerCode.ISchedulerCustom_2
    {
        public int ScheduledJobID { get; set; }
        public string EmailOnSuccess { get; set; }
        public string EmailOnFailure { get; set; }
        public SchedulerCode.ISchedulerCode Parent { get; set; }
        public string ScheduledJobCode { get; set; }
        public string ScheduledJobDescription { get; set; }
        public bool ConsoleMode { get; set; }
        public int SleepSeconds { get; set; }
        public List<string> Tables { get; set; }


        public void TableChanged(ILVisionForm args, SchedulerCode.SchedulerTableChangedInfo e)
        {
            VirtualStockCheck(args, e);
        }

        private void VirtualStockCheck(ILVisionForm args, SchedulerCode.SchedulerTableChangedInfo e)
        {

            VILivionForm.Args = args;

            var result = VirtualStockService.ShouldExecuteTrigger(e);

            if (!result.shouldExecute)
            {
                return;
            }
            else 
            {
                VirtualStockService.ExecuteVirtualStock(result.logID);
            }

                
        }



        public VirtualStock()
        {
          
            Tables = new List<string>();
            Tables.Add("LV_Log");
        }

    }

}


    

