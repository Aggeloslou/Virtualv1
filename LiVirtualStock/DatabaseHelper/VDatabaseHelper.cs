using LiVirtualStock.DatabaseHelper;
using System.Data;
using System.Data.SqlClient;
using System.Threading;


namespace LiVirtualStock
{
    public static class VDatabaseHelper
    {
        public static void ExecuteQuery(string sql1)
        {
            
                    using (IDbConnection con = Mantis.LVision.DBAccess.DbRoutines.DBConnection.Open(VILivionForm.Args))
                    using (IDbTransaction tran = con.BeginTransaction())
                    {
                        var dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();
                        dbr.Execute(sql1, con, tran, VILivionForm.Args);
                        tran.Commit();
                    }
                

            
            
        }

        public static object ExecuteScalar(string sql2)
        {
              using (IDbConnection con = Mantis.LVision.DBAccess.DbRoutines.DBConnection.Open(VILivionForm.Args))
                    {
                        using (IDbTransaction tran = con.BeginTransaction())
                        {
                            var dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();
                            var result = dbr.SelectSingleValue(sql2, VILivionForm.Args, con, tran);
                            tran.Commit();
                            return result;

                        }
                    }
                
            
        }


        public static DataTable ExecuteSelectTable(string sql3)
        {
            using (IDbConnection con = Mantis.LVision.DBAccess.DbRoutines.DBConnection.Open(VILivionForm.Args))
                    using (IDbTransaction tran = con.BeginTransaction())
                    {
                        //try
                        //{
                        var dbr = new Mantis.LVision.DBAccess.DbRoutines.Routines();
                        var ds = dbr.SelectTable(sql3, VILivionForm.Args, con,tran);
                        tran.Commit();
                        return ds.Tables[0];
                        //}
                        //catch (Exception ex) 
                        //{ 
                        //   tran.Rollback(); 
                        //  throw ex; 
                        //}
                    }
                
            
        }



    }
}
