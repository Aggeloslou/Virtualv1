using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LiVirtualStock.Utillitys
{
    internal static class Utils
    {
        public static T DataTableToObj<T>(DataTable table)
        {
            try
            {
                var json = LiNSConverter.NewtonSoftUtils.SerializeObject(table);
                var result = LiNSConverter.NewtonSoftUtils.DeserializeObject<T>(json);
                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

    }
}
