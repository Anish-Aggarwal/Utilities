using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TransformTransform.Models;
using TransformTransform.Repository;

namespace ConvertCSVBuilderToXForm
{
    public class XFormSvcDetails
    {
        public XformTransformationDetail FetchTransformationStepDetailonKey(Tuple<string, string, string> trAssetOReportType)
        {
            string key;
            string longItem2 = this.ConvertShortToLongNames(trAssetOReportType.Item2);
            key = string.Format("Load{1}{0}Data", trAssetOReportType.Item1.Replace("-", ""), longItem2);

            if (trAssetOReportType.Item3 != string.Empty)
            {
                key = string.Format("Load{1}{0}{2}Data", trAssetOReportType.Item1.Replace("-", ""), longItem2, trAssetOReportType.Item3);
            }

            return new XformData().GetXformTransformationDetails().FirstOrDefault(x => x.StepName.Equals(key, StringComparison.InvariantCultureIgnoreCase));
        }

        public List<SchemaColumnDetail> Fetchxformrawdbcolumnnames(Tuple<string, string, string> trAssetOReportType)
        {
            var xformRepository = new XformData();
            string rawdbSchemaName = this.MakeRawDbSchemaName(trAssetOReportType);
            return xformRepository.GetColumnNamesOnSchema(rawdbSchemaName);
        }

        public List<SchemaColumnDetail> FetchxformInterimColumnNames(Tuple<string, string, string> trAssetOReportType)
        {
            var xformRepository = new XformData();
            string interimSchemaName = this.MakeInterimSchemaName(trAssetOReportType);
            return xformRepository.GetColumnNamesOnSchema(interimSchemaName);
        }

        private string MakeInterimSchemaName(Tuple<string, string, string> trAssetOReportType)
        {
            return string.Format("INTR_PortfolioData{0}", trAssetOReportType.Item2);
        }

        private string MakeRawDbSchemaName(Tuple<string, string, string> trAssetOReportType)
        {
            if (trAssetOReportType.Item3 == string.Empty)
            {
                return string.Format("{0}{1}", this.ConvertShortToLongNames(trAssetOReportType.Item2), trAssetOReportType.Item1.Replace("-", ""));
            }

            return string.Format("{0}{1}{2}", this.ConvertShortToLongNames(trAssetOReportType.Item2), trAssetOReportType.Item1.Replace("-", ""), trAssetOReportType.Item3);
        }

        private string ConvertShortToLongNames(string p)
        {
            string output = string.Empty;

            Dictionary<string, string> namesdict = new Dictionary<string, string>();
            namesdict.Add("IR", "InterestRates");
            namesdict.Add("EQ", "Equity");
            namesdict.Add("Commodity", "Commodity");
            namesdict.Add("FX", "FX");
            namesdict.Add("Credit", "Credit");
            namesdict.Add("ETD", "ETD");
            
            return namesdict.TryGetValue(p, out output) ? output : p;
        }
    }
}
