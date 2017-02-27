// -----------------------------------------------------------------------
// <copyright file="CSVBuilderStrategyData.cs" company="SAPIENT">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace TransformTransform.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Xml;
    using System.Text;
    using System.Data;
    using TransformTransform.DataBase;
    using System.Configuration;
    using TransformTransform.Models;
    using System.Xml.Linq;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class CSVBuilderStrategyData
    {
        public DataTable GetTransformationStrategySteponKey(Tuple<string, string, string> trAssetOReportType)
        {
            string filter = string.Format("PortRecon_{0}_{1}_Transformation", trAssetOReportType.Item1.Replace('-', '_'), trAssetOReportType.Item2);
            if (!trAssetOReportType.Item3.Equals(string.Empty))
            {
                filter = string.Format("PortRecon_{0}_{1}_{2}_Transformation", trAssetOReportType.Item1.Replace('-', '_'), trAssetOReportType.Item2, trAssetOReportType.Item3);
            }
            string sqlQuery = string.Format("SELECT [Id],[StepName],[TableViewName],[TransactSQL],MetaData FROM [CMRS_MASTER].[common].[TransformationStrategyStep] where StepName = '{0}'", filter);

            var obj = AdoDbAccessor.ExecuteQuery(sqlQuery, ConfigurationManager.ConnectionStrings["CMRS_MASTER"].ConnectionString);

            obj.TableName = "CSVTransformation";

            return obj;
        }

        public CSVBuilderTransformationDatainDB GetTransformationDatainDBonKey(Tuple<string, string, string> trAssetOReportType)
        {
            var transfdb = new List<CSVBuilderTransformationDatainDB>();
            var dataTable = this.GetTransformationStrategySteponKey(trAssetOReportType);
            if (dataTable == null || dataTable.Rows.Count == 0) { return null; }

            transfdb.AddRange(from DataRow r in dataTable.Rows
                              select new CSVBuilderTransformationDatainDB
                              {
                                  Id = r["Id"].ToString(),
                                  InterimName = r["TableViewName"].ToString(),
                                  RawDbStepName = r["StepName"].ToString(),
                                  TransactSQL = r["TransactSQL"].ToString(),
                                  MetaData = r["MetaData"].ToString()
                              });

            return transfdb.FirstOrDefault();
        }

        public CSVBuilderTransformationListXML GetXMLTransformationonKey(Tuple<string, string, string> trAssetOReportType)
        {
            var transfXML = new CSVBuilderTransformationListXML();
            var transfdb = this.GetTransformationDatainDBonKey(trAssetOReportType);
            if (transfdb == null) { return null; }

            transfXML.CSVBuilderTransformationDatainDB = transfdb;
            transfXML.MetaDataToStrategyList = this.ConvertMetaDataToListStrategy(transfdb.MetaData);


            return transfXML;
        }

        private List<TransformationStrategy> ConvertMetaDataToListStrategy(string metaData)
        {
            List<TransformationStrategy> strategyList = new List<TransformationStrategy>();
            var xmlDoc = XDocument.Parse(metaData);
            foreach (var item in xmlDoc.Descendants("ResultFieldViewEntity"))
            {
                var strategy = new TransformationStrategy();
                strategy.ExpressionButtonHeader = item.Element("ExpressionButtonHeader").Value.ToString();
                strategy.Alias = item.Element("Alias") == null ? null : item.Element("Alias").Value.ToString();
                strategy.Field = this.GetField(item.Element("Field"));
                strategyList.Add(strategy);
            }

            return strategyList;
        }

        private Field GetField(XElement xElement)
        {
            var field = new Field();
            field.Expression = xElement.Element("Expression").Value.ToString();
            field.ExpressionType = xElement.Element("Name").Value.ToString().Equals("Expression Field") ? "CASE" : null;
            field.Table = xElement.Element("Name").Value.ToString().Equals("Expression Field") ? null : this.GetTable(xElement.Element("Table"));
            field.Name = xElement.Element("Name").Value.ToString();
            return field;
        }

        private Table GetTable(XElement xElement)
        {
            var table = new Table();
            table.Name = xElement.Element("Name").Value.ToString();
            return table;
        }
    }
}
