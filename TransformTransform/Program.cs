using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TransformTransform.Repository;
using System.Text.RegularExpressions;
using ConvertCSVBuilderToXForm;
using System.Speech.Synthesis;

namespace TransformTransform
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Tuple<string, string, string> trAssetOReportType;
        private static string rawtableName;
        private static char[] delimiters;
        private static SpeechSynthesizer synth;
        static void Main(string[] args)
        {
            delimiters = new char[] { ' ', '.', ',', '(', ')', '+' };
            synth = new SpeechSynthesizer();
            trAssetOReportType = FetchStrategyParams();
            MakeRawTableName(trAssetOReportType);

            MakeXFormMapping(trAssetOReportType);
            Console.ReadLine();
        }

        private static void MakeRawTableName(Tuple<string, string, string> trAssetOReportType)
        {
            rawtableName = string.Format("rawdb.PortFolio_Recon_{0}_{1}", trAssetOReportType.Item1.Replace('-', '_'), trAssetOReportType.Item2);
            if (!trAssetOReportType.Item3.Equals(string.Empty))
            {
                rawtableName = string.Format("rawdb.PortFolio_Recon_{0}_{1}_{2}", trAssetOReportType.Item1.Replace('-', '_'), trAssetOReportType.Item2, trAssetOReportType.Item3);
            }
        }

        private static Tuple<string, string, string> FetchStrategyParams()
        {
            Console.WriteLine("Input the AssetClass,TR and Origination Report Type in format AssetClass_TR_Origination Report Type");
            var input = Console.ReadLine();
            var array = input.Split('_');
            if (array.Count() == 2)
            {
                return new Tuple<string, string, string>(array[1], array[0], string.Empty);
            }

            return new Tuple<string, string, string>(array[1], array[0], array[2]);
        }

        public static void MakeXFormMapping(Tuple<string, string, string> trAssetOReportType)
        {
            var xformData = new XFormSvcDetails();
            var csvBuilderData = new CSVBuilderStrategyData();

            var transformationStepdetail = xformData.FetchTransformationStepDetailonKey(trAssetOReportType);
            var xformRawDbColumns = xformData.Fetchxformrawdbcolumnnames(trAssetOReportType);
            var xformInterimDbColumns = xformData.FetchxformInterimColumnNames(trAssetOReportType);
            var csvStrategyList = csvBuilderData.GetXMLTransformationonKey(trAssetOReportType);
            HandleSEWithoutAlias(transformationStepdetail, xformRawDbColumns, xformInterimDbColumns, csvStrategyList);

            // This method is not required as it will be taken care in with Alias method. It's there until the Alias method is solved
            //HandleCEWithOnetoOneMapping(transformationStepdetail, xformRawDbColumns, xformInterimDbColumns, csvStrategyList);

            HandleCEWithAlias(transformationStepdetail, xformRawDbColumns, xformInterimDbColumns, csvStrategyList);
        }

        private static void HandleCEWithAlias(Models.XformTransformationDetail transformationStepdetail, List<Models.SchemaColumnDetail> xformRawDbColumns, List<Models.SchemaColumnDetail> xformInterimDbColumns, Models.CSVBuilderTransformationListXML csvStrategyList)
        {
            var convertibleCSVStrategyList = csvStrategyList.MetaDataToStrategyList.Where(x => x.ExpressionButtonHeader.Equals("E") && !x.Field.Expression.ToLower().Contains("portrecon.")).ToList();

            List<string> columnadded = new List<string>();

            int nullcount = 0;
            int correctCount = 0;
            string expressionText = string.Empty;
            int xformMappingId = 0;
            bool ifconvertible;

            var xformData = new XformData();

            log.Info("----------------------------------------");
            log.Info("Handling of Complex Expression starting");
            synth.Speak("Handling of Complex Expression Starting");


            foreach (var item in convertibleCSVStrategyList)
            {
                List<string> csvrawdbColumns = MakeRawDbColumnFromExpression(item.Field.Expression, xformRawDbColumns);
                var csvInterimColumnName = GetRequiredFieldName(item.Alias);
                var xformInterimDBName = xformInterimDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvInterimColumnName, StringComparison.InvariantCultureIgnoreCase));

                if (xformInterimDBName != null && !columnadded.Contains(xformInterimDBName.SchemaName.ToString() + xformInterimDBName.ColumnName.ToString()) && csvrawdbColumns.Count > 0)
                {
                    string rawdbexpressionText = MakeComplexExpression(item.Field.Expression, xformRawDbColumns.FirstOrDefault().SchemaName, csvrawdbColumns, xformRawDbColumns, out ifconvertible);
                    if (ifconvertible == true)
                    {
                        expressionText = MakeExpressionTextforCE(xformInterimDBName, ConvertToXformColumnsCase(rawdbexpressionText, csvrawdbColumns, xformRawDbColumns));
                        xformMappingId = xformData.InsertInXformMappingTable(transformationStepdetail, expressionText);

                        log.InfoFormat("Expression: {0}", expressionText);
                        int outputofintermColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformInterimDBName.Id, 1);
                        if (outputofintermColumn != 0)
                        {
                            columnadded.Add(xformInterimDBName.SchemaName.ToString() + xformInterimDBName.ColumnName.ToString());

                            Console.WriteLine(string.Format("Value inserted for Interim {0}", xformInterimDBName.ColumnName));
                            log.InfoFormat("InterimColumn: {0}", xformInterimDBName.ColumnName);
                            correctCount = correctCount + 1;
                        }
                    }
                    else
                    {
                        continue;
                    }
                }

                foreach (var csvrawcolumn in csvrawdbColumns)
                {
                    var xformRawDBName = xformRawDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvrawcolumn, StringComparison.InvariantCultureIgnoreCase));
                    if (xformRawDBName == null || xformInterimDBName == null) { nullcount = nullcount + 1; }
                    if (xformRawDBName != null && xformInterimDBName != null
                        && !columnadded.Contains(xformInterimDBName.SchemaName.ToString() + xformInterimDBName.ColumnName.ToString() + xformRawDBName.SchemaName.ToString() + xformRawDBName.ColumnName.ToString()))
                    {
                        int outputofRawColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformRawDBName.Id, 1);
                        if (outputofRawColumn != 0)
                        {
                            columnadded.Add(xformInterimDBName.SchemaName.ToString() + xformInterimDBName.ColumnName.ToString() + xformRawDBName.SchemaName.ToString() + xformRawDBName.ColumnName.ToString());

                            Console.WriteLine(string.Format("Value inserted for RawColumn {0}", xformRawDBName.ColumnName));

                            log.InfoFormat("RawColumn: {0}", xformRawDBName.ColumnName);
                        }
                    }
                }

            }

            log.Info("Handling of Complex Expression Ending");
            log.Info("----------------------------------------");
            synth.Speak("Handling of Complex Expression Ending");

            Console.WriteLine("Null Count={0}, Correct Count={1}", nullcount, correctCount);
            columnadded = null;

        }

        private static string ConvertToXformColumnsCase(string rawdbexpressionText, List<string> rawdbColumns, List<Models.SchemaColumnDetail> xformRawdbcolumns)
        {
            string result = rawdbexpressionText;
            string schemaName = xformRawdbcolumns.FirstOrDefault().SchemaName;
            foreach (var item in rawdbColumns)
            {
                string replacevalue = xformRawdbcolumns.FirstOrDefault(x => x.ColumnName.Equals(item, StringComparison.InvariantCultureIgnoreCase)).ColumnName;
                result = result.Replace(schemaName + "." + item, "[" + schemaName + "." + replacevalue + "]");
            }

            return result;
        }

        private static string MakeExpressionTextforCE(Models.SchemaColumnDetail xformInterimDBName, string rawdbexpressionText)
        {
            string expression = "[{0}.{1}] = {2}";

            return string.Format(expression, xformInterimDBName.SchemaName, xformInterimDBName.ColumnName, rawdbexpressionText);
        }

        private static List<string> MakeRawDbColumnFromExpression(string csvBuilderExpression, List<Models.SchemaColumnDetail> xformRawDbColumns)
        {
            List<string> rawdbColumns = null;
            string inputString = Regex.Replace(csvBuilderExpression.ToLower(), @"\s+", " ").Replace(rawtableName.ToLower(), "");

            var expressionrawdbColumns = inputString.Split(delimiters).ToList();

            rawdbColumns = expressionrawdbColumns.Join(xformRawDbColumns.Select(x => x.ColumnName.ToLower()).ToList(), x => x, y => y, (a, b) => a).ToList();

            return rawdbColumns;
        }

        private static void HandleCEWithOnetoOneMapping(Models.XformTransformationDetail transformationStepdetail, List<Models.SchemaColumnDetail> xformRawDbColumns, List<Models.SchemaColumnDetail> xformInterimDbColumns, Models.CSVBuilderTransformationListXML csvStrategyList)
        {
            string csvExpressionforOnetoOneMapping = rawtableName;

            var convertibleCSVStrategyList = csvStrategyList.MetaDataToStrategyList.Where(x => x.ExpressionButtonHeader.Equals("E") && x.Field.Expression.Replace(csvExpressionforOnetoOneMapping.ToString() + ".", "").Equals(x.Alias, StringComparison.InvariantCultureIgnoreCase)).ToList();

            List<string> columnadded = new List<string>();

            int nullcount = 0;
            int correctCount = 0;
            var xformData = new XformData();
            foreach (var item in convertibleCSVStrategyList)
            {
                var csvrawdbColumn = GetRequiredFieldName(item.Field.Name);
                var csvInterimColumnName = GetRequiredFieldName(item.Alias);

                var xformRawDBName = xformRawDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvrawdbColumn, StringComparison.InvariantCultureIgnoreCase));
                var xformInterimDBName = xformInterimDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvInterimColumnName, StringComparison.InvariantCultureIgnoreCase));
                if (xformRawDBName == null || xformInterimDBName == null) { nullcount = nullcount + 1; }
                if (xformRawDBName != null && xformInterimDBName != null && !columnadded.Contains(xformInterimDBName.ColumnName))
                {
                    string expressionText = MakeExpressionText(xformRawDBName, xformInterimDBName);
                    int xformMappingId = xformData.InsertInXformMappingTable(transformationStepdetail, expressionText);

                    int outputofRawColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformRawDBName.Id, 1);
                    if (outputofRawColumn != 0)
                    {
                        log.InfoFormat("Expression {0}", expressionText);
                        log.InfoFormat("RawColumn: {0}", xformRawDBName.ColumnName);
                        Console.WriteLine(string.Format("Value inserted for RawColumn {0}", xformRawDBName.ColumnName));
                    }
                    int outputofintermColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformInterimDBName.Id, 1);
                    if (outputofintermColumn != 0)
                    {
                        Console.WriteLine(string.Format("Value inserted for Interim {0}", xformInterimDBName.ColumnName));
                        log.InfoFormat("InterimColumn: {0}", xformInterimDBName.ColumnName);
                    }
                    correctCount = correctCount + 1;
                    columnadded.Add(xformInterimDBName.ColumnName);
                }
            }

            Console.WriteLine("Null Count={0}, Correct Count={1}", nullcount, correctCount);
            columnadded = null;
        }

        private static string GetRequiredFieldName(string p)
        {
            // return Regex.Match(p, @"^[^\d]+").Value;
            return p;
        }

        private static void HandleSEWithoutAlias(Models.XformTransformationDetail transformationStepdetail, List<Models.SchemaColumnDetail> xformRawDbColumns, List<Models.SchemaColumnDetail> xformInterimDbColumns, Models.CSVBuilderTransformationListXML csvStrategyList)
        {
            var convertibleCSVStrategyList = csvStrategyList.MetaDataToStrategyList.Where(x => x.ExpressionButtonHeader.Equals("...")).ToList();

            List<string> columnadded = new List<string>();

            int nullcount = 0;
            int correctCount = 0;
            var xformData = new XformData();

            log.Info(" ");
            log.Info(" ");
            log.Info("----------------------------------------");
            log.Info("Handling of Simple Expression Starting");
            synth.Speak("Handling of Simple Expression Starting");

            foreach (var item in convertibleCSVStrategyList)
            {
                var csvrawdbColumn = GetRequiredFieldName(item.Field.Name);
                var csvInterimColumnName = GetRequiredFieldName(item.Alias ?? item.Field.Name);

                var xformRawDBName = xformRawDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvrawdbColumn, StringComparison.InvariantCultureIgnoreCase));

                var xformInterimDBName = xformInterimDbColumns.FirstOrDefault(x => x.ColumnName.Equals(csvInterimColumnName, StringComparison.InvariantCultureIgnoreCase));
                if (xformRawDBName == null || xformInterimDBName == null) { nullcount = nullcount + 1; }
                if (xformRawDBName != null && xformInterimDBName != null && !columnadded.Contains(xformInterimDBName.ColumnName))
                {
                    string expressionText = MakeExpressionText(xformRawDBName, xformInterimDBName);
                    int xformMappingId = xformData.InsertInXformMappingTable(transformationStepdetail, expressionText);

                    int outputofRawColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformRawDBName.Id, 1);
                    if (outputofRawColumn != 0)
                    {
                        Console.WriteLine(string.Format("Value inserted for RawColumn {0}", xformRawDBName.ColumnName));
                        log.InfoFormat("RawColumn: {0}", xformRawDBName.ColumnName);
                    }
                    int outputofintermColumn = xformData.InsertinXformMappingFieldTable(xformMappingId, xformInterimDBName.Id, 1);
                    if (outputofintermColumn != 0)
                    {
                        Console.WriteLine(string.Format("Value inserted for Interim {0}", xformInterimDBName.ColumnName));
                        log.InfoFormat("InterimColumn: {0}", xformInterimDBName.ColumnName);
                    }
                    correctCount = correctCount + 1;
                    columnadded.Add(xformInterimDBName.ColumnName);
                }
            }

            log.Info("Handling of Simple Expression Ending");
            log.Info("----------------------------------------");

            Console.WriteLine("Null Count={0}, Correct Count={1}", nullcount, correctCount);
            synth.Speak("Handling of Simple Expression Ending");
            columnadded = null;

        }

        private static string MakeExpressionText(Models.SchemaColumnDetail xformRawDBName, Models.SchemaColumnDetail xformInterimDBName)
        {
            string expression = "[{0}.{1}] = [{2}.{3}]";
            return string.Format(expression, xformInterimDBName.SchemaName, xformInterimDBName.ColumnName, xformRawDBName.SchemaName, xformRawDBName.ColumnName);
        }

        private static string MakeComplexExpression(string csvBuilderExpression, string xformSchemaName, List<string> rawdbColumns, List<Models.SchemaColumnDetail> xformRawdbcolumns, out bool ifConvertible)
        {
            ifConvertible = false;
            // string inputString = csvBuilderExpression.ToLower().Replace(rawtableName.ToLower() + ".", string.Empty);

            // inputString = rawdbColumns.Aggregate(inputString, (current, rawdbColumn) => current.Replace(rawdbColumn, rawtableName.ToLower().ToString() + "." + rawdbColumn));

            string inputString = csvBuilderExpression.ToLower();

            if (csvBuilderExpression.Equals(rawtableName.ToString() + "." + rawdbColumns.FirstOrDefault().ToString(), StringComparison.InvariantCultureIgnoreCase))
            {
                ifConvertible = true;
                return Regex.Replace(inputString, @"\s+", " ").Replace(rawtableName.ToLower(), xformSchemaName).Replace("'", "\"");
            }

            if (inputString.Contains(rawtableName.ToLower()))
            {
                inputString = Regex.Replace(inputString, @"\s+", " ").Replace(rawtableName.ToLower(), xformSchemaName).Replace("'", "\"");
            }
            else
            {
                inputString = Regex.Replace(inputString, @"\s+", " ");
                foreach (var r in rawdbColumns)
                {
                    inputString = inputString.Replace(r, xformSchemaName.ToLower() + "." + r).Replace("'", "\"");
                }
            }

            var arrayOfWords = inputString.Split(' ');
            string outPutstring = inputString;
            //if (inputString.Contains("case") && inputString.Contains("end"))
            //{
            //    outPutstring = ReplaceString(inputString);
            //}

            if (inputString.Contains("isnull"))
            {
                bool converted = false;
                outPutstring = ReplaceNullfunction(outPutstring, rawdbColumns, xformSchemaName, out converted);
                ifConvertible = converted;
            }

            if (inputString.Contains("regulatoridentifier"))
            {
                outPutstring = ReplaceToCorrectColumnfunction(outPutstring, xformSchemaName, "regulatoridentifier", "RegulatorName");
            }

            if (inputString.Contains("traderepository"))
            {
                outPutstring = ReplaceToCorrectColumnfunction(outPutstring, xformSchemaName, "traderepository", "TradeRepositoryName");
            }

            return outPutstring;
        }

        private static string ReplaceToCorrectColumnfunction(string outPutstring, string xformSchemaName, string valuetobeReplaced, string requiredValue)
        {
            return outPutstring.Replace(valuetobeReplaced, string.Format("{0}.{1}", xformSchemaName, requiredValue));
        }

        private static string ReplaceNullfunction(string sourceString, List<string> rawdbColumns, string xformSchemaName, out bool converted)
        {
            string input = sourceString;
            var output = input.Split(delimiters).ToList();

            // adding rawtable name to as it will be always available in the input expression
            var rawdbDelimsList = xformSchemaName.ToLower().Split(delimiters).ToList();
            var escapedFunctionNames = new List<string> { "isnull", "" };
            var result = output.Except(rawdbColumns).Except(rawdbDelimsList).Except(escapedFunctionNames).ToList();
            //foreach (var r in result)
            //{
            //    if (!r.Equals("isnull") && !(r.Equals("")))
            //    {
            //        return input;
            //    }
            //}

            if (result.Count > 0)
            {
                converted = false;
                log.InfoFormat("Raw Columns not present in Xform Raw but present in CSV Builder Raw are: {0}", string.Join<string>(",", result));
            }

            else
            {
                converted = true;
                input = string.Format("COALESCE({0})", string.Join<string>(",", rawdbColumns.Select(x => xformSchemaName + "." + x.ToString()).ToList()));
            }

            return input;
        }

        public static string ReplaceString(string sourceString)
        {
            Dictionary<string, string> sourceToDestination = new Dictionary<string, string>();
            sourceToDestination.Add("case when", "(");
            sourceToDestination.Add("end", ")");
            sourceToDestination.Add("then", " ? ");
            sourceToDestination.Add("when", " : ");
            sourceToDestination.Add("else", " : ");
            sourceToDestination.Add("\'", "\"");
            sourceToDestination.Add("=", "==");

            string destString = sourceString;
            foreach (var key in sourceToDestination.Keys)
            {
                sourceString = sourceString.Replace(key, sourceToDestination[key]);
            }

            return sourceString;
        }
    }
}
