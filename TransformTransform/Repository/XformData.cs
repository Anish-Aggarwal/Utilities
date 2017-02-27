// -----------------------------------------------------------------------
// <copyright file="XformData.cs" company="SAPIENT">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace TransformTransform.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using TransformTransform.Models;
    using TransformTransform.DataBase;
    using System.Configuration;
    using System.Data;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class XformData
    {
        public List<XformRawSchema> GetXformRawSchemaNames()
        {
            var schmList = new List<XformRawSchema>();
            string sqlQuery = string.Format("SELECT [Id] ,[DbDetailId],[SchemaName],[SchemaType],[IsSystemGenerated],[HeaderRow],[IgnorableRows],[IgnorableRowsStartingWith],[Version],[ModifiedBy],[ModifiedDateTime],[PackageId],[SsisDbDetailId],[SourceDateFormatId],[SourceDateTimeFormatId],[IsActive],[SchemaTableName],[SchemaDescription]  FROM [XFormSvc].[dbo].[SchemaDetail]");
            var obj = AdoDbAccessor.ExecuteQuery(sqlQuery, ConfigurationManager.ConnectionStrings["Xform_connection_String"].ConnectionString);

            if (obj == null)
            {
                return null;
            }

            schmList.AddRange(from DataRow rowdata in obj.Rows
                              select new XformRawSchema
                              {
                                  Id = Int32.Parse(rowdata["Id"].ToString()),
                                  SchemaName = rowdata["SchemaName"].ToString()
                              });

            return schmList;
        }

        public List<SchemaColumnDetail> GetColumnNamesOnSchema(string schemaName)
        {
            var colmnList = new List<SchemaColumnDetail>();
            string sqlQuery = string.Format("select SCD.*,SD.SchemaName  from dbo.SchemaColumnDetail SCD join dbo.SchemaDetail SD on SCD.RawSchemaDetailId = SD.Id where SD.IsActive=1 AND SCD.IsSelected=1 AND SD.SchemaName='{0}'", schemaName);
            var obj = AdoDbAccessor.ExecuteQuery(sqlQuery, ConfigurationManager.ConnectionStrings["Xform_connection_String"].ConnectionString);

            if (obj == null)
            {
                return null;
            }

            colmnList.AddRange(from DataRow rowdata in obj.Rows
                               select new SchemaColumnDetail
                               {
                                   Id = Int32.Parse(rowdata["Id"].ToString()),
                                   ColumnDataType = Int32.Parse(rowdata["ColumnDataTypeId"].ToString()),
                                   ColumnName = rowdata["ColumnName"].ToString(),
                                   RawSchemaDetailId = Int32.Parse(rowdata["RawSchemaDetailId"].ToString()),
                                   SchemaName = rowdata["SchemaName"].ToString()
                               });

            return colmnList;

        }

        public List<XformTransformationDetail> GetXformTransformationDetails()
        {
            var colmnList = new List<XformTransformationDetail>();
            string sqlQuery = string.Format("SELECT [Id] ,[StepName] ,[Description] ,[SourceSliceId] ,[TransformationSliceId] ,[OrderSequence] ,[IsActive] ,[Version] ,[ModifiedBy] ,[ModifiedDateTime] ,[IsFilterEnable] ,[IsOrderEnable] ,[IsMappingEnable] ,[IsCorrelationEnable] ,[IsTargetFilterEnable]  FROM [XFormSvc].[dbo].[TransformationStepDetails] WHERE IsActive=1");
            var obj = AdoDbAccessor.ExecuteQuery(sqlQuery, ConfigurationManager.ConnectionStrings["Xform_connection_String"].ConnectionString);

            if (obj == null)
            {
                return null;
            }

            colmnList.AddRange(from DataRow rowdata in obj.Rows
                               select new XformTransformationDetail
                               {
                                   Id = Int32.Parse(rowdata["Id"].ToString()),
                                   StepName = rowdata["StepName"].ToString()
                               });

            return colmnList;
        }

        internal int InsertInXformMappingTable(XformTransformationDetail transformationStepdetail, string expressionText)
        {
            string sqlQuery = @"INSERT INTO [XFormSvc].[dbo].[XformMapping]
           ([TransformationStepId]
           ,[ExpressionText]
           ,[IsActive])
     VALUES
           ({0},'{1}',{2}); SELECT SCOPE_IDENTITY();";
            sqlQuery = string.Format(sqlQuery, transformationStepdetail.Id, expressionText, 1);
            //var outputId = AdoDbAccessor.ExecuteScalarQuery(sqlQuery, null, ConfigurationManager.ConnectionStrings["Xform_connection_String"].ConnectionString);
            var outputId = 1;
            return Convert.ToInt32(outputId);
        }

        internal int InsertinXformMappingFieldTable(int xformMappingId, int columnId, int IsActive)
        {
            string sqlQuery = @"INSERT INTO [XFormSvc].[dbo].[XformMappingField]
           ([XformMappingId]
           ,[SchemaColumnDetailId]
           ,[IsActive])
     VALUES
           ({0},{1},{2});";
            sqlQuery = string.Format(sqlQuery, xformMappingId, columnId, IsActive);
            //return AdoDbAccessor.ExecuteNonQuery(sqlQuery, ConfigurationManager.ConnectionStrings["Xform_connection_String"].ConnectionString, null);
            return 1;
        }
    }
}
