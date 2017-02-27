// -----------------------------------------------------------------------
// <copyright file="SchemaColumnDetail.cs" company="SAPIENT">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace TransformTransform.Models
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class SchemaColumnDetail
    {
        public int Id { get; set; }

        public string ColumnName { get; set; }

        public int RawSchemaDetailId { get; set; }

        public int ColumnDataType { get; set; }

        public string SchemaName { get; set; }
    }
}
