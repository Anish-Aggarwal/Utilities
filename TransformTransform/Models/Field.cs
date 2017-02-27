// -----------------------------------------------------------------------
// <copyright file="Field.cs" company="SAPIENT">
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
    public class Field
    {
        public string Name { get; set; }
        public string Expression { get; set; }
        public string ExpressionType { get; set; }
        public Table Table { get; set; }
    }
}
