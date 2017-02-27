// -----------------------------------------------------------------------
// <copyright file="CSVBuilderTransformationData.cs" company="SAPIENT">
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
    public class CSVBuilderTransformationDatainDB
    {
        public string Id { get; set; }

        public string RawDbStepName { get; set; }

        public string InterimName { get; set; }

        public string TransactSQL { get; set; }

        public string MetaData { get; set; }
    }
}
