// -----------------------------------------------------------------------
// <copyright file="ApplicationConfigurationReader.cs" company="SAPIENT">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace TransformTransform
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Configuration;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class ApplicationConfigurationReader
    {
        private const int DefaultLongRunningCommandTimeoutInSecs = 1800;

        private static int? longRunningCommandTimeoutInSecs;

        public static int LongRunningCommandTimeoutInSecs
        {
            get
            {
                if (longRunningCommandTimeoutInSecs.HasValue == false)
                {
                    int tempLongRunningCommandTimeoutInSecs;
                    int.TryParse(ConfigurationManager.AppSettings["LONG_RUNNING_TRANSACTION_TIMEOUT"], out tempLongRunningCommandTimeoutInSecs);

                    if (tempLongRunningCommandTimeoutInSecs <= 0)
                    {
                        tempLongRunningCommandTimeoutInSecs = DefaultLongRunningCommandTimeoutInSecs;
                    }

                    longRunningCommandTimeoutInSecs = tempLongRunningCommandTimeoutInSecs;
                }

                return longRunningCommandTimeoutInSecs.GetValueOrDefault();
            }
        }
    }
}
