/*
 * Copyright 2014 tckz<at.tckz@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ImpalaSharp
{
    /// <summary>
    /// Container of result set and additional information.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryResult<T>
    {
        /// <summary>
        /// Detailed information about the last query.
        /// </summary>
        public string RuntimeProfile { get; set; }

        /// <summary>
        /// Result set of the query.
        /// </summary>
        public T Result { get; set; }

        /// <summary>
        /// Elapsed time from before run query to finished fetching.
        /// </summary>
        public TimeSpan ElapsedTime { get; set; }

        /// <summary>
        /// Elapsed time from before run query to finish of first result fetching.
        /// </summary>
        public TimeSpan QueryTime { get; set; }
    }
}

