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

using ImpalaSharp.Thrift.Beeswax;

namespace ImpalaSharp
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Type of QueryResult.Result</typeparam>
    public interface ResultHandler<T>
    {
        /// <summary>
        /// Return the result object which will be popullated by calling HandleResult().
        /// </summary>
        /// <param name="metadata"></param>
        /// <returns></returns>
        T GetResult(ResultsMetadata metadata);

        /// <summary>
        /// Method which populates the result object.
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="results">Results from beeswax which to be populated to the result object.</param>
        /// <param name="result"></param>
        void HandleResult(ResultsMetadata metadata, Results results, T result);
    }
}
