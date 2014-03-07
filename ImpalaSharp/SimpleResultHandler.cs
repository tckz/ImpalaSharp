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
    internal class SimpleResultHandler : ResultHandler<List<Dictionary<string, string>>>
    {

        public List<Dictionary<string, string>> GetResult(ResultsMetadata metadata)
        {
            return new List<Dictionary<string, string>>();
        }

        public void HandleResult(ResultsMetadata metadata, Results results, List<Dictionary<string, string>> result)
        {
            var splitter = new string[] { metadata.Delim };
            var rows = results.Data.Select(e =>
            {
                var dic = new Dictionary<string, string>();
                var fields = e.Split(splitter, StringSplitOptions.None);

                for (var i = 0; i < fields.Length; i++)
                {
                    var fieldsSchema = metadata.Schema.FieldSchemas[i];
                    dic[fieldsSchema.Name] = fields[i];
                }

                return dic;
            });

            result.AddRange(rows);
        }

    }
}
