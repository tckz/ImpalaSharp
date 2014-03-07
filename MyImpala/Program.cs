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
using System.Threading;
using System.Threading.Tasks;

using ImpalaSharp.Thrift.Beeswax;

using ImpalaSharp;

namespace MyImpala
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = "somehost.example.com";
            var port = 21000;

            var sql = @"select count(*) cc, somekey from sometable group by somekey";

            using (var impala = ImpalaClient.Connect(host, port))
            {
                {
                    var queryResult = impala.Query(sql);
                    queryResult.Result.ForEach(e =>
                    {
                        Console.WriteLine(@"{0}={1}", e["somekey"], e["cc"]);
                    });
                    Console.WriteLine(@"Elapsed: {0}", queryResult.ElapsedTime);
                }

                // Specify configuration each query.
                var conf = new Dictionary<string, string>()
                {
                    // e.g. "2g"
                    {"mem_limit", "-1"},
                };
                {
                    var queryResult = impala.Query(sql, conf);
                    Console.WriteLine(@"Elapsed: {0}", queryResult.ElapsedTime);
                }


                // Specific result handling.
                {
                    var queryResult = impala.Query(sql, conf, new MyHandler());
                    Console.WriteLine(@"Elapsed: {0}", queryResult.ElapsedTime);
                }
            }
        }

        class MyHandler : ResultHandler<List<MyRecord>>
        {
            public List<MyRecord> GetResult(ResultsMetadata metadata)
            {
                return new List<MyRecord>();
            }

            public void HandleResult(ResultsMetadata metadata, Results fetchedResults, List<MyRecord> result)
            {
                var splitter = new string[] { metadata.Delim };
                result.AddRange(fetchedResults.Data.Select(e =>
                {
                    var fields = e.Split(splitter, StringSplitOptions.None);

                    return new MyRecord()
                    {
                        Count = Convert.ToInt64(fields[0]),
                        Key = fields[1],
                    };
                }));
            }
        }

        class MyRecord
        {
            public long Count { get; set; }
            public string Key { get; set; }
        }
    }
}
