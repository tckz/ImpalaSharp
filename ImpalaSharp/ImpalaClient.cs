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
using System.Diagnostics;

using Thrift;
using Thrift.Transport;
using Thrift.Protocol;

using ImpalaSharp.Thrift.Impala;
using ImpalaSharp.Thrift.Beeswax;

namespace ImpalaSharp
{
    /// <summary>
    /// Connect and run query with impalad using Thrift.
    /// </summary>
    public class ImpalaClient : IDisposable
    {
        public string Host { get; private set; }
        public int Port { get; private set; }

        private readonly Disposer disposer = new Disposer();

        private TTransport transport = null;
        private ImpalaService.Client service = null;

        private ImpalaClient(string host, int port)
        {
            this.Host = host;
            this.Port = port;
        }

        /// <summary>
        /// Run query and handle results with SimpleResultHandler().
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        public QueryResult<List<Dictionary<string, object>>> Query(string q)
        {
            var ret = this.Query(q, new SimpleResultHandler());
            return ret;
        }

        /// <summary>
        /// Run query and handle results with specified handler.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="q"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public QueryResult<T> Query<T>(string q, ResultHandler<T> handler)
        {
            var ret = this.Query(q,
                (metadata) => handler.GetResult(metadata),
                (metadata, results, result) => handler.HandleResult(metadata, results, result)
            );

            return ret;
        }

        private QueryHandleWrapper currentQuery;

        public TStatus Cancel()
        {
            if (currentQuery != null)
            {
                return this.currentQuery.Cancel();
            }

            return null;
        }

        /// <summary>
        /// Run query on the conneciton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="q">Query text.</param>
        /// <param name="createResult">Function to instantiate the result object which is a member of QueryResult.</param>
        /// <param name="handleResults">Function to handle each Beeswax.Results and pack them into the result object.</param>
        /// <returns></returns>
        public QueryResult<T> Query<T>(string q, Func<ResultsMetadata, T> createResult, Action<ResultsMetadata, Results, T> handleResults)
        {
            var sw = new Stopwatch();
            sw.Start();

            using (var qh = this.CreateQueryHandle(q))
            {
                try
                {
                    this.currentQuery = qh;

                    this.WaitQueryStateFinished(qh);

                    var metadata = this.service.get_results_metadata(qh);

                    var result = new QueryResult<T>();
                    result.Result = createResult(metadata);
                    var firstResponse = true;
                    for (; ; )
                    {
                        var results = this.service.fetch(qh, false, 1024);
                        if (firstResponse)
                        {
                            result.QueryTime = sw.Elapsed;
                            firstResponse = false;
                        }

                        handleResults(metadata, results, result.Result);

                        if (!results.Has_more)
                        {
                            break;
                        }
                    }

                    var profile = this.service.GetRuntimeProfile(qh);
                    result.RuntimeProfile = profile;

                    result.ElapsedTime = sw.Elapsed;

                    return result;
                }
                finally
                {
                    this.currentQuery = null;
                }
            }

        }

        private void WaitQueryStateFinished(QueryHandleWrapper qh)
        {
            for (; ; )
            {
                var state = this.service.get_state(qh);
                if (state == QueryState.FINISHED)
                {
                    return ;
                }
                else if (state == QueryState.EXCEPTION)
                {
                    var log = this.service.get_log(qh.Handle.Log_context);
                    throw new TException("Error: " + log);
                }
                Thread.Sleep(100);
            }
        }

        protected void Connect()
        {
            var socket = new TSocket(this.Host, this.Port);
            this.disposer.Add(socket);

            this.transport = new TBufferedTransport(socket);
            this.disposer.Add(this.transport);
            this.transport.Open();

            var protocol = new TBinaryProtocol(this.transport);
            this.disposer.Add(protocol);

            this.service = new ImpalaService.Client(protocol);
            this.disposer.Add(this.service);
        }

        private QueryHandleWrapper CreateQueryHandle(string q)
        {
            var query = new Query();
            query.Query_string = q;
            var qh = this.service.query(query);
            return new QueryHandleWrapper(this.service, qh);
        }

        /// <summary>
        /// Instantiate ImpalaClient and connect it to the impalad.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        public static ImpalaClient Connect(string host, int port)
        {
            var ret = new ImpalaClient(host, port);
            ret.Connect();

            return ret;
        }



        #region IDisposable メンバー

        public void Dispose()
        {
            this.disposer.Dispose();
        }

        #endregion
    }
}
