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
        public string ServerVersion { get; private set; }

        private readonly ConnectionParameter connectionParameter;
        public ConnectionParameter ConnectionParameter
        {
            get
            {
                return this.connectionParameter;
            }
        }

        public int FetchSize { get; set; }

        private readonly Disposer disposer = new Disposer();

        private TTransport transport = null;
        private ImpalaService.Client service = null;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="param"></param>
        private ImpalaClient(ConnectionParameter param)
        {
            this.connectionParameter = param;
            this.FetchSize = 1024;
        }

        /// <summary>
        /// Run query and handle results using SimpleResultHandler().
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        public QueryResult<List<Dictionary<string, string>>> Query(string q)
        {
            return this.Query(q, new Dictionary<string, string>());
        }

        /// <summary>
        /// Run query with its configuration and handle results using SimpleResultHandler().
        /// </summary>
        /// <param name="q"></param>
        /// <param name="conf">Query configuration like {Key="mem_limit", Value="-1"}</param>
        /// <returns></returns>
        public QueryResult<List<Dictionary<string, string>>> Query(string q, Dictionary<string, string> conf)
        {
            var ret = this.Query(q, conf, new SimpleResultHandler());
            return ret;
        }

        /// <summary>
        /// Run query and handle results with specified handler.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="q"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public QueryResult<T> Query<T>(string q, Dictionary<string, string>conf, ResultHandler<T> handler)
        {
            var ret = this.Query(q, conf,
                (metadata) => handler.GetResult(metadata),
                (metadata, results, result) => handler.HandleResult(metadata, results, result)
            );

            return ret;
        }

        /// <summary>
        /// Get default configuration parameter list from the server.
        /// </summary>
        /// <param name="includeHadoop"></param>
        /// <returns></returns>
        public List<ConfigVariable> GetDefaultConfiguration(bool includeHadoop)
        {
            var ret =  this.service.get_default_configuration(false);
            return ret;
        }

        private QueryHandleWrapper currentQuery;

        public TStatus Cancel()
        {
            var qh = this.currentQuery;
            if (qh != null)
            {
                using (var anotherConnection = ImpalaClient.Connect(this.ConnectionParameter))
                {
                    var ret = anotherConnection.service.Cancel(qh);
                    return ret;
                }
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
        public QueryResult<T> Query<T>(string q, Dictionary<string, string>conf, Func<ResultsMetadata, T> createResult, Action<ResultsMetadata, Results, T> handleResults)
        {
            try
            {
                return this.QueryInternal(q, conf, createResult, handleResults);
            }
            catch (BeeswaxException ex)
            {
                throw new ImpalaException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Cast query and get results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="q"></param>
        /// <param name="conf"></param>
        /// <param name="createResult"></param>
        /// <param name="handleResults"></param>
        /// <returns></returns>
        private QueryResult<T> QueryInternal<T>(string q, Dictionary<string, string>conf, Func<ResultsMetadata, T> createResult, Action<ResultsMetadata, Results, T> handleResults)
        {
            var sw = Stopwatch.StartNew();

            using (var qh = this.CreateQueryHandle(q, conf))
            using (var cleaning = new DisposableAction(() => { this.currentQuery = null; }))
            {
                this.currentQuery = qh;

                this.WaitQueryStateFinished(qh, sw);

                var metadata = this.service.get_results_metadata(qh);

                var result = new QueryResult<T>();
                result.Result = createResult(metadata);
                var firstResponse = true;
                for (; ; )
                {
                    var results = this.service.fetch(qh, false, this.FetchSize);
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
        }

        /// <summary>
        /// Wait until query state became FINISHED.
        /// 
        /// If the state became EXCEPTION throw exception.
        /// </summary>
        /// <param name="qh">QueryHandle to wait</param>
        /// <param name="sw">StopWatch to get elapsed time since the query starts.</param>
        private void WaitQueryStateFinished(QueryHandleWrapper qh, Stopwatch sw)
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
                    throw new ImpalaException("Query aborted: " + log);
                }

                var msec = 1000;
                var elapsed = sw.ElapsedMilliseconds;
                if (elapsed < 10 * 1000)
                {
                    msec = 100;
                }
                else if (elapsed < 60 * 1000)
                {
                    msec = 500;
                }

                Thread.Sleep(msec);
            }
        }

        /// <summary>
        /// Connect to impalad and construct ImpalaService.Client.
        /// </summary>
        protected void Connect()
        {
            var socket = new TSocket(this.ConnectionParameter.Host, this.ConnectionParameter.Port);
            this.disposer.Add(socket);

            this.transport = new TBufferedTransport(socket);
            this.disposer.Add(this.transport);
            this.transport.Open();

            var protocol = new TBinaryProtocol(this.transport);
            this.disposer.Add(protocol);

            this.service = new ImpalaService.Client(protocol);
            this.disposer.Add(this.service);


            var pingResult = this.service.PingImpalaService();
            this.ServerVersion = pingResult.Version;
        }


        /// <summary>
        /// Create QueryHandle with query string and query configuration.
        /// </summary>
        /// <param name="q">Query string.</param>
        /// <param name="conf">Query configuration like {Key="mem_limit", Value="-1"}</param>
        /// <returns></returns>
        private QueryHandleWrapper CreateQueryHandle(string q, Dictionary<string, string>conf)
        {
            var query = new Query();
            query.Query_string = q;
            query.Configuration = conf.Select(e => e.Key + "=" + e.Value).ToList();
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
            var param = new ConnectionParameter()
            {
                Host = host,
                Port = port
            };

            return Connect(param);
        }

        /// <summary>
        /// Instantiate ImpalaClient and connect it to the impalad.
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        public static ImpalaClient Connect(ConnectionParameter param)
        {
            ImpalaClient client = null;
            try
            {
                client = new ImpalaClient(param);
                client.Connect();

                return client;
            }
            catch
            {
                client.Dispose();
                throw;
            }
        }



        #region IDisposable member

        public void Dispose()
        {
            this.disposer.Dispose();
        }

        #endregion
    }
}
