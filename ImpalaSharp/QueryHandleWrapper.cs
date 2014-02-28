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
using ImpalaSharp.Thrift.Impala;

namespace ImpalaSharp
{
    internal class QueryHandleWrapper : IDisposable
    {
        private QueryHandle queryHandle;
        private ImpalaService.Client service;

        public QueryHandleWrapper(ImpalaService.Client svc, QueryHandle qh)
        {
            this.queryHandle = qh;
            this.service = svc;
        }

        public QueryHandle Handle { get { return this.queryHandle; } }

        public static implicit operator QueryHandle(QueryHandleWrapper s)
        {
            return s.queryHandle;
        }

        public TStatus Cancel()
        {
            lock (this)
            {
                if (this.queryHandle != null)
                {
                    var status = this.service.Cancel(this.queryHandle);
                    return status;
                }
                return null;
            }
        }


        #region IDisposable member

        void IDisposable.Dispose()
        {
            lock (this)
            {
                try
                {
                    this.service.close(this.queryHandle);
                }
                catch
                {
                    // ignore
                }
            }
            this.queryHandle = null;
            this.service = null;
        }

        #endregion
    }
}
