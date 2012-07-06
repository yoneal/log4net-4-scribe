#region Apache License
//
// Licensed to the Apache Software Foundation (ASF) under one or more 
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership. 
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with 
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Transport;

using l4n = log4net;

namespace ScribeAppender.Test
{
    /// <summary>
    /// This is an example on how to use the ScribeAppender.
    /// Note that you only need to reference log4net, Thrift and ScribeAppender.
    /// There is no need to add the "com.facebook.scribe" source files if ScribeAppender is already referenced.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            // Initialize log4net
            l4n.Config.XmlConfigurator.Configure();

            // Create log
            var log = new LogEntry();
            log.Category = "Program";
            log.Message = "This is a test error message from the program";

            // Connect
            var socket = new TSocket("192.168.1.144",65510,300);
            var transport = new TFramedTransport(socket);
            var protocol = new TBinaryProtocol(transport,false,false);
            var scribeClient = new scribe.Client(protocol);
            transport.Open();

            // Send
            var logs = new List<LogEntry>();
            logs.Add(log);
            var result = scribeClient.Log(logs);

            // Close
            transport.Close();

            // use log4net to log
            var logger = l4n.LogManager.GetLogger("ScribeAppender.Test.Program");
            logger.Debug("This is a test error message from the logger");
        }
    }
}
