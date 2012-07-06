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
using System.Globalization;
using System.Net;
using System.Net.Sockets;

using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Transport;

using log4net.Layout;
using log4net.Core;
using log4net.Util;

namespace log4net.Appender
{
    /// <summary>
    /// Sends logging events to a Scribe server.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For more information regarding Scribe, please refer to the Scribe project page: https://github.com/facebook/scribe/.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code lang="XML" escaped="true">
    /// <appender name="ScribeAppender" type="log4net.Appender.ScribeAppender, ScribeAppender">
    ///     <remoteHost value="ScribeServer" />
    ///     <remotePort value="8080" />
    ///     <scribeTimeout value="200" />
    ///     <category value="Example" />
    ///     <layout type="log4net.Layout.PatternLayout" value="%-5level %logger [%ndc] - %message%newline" />
    /// </appender>
    /// </code>
    /// </example>
    /// <author>Neal Sebastian</author>
    public class ScribeAppender : AppenderSkeleton
    {
        #region Public Instance Constructors

		/// <summary>
		/// Initializes a new instance of the <see cref="ScribeAppender" /> class.
		/// </summary>
		/// <remarks>
		/// The default constructor initializes all fields to their default values.
		/// </remarks>
        public ScribeAppender() 
		{

		}

		#endregion Public Instance Constructors

        #region Public Instance Properties

        /// <summary>
        /// Gets or sets the hostname or IP address of the Scribe server to which the logging event should be sent.
        /// </summary>
        /// <value>
        /// The hostname or IP address of the Scribe server.
        /// </value>
        public string RemoteHost
        {
            get { return remoteHost; }
            set { remoteHost = value; }
        }

        /// <summary>
        /// Gets or sets the TCP port number of the Scribe server to which the logging event should be sent.
        /// </summary>
        /// <value>
        /// An integer value in the range <see cref="IPEndPoint.MinPort" /> to <see cref="IPEndPoint.MaxPort" /> 
		/// indicating the TCP port number of the remote host or multicast group to which the logging event 
		/// will be sent.
        /// </value>
        public int RemotePort
        {
            get { return remotePort; }
            set
            {
                if (value < IPEndPoint.MinPort || value > IPEndPoint.MaxPort)
                {
                    throw log4net.Util.SystemInfo.CreateArgumentOutOfRangeException("value", (object)value,
                        "The value specified is less than " +
                        IPEndPoint.MinPort.ToString(NumberFormatInfo.InvariantInfo) +
                        " or greater than " +
                        IPEndPoint.MaxPort.ToString(NumberFormatInfo.InvariantInfo) + ".");
                }
                else
                {
                    remotePort = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the time interval to determine if the connecetion to the Scribe server is lost.
        /// </summary>
        /// <value>
        /// An integer value in the range 1 to <see cref="Int32.MaxValue" /> 
        /// </value>
        public int ScribeTimeout
        {
            get { return scribeTimeout; }
            set 
            {
                if (value < 0)
                {
                    throw log4net.Util.SystemInfo.CreateArgumentOutOfRangeException("value", (object)value,
                        "The value specified is less than 0.");
                }
            }
        }

        /// <summary>
        /// Gets or sets the category of the log message
        /// </summary>
        /// <value>
        /// Any string.
        /// </value>
        public string Category
        {
            get { return this.category; }
            set { this.category = value; }
        }

        /// <summary>
        /// RFU
        /// </summary>
        public Encoding Encoding
        {
            get { return messageEncoding; }
            set { messageEncoding = value; }
        }

        #endregion Public Instance Properties

        #region Protected Instance Properties

        /// <summary>
        /// Gets or sets the underlying <see cref="TSocket" />
        /// </summary>
        protected TSocket ThriftSocket
        {
            get { return this.thriftSocket; }
            set { this.thriftSocket = value; }
        }

        /// <summary>
        /// Gets or sets the underlying <see cref="TFramedTransport" />
        /// </summary>
        protected TFramedTransport ThriftFramedTransport
        {
            get { return this.thriftFramedTransport; }
            set { this.thriftFramedTransport = value; }
        }

        /// <summary>
        /// Gets or sets the underlying <see cref="ThriftBinaryProtocol" />
        /// </summary>
        protected TBinaryProtocol ThriftBinaryProtocol
        {
            get { return this.thriftBinaryProtocol; }
            set { this.thriftBinaryProtocol = value; }
        }

        /// <summary>
        /// Gets or sets the underlying <see cref="scribe.Client" />
        /// </summary>
        protected scribe.Client ScribeClient
        {
            get { return this.scribeClient; }
            set { this.scribeClient = value; }
        }

        #endregion Protected Instance Properties

        #region Private Instance Fields

        /// <summary>
        /// The IP address of the remote host or multicast group to which 
        /// the logging event will be sent.
        /// </summary>
        private string remoteHost;

        /// <summary>
        /// The TCP port number of the remote host or multicast group to 
        /// which the logging event will be sent.
        /// </summary>
        private int remotePort;

        /// <summary>
        /// Default timeout if <see cref="scribeTimeout" /> is not specified in the configuration file.
        /// </summary>
        private const int defaultTimeout = 300;
        /// <summary>
        /// The timeout used by the internal Scribe client
        /// </summary>
        private int scribeTimeout = defaultTimeout;

        /// <summary>
        /// The underlying <see cref="TSocket" /> used by the internal Scribe client
        /// </summary>
        private TSocket thriftSocket;

        /// <summary>
        /// The underlying <see cref="TFramedTransport" /> used by the internal Scribe client
        /// </summary>
        private TFramedTransport thriftFramedTransport;

        /// <summary>
        /// The underlying <see cref="TBinaryProtocol" /> used by the internal Scribe client
        /// </summary>
        private TBinaryProtocol thriftBinaryProtocol;

        /// <summary>
        /// The internal <see cref="scribe.Client" />
        /// </summary>
        private scribe.Client scribeClient;

        /// <summary>
        /// The category of the logging event.
        /// </summary>
        private string category;

        /// <summary>
        /// The encoding to use for the message.
        /// </summary>
        private Encoding messageEncoding = Encoding.Default;

        #endregion Private Instance Fields

        #region Implementation of IOptionHandler

        /// <summary>
        /// Initialize the appender based on the options set.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is part of the <see cref="IOptionHandler"/> delayed object
        /// activation scheme. The <see cref="ActivateOptions"/> method must 
        /// be called on this object after the configuration properties have
        /// been set. Until <see cref="ActivateOptions"/> is called this
        /// object is in an undefined state and must not be used. 
        /// </para>
        /// <para>
        /// If any of the configuration properties are modified then 
        /// <see cref="ActivateOptions"/> must be called again.
        /// </para>
        /// <para>
        /// The appender will be ignored if no <see cref="RemoteHost" /> was specified or 
        /// an invalid remote TCP port number was specified.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">The required property <see cref="RemoteHost" /> was not specified.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The TCP port number assigned to <see cref="RemotePort" /> is less than <see cref="IPEndPoint.MinPort" /> or greater than <see cref="IPEndPoint.MaxPort" />.</exception>
        public override void ActivateOptions()
		{
			base.ActivateOptions();

			if (this.RemoteHost == null) 
			{
				throw new ArgumentNullException("The required property 'RemoteHost' was not specified.");
			} 
			else if (this.RemotePort < IPEndPoint.MinPort || this.RemotePort > IPEndPoint.MaxPort) 
			{
				throw log4net.Util.SystemInfo.CreateArgumentOutOfRangeException("this.RemotePort", (object)this.RemotePort,
					"The RemotePort is less than " + 
					IPEndPoint.MinPort.ToString(NumberFormatInfo.InvariantInfo) + 
					" or greater than " + 
					IPEndPoint.MaxPort.ToString(NumberFormatInfo.InvariantInfo) + ".");
			}
            else if (this.Category == null)
            {
                throw new ArgumentNullException("The required property 'Category' was not specified.");
            }
			else 
			{
				this.InitializeClientConnection();
			}
        }

        #endregion Implementation of IOptionHandler

        #region Override implementation of AppenderSkeleton

        /// <summary>
        /// This method is called by the <see cref="AppenderSkeleton.DoAppend(LoggingEvent)"/> method.
        /// </summary>
        /// <param name="loggingEvent">The event to log.</param>
        /// <remarks>
        /// <para>
        /// Sends the event to a Scribe server.
        /// </para>
        /// <para>
        /// Exceptions are passed to the <see cref="AppenderSkeleton.ErrorHandler"/>.
        /// </para>
        /// </remarks>
        protected override void Append(LoggingEvent loggingEvent)
        {
            try
            {
                var log = new LogEntry();
                log.Category = this.category;
                log.Message = RenderLoggingEvent(loggingEvent);
                var logs = new List<LogEntry>();
                logs.Add(log);
                var result = this.ScribeClient.Log(logs);
                if (result != ResultCode.OK)
                {
                    throw new ApplicationException("Scribe Log() returned " + result.ToString() + ".");
                }
            }
            catch (Exception ex)
            {
                ErrorHandler.Error(
                    "Unable to send logging event to remote host " +
                    this.RemoteHost +
                    " on port " +
                    this.RemotePort + ".",
                    ex,
                    ErrorCode.WriteFailure);
            }
        }

        /// <summary>
        /// This method is called by the <see cref="AppenderSkeleton.DoAppend(LoggingEvent[])"/> method.
        /// </summary>
        /// <param name="loggingEvent">The event to log.</param>
        /// <remarks>
        /// <para>
        /// Sends the event to a Scribe server.
        /// </para>
        /// <para>
        /// Exceptions are passed to the <see cref="AppenderSkeleton.ErrorHandler"/>.
        /// </para>
        /// </remarks>
        protected override void Append(LoggingEvent[] loggingEvents)
        {
            try
            {
                var logs = new List<LogEntry>();
                foreach (LoggingEvent le in loggingEvents)
                {
                    var log = new LogEntry();
                    log.Category = this.Category;
                    log.Message = RenderLoggingEvent(le);
                    logs.Add(log);
                }
                var result = this.ScribeClient.Log(logs);
                if (result != ResultCode.OK)
                {
                    throw new ApplicationException("Scribe Log() returned " + result.ToString() + ".");
                }
            }
            catch (Exception ex)
            {
                ErrorHandler.Error(
                    "Unable to send logging event to remote host " +
                    this.RemoteHost +
                    " on port " +
                    this.RemotePort + ".",
                    ex,
                    ErrorCode.WriteFailure);
            }
        }

        /// <summary>
        /// This appender requires a <see cref="Layout"/> to be set.
        /// </summary>
        /// <value><c>true</c></value>
        /// <remarks>
        /// <para>
        /// This appender requires a <see cref="Layout"/> to be set.
        /// </para>
        /// </remarks>
        override protected bool RequiresLayout
        {
            get { return true; }
        }

        /// <summary>
        /// Closes the Scribe connection and releases all resources associated with 
        /// this <see cref="ScribeAppender" /> instance.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Disables the underlying <see cref="scribe.Client" /> and releases all managed 
        /// and unmanaged resources associated with the <see cref="ScribeAppender" />.
        /// </para>
        /// </remarks>
        override protected void OnClose()
        {
            base.OnClose();

            if (this.ScribeClient != null)
            {
                this.ThriftFramedTransport.Close();
                this.ThriftSocket = null;
                this.ThriftFramedTransport = null;
                this.ThriftBinaryProtocol = null;
                this.ScribeClient = null;
            }
        }

        #endregion Override implementation of AppenderSkeleton

        #region Protected Instance Methods

        /// <summary>
        /// Initializes the underlying  <see cref="scribe.Client" /> connection.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The underlying <see cref="scribe.Client"/> is initialized and binds to the 
        /// port number from which you intend to communicate.
        /// </para>
        /// <para>
        /// Exceptions are passed to the <see cref="AppenderSkeleton.ErrorHandler"/>.
        /// </para>
        /// </remarks>
        protected virtual void InitializeClientConnection()
        {
            try
            {
                this.ThriftSocket = new TSocket(this.RemoteHost, this.RemotePort, this.ScribeTimeout);
                this.ThriftFramedTransport = new TFramedTransport(this.ThriftSocket);
                this.ThriftBinaryProtocol = new TBinaryProtocol(this.ThriftFramedTransport, false, false);
                this.ScribeClient = new scribe.Client(this.ThriftBinaryProtocol);
                this.ThriftFramedTransport.Open();
            }
            catch (Exception ex)
            {
                ErrorHandler.Error(
                    "Could not initialize the scribe connection to " +
                    this.RemoteHost + ":" +
                    this.RemotePort.ToString(NumberFormatInfo.InvariantInfo),
                    ex,
                    ErrorCode.GenericFailure);
                this.ThriftSocket = null;
                this.ThriftFramedTransport = null;
                this.ThriftBinaryProtocol = null;
                this.ScribeClient = null;
            }
        }

        #endregion Protected Instance Methods
    }
}
