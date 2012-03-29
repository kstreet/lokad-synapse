#region (c) 2012 Lokad.Synapse - New BSD License
// Copyright (c) Lokad 2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence
#endregion

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Lokad.Cqrs.Feature.TapeStorage;
using Lokad.Cqrs.TapeStorage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.StorageClient;

namespace event_store_worker
{
    public class WorkerRole : RoleEntryPoint
    {
        readonly CancellationTokenSource _source = new CancellationTokenSource();
        readonly ManualResetEvent _terminated = new ManualResetEvent(false);
        Server _server;
        string _endpoint;

        public override void Run()
        {
            try
            {
                Trace.WriteLine("Started.");

                new Listener(_endpoint, _server).Run(_source.Token);

                Trace.WriteLine("Stopped.");
            }
            catch (Exception e)
            {
                Trace.WriteLine("Exception during Run: " + e);
            }
            finally
            {
                _terminated.Set();
            }
        }

        public override bool OnStart()
        {
            try
            {
                // Set the maximum number of concurrent connections 
                ServicePointManager.DefaultConnectionLimit = 12;

                RoleEnvironment.Changing += RoleEnvironmentChanging;

                _endpoint = "tcp://" + RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["Endpoint"].IPEndpoint;

                var storageConnectionString = RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString");
                var containerName = RoleEnvironment.GetConfigurationSettingValue("BlobContainerName");
                _server = BuildServer(containerName, storageConnectionString);

                Trace.WriteLine("Initialized.");
                return true;
            }
            catch (Exception e)
            {
                Trace.WriteLine("Exception during OnStart: " + e);
                return false;
            }
        }

        static Server BuildServer(string containerName, string storageConnectionString)
        {
            var account = CloudStorageAccount.Parse(storageConnectionString);
            var cloudBlobClient = account.CreateCloudBlobClient();
            var tapeStorageFactory = new BlobTapeStorageFactory(cloudBlobClient, containerName);
            tapeStorageFactory.InitializeForWriting();
            Func<string, ITapeStream> streamFactory = tapeStorageFactory.GetOrCreateStream;
            return new Server(streamFactory);
        }

        public override void OnStop()
        {
            try
            {
                _source.Cancel();
                _terminated.WaitOne();
            }
            catch (Exception e)
            {
                Trace.WriteLine("Exception during OnStop: " + e);
            }
        }

        static void RoleEnvironmentChanging(object sender, RoleEnvironmentChangingEventArgs e)
        {
            // If a configuration setting is changing
            if (e.Changes.Any(change => change is RoleEnvironmentConfigurationSettingChange))
            {
                // Set e.Cancel to true to restart this role instance
                e.Cancel = true;
            }
        }
    }
}
