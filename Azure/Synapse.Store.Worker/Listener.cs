#region (c) 2012 Lokad.Synapse - New BSD License
// Copyright (c) Lokad 2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence
#endregion

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using ZMQ;

namespace event_store_worker
{
    class Listener
    {
        readonly string _endpoint;
        readonly Server _server;

        public Listener(string endpoint, Server server)
        {
            _endpoint = endpoint;
            _server = server;
        }

        public void Run(CancellationToken cancellationToken)
        {
            using (var ctx = new Context(1))
            {
                Socket sock = null;
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            if (sock == null)
                            {
                                sock = ctx.Socket(SocketType.REP);
                                sock.Bind(_endpoint);
                            }

                            var command = sock.Recv(Encoding.UTF8, new[] {SendRecvOpt.NOBLOCK});
                            if (null == command)
                                cancellationToken.WaitHandle.WaitOne(500);
                            else
                                ExecCommand(command, sock);
                        }
                        catch (ZMQ.Exception ex)
                        {
                            // context is shutting down
                            if (ex.Errno == (int) ERRNOS.ETERM)
                                return;

                            // Console.WriteLine(ex);
                            if (null == sock)
                                continue;

                            sock.Dispose();
                            sock = null;
                        }
                        catch (System.Exception ex)
                        {
                            Trace.WriteLine("Exception during Run: " + ex);
                            if (null != sock)
                                sock.Send(ex.ToString(), Encoding.UTF8);
                        }
                    }
                }
                finally
                {
                    if (sock != null)
                        sock.Dispose();
                }
            }
        }

        void ExecCommand(string op, Socket sock)
        {
            string name;
            long version;
            switch (op)
            {
                case "Append":
                    name = sock.Recv(Encoding.UTF8);
                    version = _server.Append(name, sock.Recv);
                    sock.Send(BitConverter.GetBytes(version));
                    break;
                case "Version":
                    name = sock.Recv(Encoding.UTF8);
                    version = _server.Version(name);
                    sock.Send(BitConverter.GetBytes(version));
                    break;
                default:
                    sock.Send("UNKNOWN", Encoding.UTF8);
                    break;
            }
        }
    }
}
