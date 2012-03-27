#region (c) 2012 Lokad.Synapse - New BSD License 

// Copyright (c) Lokad 2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence

#endregion

using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZMQ;
using Exception = ZMQ.Exception;

namespace EventStore
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                var fp = Assembly.GetEntryAssembly().Location + ".txt";
                if (File.Exists(fp))
                {
                    args = File.ReadAllText(fp).Split('|');
                }
            }

            if (args.Length < 2)
            {
                Console.WriteLine("Usage: {socket} [folder]");
                Environment.Exit(1);
            }
            var root = string.IsNullOrEmpty(args[1]) ? Directory.GetCurrentDirectory() : args[1];
            var socket = args[0];

            // ackquire lock
            string lockWriterBin = Path.Combine(root, "lock-store.txt");
            using (File.Open(lockWriterBin, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
            {
                using (var cts = new CancellationTokenSource())
                {
                    var token = cts.Token;


                    var writerTask = Task.Factory.StartNew(() =>
                        {
                            using (var ctx = new Context(1))
                            {
                                RunWriter(token, root, () =>
                                    {
                                        var sock = ctx.Socket(SocketType.REP);
                                        sock.Bind(socket);
                                        return sock;
                                    });
                            }
                        }, token);

                    Console.WriteLine("Running ES. Hit enter to kill");
                    Console.ReadLine();
                    Console.WriteLine("Terminating....");
                    cts.Cancel();

                    if (!Task.WaitAll(new[] {writerTask}, 5000))
                    {
                        Console.WriteLine("Termination failed");
                    }
                }
            }
            File.Delete(lockWriterBin);
        }

        static void Version(Socket sock, string root)
        {
            var name = sock.Recv(Encoding.UTF8);
            var fileName = Path.GetFileName(name) ?? "";
            var combine = Path.Combine(root, fileName);
            if (!File.Exists(combine))
            {
                sock.Send(BitConverter.GetBytes(0L));
            }
            else
            {
                var s = new FileTapeStream(combine);
                sock.Send(BitConverter.GetBytes(s.GetCurrentVersion()));
            }
        }

        static void Append(Socket sock, string root)
        {
            var name = sock.Recv(Encoding.UTF8);
            var fileName = Path.GetFileName(name) ?? "";
            var combine = Path.Combine(root, fileName);

            var s = new FileTapeStream(combine);
            Console.WriteLine("Append {0}", fileName);
            long result = 0;
            while (true)
            {
                var verDat = sock.Recv();
                if ((null == verDat) || (verDat.Length == 0))
                    break;

                var version = BitConverter.ToInt64(verDat, 0);
                Console.WriteLine("  " + version);
                var data = sock.Recv();

                var cond = TapeAppendCondition.VersionIs(version);
                result = s.TryAppend(data, cond);
            }
            s.UpdateCounter(result);
            sock.Send(BitConverter.GetBytes(result));
        }


        static void RunWriter(CancellationToken token, string root, Func<Socket> factory)
        {
            Socket sock = null;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (sock == null)
                    {
                        sock = factory();
                    }

                    var op = sock.Recv(Encoding.UTF8, new[]{SendRecvOpt.NOBLOCK, });
                    if (null == op)
                    {
                        token.WaitHandle.WaitOne(500);
                        continue;
                    }
                        

                    switch (op)
                    {
                        case "Append":
                            Append(sock, root);
                            break;
                        case "Version":
                            Version(sock, root);
                            break;
                        default:
                            sock.Send("UNKNOWN", Encoding.UTF8);
                            break;
                    }
                }
                catch (Exception ex)
                {

                    // context is shutting down
                    if (ex.Errno == (int) ERRNOS.ETERM)
                        return;

                    Console.WriteLine(ex);
                    if (null != sock)
                    {
                        sock.Dispose();
                        sock = null;
                    }
                }
                catch (System.Exception ex)
                {
                    Console.WriteLine(ex);
                    if (null != sock)
                    {
                        sock.Send(ex.ToString(), Encoding.UTF8);
                    }
                }
            }
        }
    }
}