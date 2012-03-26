#region (c) 2012 Lokad.Synapse - New BSD License 

// Copyright (c) Lokad 2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using EventStore;
using ZMQ;
using Exception = System.Exception;

namespace EventTest
{
    class Program
    {
        // active replicator
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

            if (args.Length < 3)
            {
                Console.WriteLine("Usage: {file} {name} {target1} {target2} etc");
                Environment.Exit(1);
            }

            var file = args[0];
            var name = args[1];

            using (var ctx = new Context())
            {
                var remotes = args.Skip(2).Select(s => new RemoteTapeStream(ctx, s)).ToArray();
                var path = Path.GetDirectoryName(file) ?? "";
                var e = new ManualResetEventSlim(true);
                using (var w = new FileSystemWatcher(path, Path.GetFileName(file + ".ver") ?? ""))
                {
                    var stream = new FileTapeStream(file);
                    w.Changed += (sender, eventArgs) =>
                        {
                            if (eventArgs.ChangeType == WatcherChangeTypes.Changed)
                            {
                                e.Set();
                            }
                        };
                    w.EnableRaisingEvents = true;

                    while (true)
                    {
                        e.Wait();
                        bool allSynced = true;
                        var knownVersion = GetKnownVersion(file);
                        foreach (var remote in remotes)
                        {
                            var result = SyncChanges(remote, stream, knownVersion, name);
                            if (!result)
                                allSynced = false;
                        }

                        if (allSynced && knownVersion == GetKnownVersion(file))
                        {
                            e.Reset();
                        }
                    }
                }
            }
        }

        static long GetKnownVersion(string file)
        {
            long knownVersion;
            using (var r = File.Open(file + ".ver", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var read = new BinaryReader(r))
            {
                knownVersion = read.ReadInt64();
            }
            return knownVersion;
        }

        static bool SyncChanges(RemoteTapeStream remote, FileTapeStream source, long knownVersion, string name)
        {
            try
            {
                // remote ver
                var rVer = remote.GetVersion(name);
                if (rVer == -1)
                {
                    Console.WriteLine("Server unavailable: {0}", remote.Address);
                    return false;
                }

                if (rVer >= knownVersion)
                {
                    Console.WriteLine("We are up to date");
                    return true;
                }

                while (rVer < knownVersion)
                {
                    var diff = knownVersion - rVer;
                    var change = Math.Min(1000, diff);
                    Console.WriteLine("Update {0} from {1} to {2}", remote.Address, rVer, rVer + change);
                    var difference = source.ReadRecords(rVer, (int) change);
                    // send data in groups

                    var result = remote.Append(name, difference);
                    if (result == -1)
                    {
                        Console.WriteLine("Server went offline");
                        return false;
                    }
                    rVer += change;
                }
                Console.WriteLine("Server updates sent");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return false;
            }
        }
    }

    public sealed class RemoteTapeStream
    {
        readonly Context _ctx;
        public readonly string Address;
        Socket _socket;

        public RemoteTapeStream(Context ctx, string address)
        {
            _ctx = ctx;
            Address = address;
        }


        public long Append(string name, IEnumerable<TapeRecord> record)
        {
            var sock = GetSocket();
            try
            {
                sock.SendMore("Append", Encoding.UTF8);
                sock.SendMore(name, Encoding.UTF8);

                foreach (var r in record)
                {
                    sock.SendMore(BitConverter.GetBytes(r.Version - 1));
                    sock.SendMore(r.Data);
                }
                sock.Send();


                var value = _socket.Recv(5000);
                if (null == value)
                {
                    ClearSocket();
                    return -1;
                }
                return BitConverter.ToInt64(value, 0);
            }
            catch (ZMQ.Exception)
            {
                ClearSocket();
                throw;
            }
        }

        public long GetVersion(string name)
        {
            var sock = GetSocket();
            try
            {
                sock.SendMore("Version", Encoding.UTF8);
                sock.Send(name, Encoding.UTF8);
                var result = _socket.Recv(2000);
                if (null == result)
                {
                    ClearSocket();
                    return -1;
                }
                return BitConverter.ToInt64(result, 0);
            }
            catch (ZMQ.Exception)
            {
                ClearSocket();
                throw;
            }
        }

        void ClearSocket()
        {
            _socket.Dispose();
            _socket = null;
        }

        Socket GetSocket()
        {
            if (null == _socket)
            {
                _socket = _ctx.Socket(SocketType.REQ);
                _socket.Connect(Address);
            }
            return _socket;
        }
    }
}