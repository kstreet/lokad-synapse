#region (c) 2012 Lokad.Synapse - New BSD License
// Copyright (c) Lokad 2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence
#endregion

using System;
using System.Diagnostics;
using Lokad.Cqrs.TapeStorage;

namespace event_store_worker
{
    class Server
    {
        readonly Func<string, ITapeStream> _tapeStreamFactory;

        public Server(Func<string, ITapeStream> tapeStreamFactory)
        {
            _tapeStreamFactory = tapeStreamFactory;
        }

        public long Version(string name)
        {
            var s = _tapeStreamFactory(name);
            return s.GetCurrentVersion();
        }

        public long Append(string name, Func<byte[]> receive)
        {
            var s = _tapeStreamFactory(name);

            Trace.WriteLine("Append {0}", name);

            var lastVersion = 0L;
            while (true)
            {
                var versionBytes = receive();
                if ((null == versionBytes) || (versionBytes.Length == 0))
                    break;

                var version = BitConverter.ToInt64(versionBytes, 0);
                Trace.WriteLine("  " + version);

                var cond = TapeAppendCondition.VersionIs(version);
                var data = receive();
                if (s.TryAppend(data, cond))
                    lastVersion = version;
            }

            return lastVersion;
        }
    }
}