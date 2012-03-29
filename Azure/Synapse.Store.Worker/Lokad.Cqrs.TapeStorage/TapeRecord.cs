#region (c) 2010-2012 Lokad - CQRS for Windows Azure - New BSD License
// Copyright (c) Lokad 2010-2012, http://www.lokad.com
// This code is released as Open Source under the terms of the New BSD Licence
#endregion

namespace Lokad.Cqrs.TapeStorage
{
    /// <summary>
    /// Contains information about the committed data
    /// </summary>
    public sealed class TapeRecord
    {
        public readonly long Version;
        public readonly byte[] Data;

        public TapeRecord(long version, byte[] data)
        {
            Version = version;
            Data = data;
        }
    }
}