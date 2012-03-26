using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace EventStore
{

    /// <summary>
    /// <para>Persists records in a tape stream, using SHA1 hashing and "magic" number sequences
    /// to detect corruption and offer partial recovery.</para>
    /// <para>System information is written in such a way, that if data is Unicode human-readable,
    /// then the file will be human-readable as well.</para>
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class FileTapeStream 
    {
        readonly FileInfo _data;

        public FileTapeStream(string name)
        {
            _data = new FileInfo(name);
        }

        public IEnumerable<TapeRecord> ReadRecords(long afterVersion, int maxCount)
        {
            if (afterVersion < 0)
                throw new ArgumentOutOfRangeException("afterVersion", "Must be zero or greater.");

            if (maxCount <= 0)
                throw new ArgumentOutOfRangeException("maxCount", "Must be more than zero.");

            // afterVersion + maxCount > long.MaxValue, but transformed to avoid overflow
            if (afterVersion > long.MaxValue - maxCount)
                throw new ArgumentOutOfRangeException("maxCount", "Version will exceed long.MaxValue.");

            if (!_data.Exists)
            {
                // file could've been created since the last check
                _data.Refresh();
                if (!_data.Exists)
                {
                    yield break;
                }
            }

            using (var file = OpenForRead())
            {
                if (!TapeStreamSerializer.SkipRecords(afterVersion, file))
                    yield break;

                for (var i = 0; i < maxCount; i++)
                {
                    if (file.Position == file.Length)
                        yield break;

                    var record = TapeStreamSerializer.ReadRecord(file);

                    yield return record;
                }
            }
        }

        public long GetCurrentVersion()
        {
            try
            {
                using (var s = OpenForRead())
                {
                    // seek end
                    s.Seek(0, SeekOrigin.End);
                    return TapeStreamSerializer.ReadVersionFromTheEnd(s);
                }
            }
            catch (FileNotFoundException)
            {
                return 0;
            }
            catch (DirectoryNotFoundException)
            {
                return 0;
            }
        }

        public long TryAppend(byte[] buffer, TapeAppendCondition condition)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");

            if (buffer.Length == 0)
                throw new ArgumentException("Buffer must contain at least one byte.");

            using (var file = OpenForWrite())
            {
                // we need to know version first.
                file.Seek(0, SeekOrigin.End);
                var version = TapeStreamSerializer.ReadVersionFromTheEnd(file);

                if (!condition.Satisfy(version))
                    return 0;

                var versionToWrite = version + 1;
                TapeStreamSerializer.WriteRecord(file, buffer, versionToWrite);

                return versionToWrite;
            }
        }

        public void AppendNonAtomic(IEnumerable<TapeRecord> records)
        {
            if (records == null)
                throw new ArgumentNullException("records");

            // if enumeration is lazy, then this would cause another enum
            //if (!records.Any())
            //    return;

            using (var file = OpenForWrite())
            {
                file.Seek(0, SeekOrigin.End);

                foreach (var record in records)
                {
                    if (record.Data.Length == 0)
                        throw new ArgumentException("Record must contain at least one byte.");

                    var versionToWrite = record.Version;
                    TapeStreamSerializer.WriteRecord(file, record.Data, versionToWrite);
                }
            }
        }

        FileStream OpenForWrite()
        {
            // we allow concurrent reading
            // no more writers are allowed
            return _data.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        }

        FileStream OpenForRead()
        {
            // we allow concurrent writing or reading
            return _data.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }
    }

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

    public class TapeStreamSerializer
    {

        static readonly byte[] ReadableHeaderStart = Encoding.UTF8.GetBytes("/* header ");
        static readonly byte[] ReadableHeaderEnd = Encoding.UTF8.GetBytes(" */\r\n");
        static readonly byte[] ReadableFooterStart = Encoding.UTF8.GetBytes("\r\n/* footer ");
        static readonly byte[] ReadableFooterEnd = Encoding.UTF8.GetBytes(" */\r\n");

        public static bool SkipRecords(long count, Stream file)
        {
            for (var i = 0; i < count; i++)
            {
                if (file.Position == file.Length)
                    return false;

                file.Seek(ReadableHeaderStart.Length, SeekOrigin.Current);
                var dataLength = ReadReadableInt64(file);
                var skip = ReadableHeaderEnd.Length + dataLength +
                    ReadableFooterStart.Length + 16 + 16 + 28 + ReadableFooterEnd.Length;
                file.Seek(skip, SeekOrigin.Current);
            }

            return true;
        }

        public static void WriteRecord(Stream stream, byte[] data, long versionToWrite)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            using (var managed = new SHA1Managed())
            {
                writer.Write(ReadableHeaderStart);
                WriteReadableInt64(writer, data.Length);
                writer.Write(ReadableHeaderEnd);

                writer.Write(data);
                writer.Write(ReadableFooterStart);
                WriteReadableInt64(writer, data.Length);
                WriteReadableInt64(writer, versionToWrite);
                WriteReadableHash(writer, managed.ComputeHash(data));
                writer.Write(ReadableFooterEnd);

                ms.Seek(0, SeekOrigin.Begin);
                ms.CopyTo(stream);
            }
        }

        public static TapeRecord ReadRecord(Stream file)
        {
            ReadAndVerifySignature(file, ReadableHeaderStart, "Start");
            var dataLength = ReadReadableInt64(file);
            ReadAndVerifySignature(file, ReadableHeaderEnd, "Header-End");

            var data = new byte[dataLength];
            file.Read(data, 0, (int)dataLength);

            ReadAndVerifySignature(file, ReadableFooterStart, "Footer-Start");

            ReadReadableInt64(file); //length verified
            var recVersion = ReadReadableInt64(file);
            var hash = ReadReadableHash(file);
            using (var managed = new SHA1Managed())
            {

                var computed = managed.ComputeHash(data);

                if (!computed.SequenceEqual(hash))
                    throw new InvalidOperationException("Hash corrupted");
            }
            ReadAndVerifySignature(file, ReadableFooterEnd, "End");

            return new TapeRecord(recVersion, data);
        }

        public static long ReadVersionFromTheEnd(Stream stream)
        {
            if (stream.Position == 0)
                return 0;

            var seekBack = ReadableFooterEnd.Length + 28 + 16;
            stream.Seek(-seekBack, SeekOrigin.Current);

            var version = ReadReadableInt64(stream);

            stream.Seek(28, SeekOrigin.Current);
            ReadAndVerifySignature(stream, ReadableFooterEnd, "End");

            return version;
        }

        static long ReadReadableInt64(Stream stream)
        {
            var buffer = new byte[16];
            stream.Read(buffer, 0, 16);
            var s = Encoding.UTF8.GetString(buffer);
            return Int64.Parse(s, NumberStyles.HexNumber);
        }

        static IEnumerable<byte> ReadReadableHash(Stream stream)
        {
            var buffer = new byte[28];
            stream.Read(buffer, 0, buffer.Length);
            var hash = Convert.FromBase64String(Encoding.UTF8.GetString(buffer));
            return hash;
        }

        static void ReadAndVerifySignature(Stream source, byte[] signature, string name)
        {
            for (var i = 0; i < signature.Length; i++)
            {
                var readByte = source.ReadByte();
                if (readByte == -1)
                    throw new InvalidOperationException(String.Format("Expected byte[{0}] of signature '{1}', but found EOL", i, name));
                if (readByte != signature[i])
                {
                    throw new InvalidOperationException("Signature failed: " + name);
                }
            }
        }

        static void WriteReadableInt64(BinaryWriter writer, long value)
        {
            // long is 8 bytes ==> 16 bytes of readable Unicode.
            var buffer = Encoding.UTF8.GetBytes(value.ToString("x16"));

            writer.Write(buffer);
        }

        static void WriteReadableHash(BinaryWriter writer, byte[] hash)
        {
            // hash is 20 bytes, which is encoded into 28 bytes of readable Unicode
            var buffer = Encoding.UTF8.GetBytes(Convert.ToBase64String(hash));

            writer.Write(buffer);
        }
    }

    /// <summary>
    /// <para>Allows to specify optional condition for appending to the storage.</para>
    /// <para>This is defined as struct to allow proper use in optional params</para>
    /// </summary>
    public struct TapeAppendCondition
    {
        /// <summary>
        /// Version to match against, if <see cref="IsSpecified"/> is set to <em>True</em>.
        /// </summary>
        public readonly long Version;
        /// <summary>
        /// If the condition has been specified
        /// </summary>
        public readonly bool IsSpecified;

        TapeAppendCondition(long index)
        {
            Version = index;
            IsSpecified = true;
        }

        /// <summary>
        /// Constructs condition that matches the specified version
        /// </summary>
        /// <param name="version">The version.</param>
        /// <returns>new condition instance</returns>
        public static TapeAppendCondition VersionIs(long version)
        {
            return new TapeAppendCondition(version);
        }

        /// <summary>
        /// Condition that always matches
        /// </summary>
        public static readonly TapeAppendCondition None = default(TapeAppendCondition);

        public bool Satisfy(long index)
        {
            if (!IsSpecified)
                return true;
            return index == Version;
        }

        /// <summary>
        /// Enforces the specified version, throwing exception if the condition was not met.
        /// </summary>
        /// <param name="version">The version to match.</param>
        /// <exception cref="TapeAppendConditionException">when the condition was not met by the version specified</exception>
        public void Enforce(long version)
        {
            if (!Satisfy(version))
            {
                var message = string.Format("Expected store version {0} but was {1}. Probablye is was modified concurrently.", Version, version);
                throw new TapeAppendConditionException(message);
            }
        }
    }

    /// <summary>
    /// Is thrown internally, when storage version does not match the condition specified in <see cref="TapeAppendCondition"/>
    /// </summary>
    [Serializable]
    public class TapeAppendConditionException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public TapeAppendConditionException() { }
        public TapeAppendConditionException(string message) : base(message) { }
        public TapeAppendConditionException(string message, Exception inner) : base(message, inner) { }

        protected TapeAppendConditionException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context) { }


    }


}