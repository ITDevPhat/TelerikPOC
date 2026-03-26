using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using Telerik.Reporting.Cache.Interfaces;

namespace TelerikPOC.Services;

public sealed class S3Storage : IStorage
{
    private readonly IAmazonS3 _s3;
    private readonly string _bucketName;
    private readonly string _prefix;

    public S3Storage(IAmazonS3 s3, string bucketName, string prefix)
    {
        _s3 = s3 ?? throw new ArgumentNullException(nameof(s3));
        _bucketName = string.IsNullOrWhiteSpace(bucketName)
            ? throw new ArgumentException("Bucket name is required.", nameof(bucketName))
            : bucketName;
        _prefix = string.IsNullOrWhiteSpace(prefix)
            ? throw new ArgumentException("Prefix is required.", nameof(prefix))
            : prefix.Trim('/');
    }

    public void SetBytes(string key, byte[] value) => PutObject(KeyFor(key), value);

    public byte[] GetBytes(string key)
    {
        var bytes = GetObject(KeyFor(key));
        if (bytes == null)
            throw new KeyNotFoundException($"Storage key '{key}' not found.");
        return bytes;
    }

    public void SetString(string key, string value) => SetBytes(key, Encoding.UTF8.GetBytes(value));

    public string GetString(string key) => Encoding.UTF8.GetString(GetBytes(key));

    public bool Exists(string key) => ExistsObject(KeyFor(key));

    public void AddInSet(string key, string value)
    {
        var members = new HashSet<string>(GetSet(key), StringComparer.Ordinal);
        members.Add(value);
        SaveSet(key, members);
    }

    public bool DeleteInSet(string key, string value)
    {
        var members = new HashSet<string>(GetSet(key), StringComparer.Ordinal);
        var removed = members.Remove(value);
        SaveSet(key, members);
        return removed;
    }

    public void DeleteSet(string key) => Delete(KeyForSet(key));

    public bool ExistsInSet(string key, string value) => GetSet(key).Contains(value);

    public IEnumerable<string> GetAllMembersInSet(string key) => GetSet(key);

    public long GetCountInSet(string key) => GetSet(key).Count;

    public void Delete(string key) => Delete(KeyFor(key));

    public IDisposable AcquireLock(string key)
    {
        var lockKey = KeyForLock(key);
        var leaseId = Guid.NewGuid().ToString("N");

        while (true)
        {
            if (!ExistsObject(lockKey))
            {
                PutObject(lockKey, Encoding.UTF8.GetBytes(leaseId));
                var current = GetObject(lockKey);
                if (current != null && Encoding.UTF8.GetString(current) == leaseId)
                    break;
            }

            Thread.Sleep(50);
        }

        return new ReleaseLock(() =>
        {
            var current = GetObject(lockKey);
            if (current != null && Encoding.UTF8.GetString(current) == leaseId)
                Delete(lockKey);
        });
    }

    public IDisposable AcquireReadLock(string key) => AcquireLock(key);

    private string KeyFor(string key) => $"{_prefix}/bytes/{Sanitize(key)}";
    private string KeyForSet(string key) => $"{_prefix}/sets/{Sanitize(key)}.json";
    private string KeyForLock(string key) => $"{_prefix}/locks/{Sanitize(key)}.lock";

    private static string Sanitize(string key) => key.Replace('\\', '/');

    private void PutObject(string key, byte[] value)
    {
        using var ms = new MemoryStream(value);
        _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            InputStream = ms
        }).GetAwaiter().GetResult();
    }

    private byte[]? GetObject(string key)
    {
        try
        {
            using var response = _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = key
            }).GetAwaiter().GetResult();

            using var ms = new MemoryStream();
            response.ResponseStream.CopyTo(ms);
            return ms.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private bool ExistsObject(string key)
    {
        try
        {
            _s3.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = key
            }).GetAwaiter().GetResult();
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private void Delete(string key)
    {
        _s3.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = _bucketName,
            Key = key
        }).GetAwaiter().GetResult();
    }

    private HashSet<string> GetSet(string key)
    {
        var data = GetObject(KeyForSet(key));
        if (data == null || data.Length == 0)
            return [];

        var parsed = JsonSerializer.Deserialize<HashSet<string>>(data) ?? [];
        return parsed;
    }

    private void SaveSet(string key, HashSet<string> members)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(members);
        PutObject(KeyForSet(key), bytes);
    }

    private sealed class ReleaseLock : IDisposable
    {
        private readonly Action _release;
        private int _disposed;

        public ReleaseLock(Action release) => _release = release;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _release();
        }
    }
}
