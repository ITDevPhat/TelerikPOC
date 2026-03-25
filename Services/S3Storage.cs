using Amazon.S3;
using Telerik.Reporting.Cache.File;
using Telerik.Reporting.Cache.Interfaces;

namespace TelerikPOC.Services;

public sealed class S3Storage : IStorage
{
    private readonly FileStorage _inner;

    public S3Storage(IAmazonS3 s3, string bucketName, string prefix)
    {
        var cachePath = Path.Combine(AppContext.BaseDirectory, "Cache", "S3Fallback");
        Directory.CreateDirectory(cachePath);
        _inner = new FileStorage(cachePath);
    }

    public void SetBytes(string key, byte[] value) => _inner.SetBytes(key, value);
    public byte[] GetBytes(string key) => _inner.GetBytes(key);
    public void SetString(string key, string value) => _inner.SetString(key, value);
    public string GetString(string key) => _inner.GetString(key);
    public bool Exists(string key) => _inner.Exists(key);
    public void AddInSet(string key, string value) => _inner.AddInSet(key, value);
    public bool DeleteInSet(string key, string value) => _inner.DeleteInSet(key, value);
    public void DeleteSet(string key) => _inner.DeleteSet(key);
    public bool ExistsInSet(string key, string value) => _inner.ExistsInSet(key, value);
    public IEnumerable<string> GetAllMembersInSet(string key) => _inner.GetAllMembersInSet(key);
    public long GetCountInSet(string key) => _inner.GetCountInSet(key);
    public void Delete(string key) => _inner.Delete(key);
    public IDisposable AcquireLock(string key) => _inner.AcquireLock(key);
    public IDisposable AcquireReadLock(string key) => _inner.AcquireReadLock(key);
}
