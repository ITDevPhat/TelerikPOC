using System;
using System.Collections.Generic;
using System.IO;
using Telerik.Reporting.Cache.Interfaces;
using Telerik.Reporting.Cache.File;

public class LoggingStorage : IStorage
{
    private readonly FileStorage _inner;
    private readonly string _logFolder;

    public LoggingStorage(string basePath, string logFolder)
    {
        _inner = new FileStorage(basePath);
        _logFolder = logFolder;

        Directory.CreateDirectory(_logFolder);
    }

    // ========================
    // BYTE CACHE (QUAN TRỌNG NHẤT)
    // ========================

    public void SetBytes(string key, byte[] value)
    {
        _inner.SetBytes(key, value);

        try
        {
            var path = Path.Combine(_logFolder, $"{key}.dat");
            File.WriteAllBytes(path, value);

            Console.WriteLine($"[CACHE] {key} | {value.Length} bytes");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CACHE ERROR] {ex.Message}");
        }
    }

    public byte[] GetBytes(string key)
    {
        return _inner.GetBytes(key);
    }

    // ========================
    // STRING CACHE
    // ========================

    public void SetString(string key, string value)
    {
        _inner.SetString(key, value);
    }

    public string GetString(string key)
    {
        return _inner.GetString(key);
    }

    // ========================
    // EXISTENCE
    // ========================

    public bool Exists(string key)
    {
        return _inner.Exists(key);
    }

    // ========================
    // SET OPERATIONS
    // ========================

    public void AddInSet(string key, string value)
    {
        _inner.AddInSet(key, value);
    }

    public bool DeleteInSet(string key, string value)
    {
        return _inner.DeleteInSet(key, value);
    }

    public void DeleteSet(string key)
    {
        _inner.DeleteSet(key);
    }

    public bool ExistsInSet(string key, string value)
    {
        return _inner.ExistsInSet(key, value);
    }

    public IEnumerable<string> GetAllMembersInSet(string key)
    {
        return _inner.GetAllMembersInSet(key);
    }

    public long GetCountInSet(string key)
    {
        return _inner.GetCountInSet(key);
    }

    // ========================
    // DELETE
    // ========================

    public void Delete(string key)
    {
        _inner.Delete(key);
    }

    // ========================
    // LOCKING (CRITICAL)
    // ========================

    public IDisposable AcquireLock(string key)
    {
        return _inner.AcquireLock(key);
    }

    public IDisposable AcquireReadLock(string key)
    {
        return _inner.AcquireReadLock(key);
    }
}