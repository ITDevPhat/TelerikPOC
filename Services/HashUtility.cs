using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace TelerikPOC.Services;

/// <summary>
/// Deterministic hash for snapshot cache keys.
///
/// Strategy: SHA256( reportId + "|" + canonicalParamsJson )
///
/// Canonical params JSON:
///   - Sort keys alphabetically (case-insensitive)
///   - Serialize values with JsonSerializer (handles nulls, numbers, bools)
///   - Result is always the same for the same logical parameter set
///
/// Example:
///   reportId   = "participant_listing:v2"
///   params     = { "StudyId": 5, "Status": "Active" }
///   canonical  = {"status":"Active","studyid":5}
///   hash input = "participant_listing:v2|{"status":"Active","studyid":5}"
///   output     = "a3f9b2...64 hex chars"
/// </summary>
public static class HashUtility
{
    /// <summary>
    /// Compute a 64-character lowercase hex SHA-256 hash suitable for use
    /// as a snapshot cache key.
    /// </summary>
    public static string ComputeHash(
        string reportId,
        IDictionary<string, object?>? parameters)
    {
        var canonical = BuildCanonical(reportId, parameters);
        var bytes     = Encoding.UTF8.GetBytes(canonical);
        var hash      = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant(); // 64 chars
    }

    // ─────────────────────────────────────────────────────────────
    // Internal
    // ─────────────────────────────────────────────────────────────

    internal static string BuildCanonical(
        string reportId,
        IDictionary<string, object?>? parameters)
    {
        if (parameters == null || parameters.Count == 0)
            return reportId + "|{}";

        // Sort keys case-insensitively, normalise to lowercase
        var sorted = parameters
            .OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                kv => kv.Key.ToLowerInvariant(),
                kv => kv.Value,
                StringComparer.OrdinalIgnoreCase);

        var paramsJson = JsonSerializer.Serialize(sorted, _jsonOptions);
        return reportId + "|" + paramsJson;
    }

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        // Ensure consistent serialization regardless of culture
        WriteIndented      = false,
        PropertyNamingPolicy = null,
    };
}
