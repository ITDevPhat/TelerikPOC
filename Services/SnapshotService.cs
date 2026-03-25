using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using TelerikPOC.Domain;
using TelerikPOC.Infrastructure;

namespace TelerikPOC.Services;

// ═══════════════════════════════════════════════════════════════
// Interface
// ═══════════════════════════════════════════════════════════════

/// <summary>
/// Snapshot layer sitting ABOVE Telerik's internal IStorage.
///
/// Telerik IStorage  →  short-lived session cache (bytes/strings/sets)
/// SnapshotService   →  long-lived rendered document cache (PDF/HTML/CSV)
///
/// Flow:
///   BeforeRender:  TryGetAsync → if hit, return bytes → skip render
///   AfterRender:   SaveAsync   → upload bytes to S3 + insert DB record
/// </summary>
public interface ISnapshotService
{
    /// <summary>
    /// Try to load a previously rendered document from S3.
    /// Returns null on cache miss or if the snapshot has expired.
    /// </summary>
    Task<byte[]?> TryGetAsync(
        string reportName, string version,
        string parametersHash, string format);

    /// <summary>
    /// Persist a rendered document:
    ///   1. Upload bytes to S3
    ///   2. Insert metadata record into rpt.ReportSnapshots
    /// </summary>
    Task SaveAsync(
        string reportName, string version,
        string parametersHash, string format,
        byte[] renderedBytes,
        TimeSpan? ttl = null);
}

// ═══════════════════════════════════════════════════════════════
// Implementation
// ═══════════════════════════════════════════════════════════════

public sealed class SnapshotService : ISnapshotService
{
    private readonly IAmazonS3            _s3;
    private readonly string               _bucketName;
    private readonly ISnapshotRepository  _repo;
    private readonly ILogger<SnapshotService> _logger;

    // Default TTL: 24 hours.  Override per call or via config.
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromHours(24);

    // S3 prefix for snapshot objects
    private const string SnapshotPrefix = "telerik/snapshots";

    public SnapshotService(
        IAmazonS3 s3,
        string bucketName,
        ISnapshotRepository repo,
        ILogger<SnapshotService> logger)
    {
        _s3         = s3         ?? throw new ArgumentNullException(nameof(s3));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _repo       = repo       ?? throw new ArgumentNullException(nameof(repo));
        _logger     = logger     ?? throw new ArgumentNullException(nameof(logger));
    }

    // ─────────────────────────────────────────────────────────────────
    // TryGet — check DB first, then S3
    // ─────────────────────────────────────────────────────────────────

    public async Task<byte[]?> TryGetAsync(
        string reportName, string version,
        string parametersHash, string format)
    {
        // ── 1. Lookup metadata in DB ─────────────────────────────────
        var snapshot = await _repo.FindAsync(reportName, version, parametersHash, format);

        if (snapshot == null)
        {
            _logger.LogDebug(
                "[Snapshot] MISS  report={Name} v={Version} hash={Hash} fmt={Fmt}",
                reportName, version, parametersHash[..8] + "…", format);
            return null;
        }

        // ── 2. Download bytes from S3 ────────────────────────────────
        try
        {
            var response = await _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key        = snapshot.S3Key
            });

            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms);
            var bytes = ms.ToArray();

            _logger.LogInformation(
                "[Snapshot] HIT   report={Name} v={Version} hash={Hash} fmt={Fmt} size={Size}b",
                reportName, version, parametersHash[..8] + "…", format, bytes.Length);

            return bytes;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // DB record exists but S3 object was deleted — treat as miss
            _logger.LogWarning(
                "[Snapshot] DB hit but S3 miss for key '{Key}'. Treating as cache miss.",
                snapshot.S3Key);
            return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Save — upload to S3 then insert DB record
    // ─────────────────────────────────────────────────────────────────

    public async Task SaveAsync(
        string reportName, string version,
        string parametersHash, string format,
        byte[] renderedBytes,
        TimeSpan? ttl = null)
    {
        var effectiveTtl = ttl ?? DefaultTtl;
        var s3Key        = BuildS3Key(reportName, version, parametersHash, format);

        // ── 1. Upload to S3 ──────────────────────────────────────────
        using var stream = new MemoryStream(renderedBytes);

        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName  = _bucketName,
            Key         = s3Key,
            InputStream = stream,
            ContentType = FormatToContentType(format),
        });

        // ── 2. Insert DB record ──────────────────────────────────────
        var snapshot = new ReportSnapshot
        {
            Id             = Guid.NewGuid(),
            ReportName     = reportName,
            Version        = version,
            ParametersHash = parametersHash,
            Format         = format.ToUpperInvariant(),
            S3Key          = s3Key,
            SizeBytes      = renderedBytes.Length,
            CreatedAt      = DateTime.UtcNow,
            ExpiresAt      = DateTime.UtcNow.Add(effectiveTtl),
        };

        await _repo.CreateAsync(snapshot);

        _logger.LogInformation(
            "[Snapshot] SAVED report={Name} v={Version} hash={Hash} fmt={Fmt} size={Size}b ttl={Ttl}h",
            reportName, version, parametersHash[..8] + "…", format,
            renderedBytes.Length, effectiveTtl.TotalHours);
    }

    // ─────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// S3 key: telerik/snapshots/{reportName}/{version}/{hash[0..2]}/{hash}.{ext}
    /// Shard by first 2 chars of hash to avoid S3 hot-key issue at scale.
    /// </summary>
    private static string BuildS3Key(
        string reportName, string version,
        string parametersHash, string format)
    {
        var ext   = format.ToUpperInvariant() switch
        {
            "PDF"  => "pdf",
            "HTML" => "html",
            "CSV"  => "csv",
            "XLSX" => "xlsx",
            _      => "bin"
        };

        return $"{SnapshotPrefix}/{reportName}/{version}/{parametersHash[..2]}/{parametersHash}.{ext}";
    }

    private static string FormatToContentType(string format) =>
        format.ToUpperInvariant() switch
        {
            "PDF"  => "application/pdf",
            "HTML" => "text/html",
            "CSV"  => "text/csv",
            "XLSX" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            _      => "application/octet-stream"
        };
}
