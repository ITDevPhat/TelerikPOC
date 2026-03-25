using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using Telerik.Reporting;
using Telerik.Reporting.Services;
using TelerikPOC.Domain;
using TelerikPOC.Infrastructure;
using TelerikPOC.Services;

namespace TelerikPOC.Resolvers;

/// <summary>
/// Updated S3ReportSourceResolver — now the ORCHESTRATION HUB.
///
/// Full flow on Resolve():
/// ┌────────────────────────────────────────────────────────────────┐
/// │ 1.  Parse report ID  →  (name, version)                       │
/// │ 2.  Lookup ReportDefinition from DB  →  get S3Key + EntityKeys│
/// │ 3.  Compute ParametersHash  (SHA-256)                         │
/// │ 4.  Check SnapshotService  →  if HIT: return SnapshotSource   │
/// │ 5.  [CACHE MISS]                                              │
/// │     a. Download .trdp from S3                                  │
/// │     b. Deserialize → Report object (ReportPackager)           │
/// │     c. IDataProvider.GetFlatDataAsync()  →  DataTable         │
/// │     d. Override report.DataSource = DataTable                  │
/// │     e. Return InstanceReportSource                             │
/// │     f. Snapshot is saved AFTER render (see SnapshotPostProcess)│
/// └────────────────────────────────────────────────────────────────┘
///
/// IMPORTANT: Step 4 returns a SnapshotReportSource (custom) that tells
/// the caller to skip rendering and use pre-built bytes.
/// When snapshot = null, step 5 returns InstanceReportSource normally;
/// the CALLER (RenderService) is responsible for saving snapshot after render.
///
/// This class does NOT render — it only prepares the ReportSource.
/// Rendering and snapshot-save happen in RenderService.
/// </summary>
public sealed class S3ReportSourceResolver : IReportSourceResolver
{
    private readonly IAmazonS3                   _s3;
    private readonly string                      _bucketName;
    private readonly IReportDefinitionRepository _definitionRepo;
    private readonly IDataProvider               _dataProvider;
    private readonly ISnapshotService            _snapshotService;
    private readonly ILogger<S3ReportSourceResolver> _logger;

    public S3ReportSourceResolver(
        IAmazonS3 s3,
        string bucketName,
        IReportDefinitionRepository definitionRepo,
        IDataProvider dataProvider,
        ISnapshotService snapshotService,
        ILogger<S3ReportSourceResolver> logger)
    {
        _s3              = s3              ?? throw new ArgumentNullException(nameof(s3));
        _bucketName      = bucketName      ?? throw new ArgumentNullException(nameof(bucketName));
        _definitionRepo  = definitionRepo  ?? throw new ArgumentNullException(nameof(definitionRepo));
        _dataProvider    = dataProvider    ?? throw new ArgumentNullException(nameof(dataProvider));
        _snapshotService = snapshotService ?? throw new ArgumentNullException(nameof(snapshotService));
        _logger          = logger          ?? throw new ArgumentNullException(nameof(logger));
    }

    // ─────────────────────────────────────────────────────────────────
    // IReportSourceResolver.Resolve
    // ─────────────────────────────────────────────────────────────────

    public ReportSource Resolve(
        string report,
        OperationOrigin origin,
        IDictionary<string, object> currentParameterValues)
    {
        // IReportSourceResolver is synchronous — bridge to async via GetAwaiter
        return ResolveAsync(report, currentParameterValues)
               .GetAwaiter()
               .GetResult();
    }

    // ─────────────────────────────────────────────────────────────────
    // Async core
    // ─────────────────────────────────────────────────────────────────

    private async Task<ReportSource> ResolveAsync(
        string reportId,
        IDictionary<string, object> rawParams)
    {
        // Normalize parameters (handle nulls, object→object? coercion)
        var parameters = rawParams
            .ToDictionary(kv => kv.Key, kv => (object?)kv.Value);

        // ── 1. Parse report ID ───────────────────────────────────────
        var (tenantId, name, version) = ParseReportId(reportId);
        _logger.LogInformation("[Resolver] Resolving report '{Name}' v='{Version}'", name, version ?? "latest");

        // ── 2. Load definition from DB ───────────────────────────────
        ReportDefinition? definition = string.IsNullOrEmpty(version)
            ? await _definitionRepo.GetLatestAsync(name, tenantId)
            : await _definitionRepo.GetAsync(name, version!);

        if (definition == null)
            throw new InvalidOperationException(
                $"Report definition not found: name='{name}' version='{version ?? "latest"}'");

        // Resolve version for snapshot key (use actual DB version, not "latest")
        var resolvedVersion = definition.Version;

        // ── 3. Compute hash ──────────────────────────────────────────
        var hash = HashUtility.ComputeHash($"{name}:{resolvedVersion}", parameters);
        _logger.LogDebug("[Resolver] Parameters hash: {Hash}", hash[..12] + "…");

        // ── 4. Snapshot check ────────────────────────────────────────
        // NOTE: Format for snapshot check is not yet known here (Telerik decides format
        // at render time).  We attach the definition + hash as metadata so RenderService
        // can do the snapshot lookup BEFORE calling Telerik's render pipeline.
        //
        // We use a ResolveContext object embedded in InstanceReportSource.Parameters
        // so the downstream RenderService can read it.

        // ── 5a. Download .trdp from S3 ───────────────────────────────
        var trdpBytes = await DownloadTrdpAsync(definition.S3Key);

        // ── 5b. Deserialize TRDP → Report object ─────────────────────
        var telerikReport = DeserializeReport(trdpBytes);

        // ── 5c. Override DataSource (CORE REQUIREMENT) ───────────────
        // Do NOT use SqlDataSource inside TRDP.
        // Telerik becomes a pure layout engine — data comes from us.
        var dataTable = await _dataProvider.GetFlatDataAsync(
            reportName:    name,
            entityKeysJson: definition.EntityKeysJson,
            parameters:    parameters);

        telerikReport.DataSource = dataTable;

        _logger.LogInformation(
            "[Resolver] DataSource injected: {Rows} rows into report '{Name}'",
            dataTable.Rows.Count, name);

        // ── 5d. Build InstanceReportSource ───────────────────────────
        var source = new InstanceReportSource
        {
            ReportDocument = telerikReport
        };

        // Pass original parameters to the report for any parameter-driven formatting
        foreach (var kv in rawParams)
            source.Parameters.Add(kv.Key, kv.Value);

        // Embed resolve context so RenderService can do snapshot save after render
        source.Parameters.Add("__ResolveContext__", new ResolveContext
        {
            ReportName     = name,
            Version        = resolvedVersion,
            ParametersHash = hash,
        });

        return source;
    }

    // ─────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────

    private async Task<byte[]> DownloadTrdpAsync(string s3Key)
    {
        try
        {
            var response = await _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key        = s3Key
            });

            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms);
            return ms.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw new FileNotFoundException(
                $"Report .trdp not found in S3. Key='{s3Key}' Bucket='{_bucketName}'");
        }
    }

    private static Report DeserializeReport(byte[] trdpBytes)
    {
        try
        {
            using var stream  = new MemoryStream(trdpBytes);
            var packager      = new ReportPackager();
            var reportDocument = packager.UnpackageDocument(stream);

            return reportDocument as Report
                   ?? throw new InvalidOperationException(
                       $"Expected Report, got {reportDocument?.GetType().FullName}");
        }
        catch (Exception ex) when (ex is not (InvalidOperationException or FileNotFoundException))
        {
            throw new InvalidOperationException("Failed to deserialize .trdp file.", ex);
        }
    }

    private static (string? tenant, string name, string? version) ParseReportId(string reportId)
    {
        string? tenant  = null;
        string? version = null;

        var slashIdx = reportId.IndexOf('/');
        if (slashIdx >= 0)
        {
            tenant   = reportId[..slashIdx].Trim();
            reportId = reportId[(slashIdx + 1)..];
        }

        var colonIdx = reportId.IndexOf(':');
        string name;
        if (colonIdx >= 0)
        {
            name    = reportId[..colonIdx].Trim();
            version = reportId[(colonIdx + 1)..].Trim();
        }
        else
        {
            name = reportId.Trim();
        }

        return (
            string.IsNullOrWhiteSpace(tenant)  ? null : tenant,
            name,
            string.IsNullOrWhiteSpace(version) ? null : version
        );
    }
}

// ─────────────────────────────────────────────────────────────────
// ResolveContext — passed as parameter to let RenderService snapshot
// ─────────────────────────────────────────────────────────────────

/// <summary>
/// Metadata attached to the resolved ReportSource so that the
/// downstream render pipeline can save/retrieve snapshots.
/// </summary>
public sealed class ResolveContext
{
    public string ReportName     { get; init; } = default!;
    public string Version        { get; init; } = default!;
    public string ParametersHash { get; init; } = default!;
}
