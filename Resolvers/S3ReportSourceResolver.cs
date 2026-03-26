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

public sealed class S3ReportSourceResolver : IReportSourceResolver
{
    private readonly IAmazonS3 _s3;
    private readonly string _bucketName;
    private readonly IReportDefinitionRepository _definitionRepo;
    private readonly IDataProvider _dataProvider;
    private readonly ILogger<S3ReportSourceResolver> _logger;

    public S3ReportSourceResolver(
        IAmazonS3 s3,
        string bucketName,
        IReportDefinitionRepository definitionRepo,
        IDataProvider dataProvider,
        ILogger<S3ReportSourceResolver> logger)
    {
        _s3 = s3 ?? throw new ArgumentNullException(nameof(s3));
        _bucketName = string.IsNullOrWhiteSpace(bucketName)
            ? throw new ArgumentException("Bucket name is required.", nameof(bucketName))
            : bucketName;
        _definitionRepo = definitionRepo ?? throw new ArgumentNullException(nameof(definitionRepo));
        _dataProvider = dataProvider ?? throw new ArgumentNullException(nameof(dataProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public ReportSource Resolve(
        string report,
        OperationOrigin origin,
        IDictionary<string, object> currentParameterValues)
    {
        if (string.IsNullOrWhiteSpace(report))
            throw new ArgumentException("Report identifier is required.", nameof(report));

        return ResolveAsync(report, origin, currentParameterValues ?? new Dictionary<string, object>())
            .GetAwaiter()
            .GetResult();
    }

    private async Task<ReportSource> ResolveAsync(
        string reportId,
        OperationOrigin origin,
        IDictionary<string, object> rawParams)
    {
        var (tenantId, name, version) = ParseReportId(reportId);

        var definition = string.IsNullOrWhiteSpace(version)
            ? await _definitionRepo.GetLatestAsync(name, tenantId)
            : await _definitionRepo.GetAsync(name, version!, tenantId);

        if (definition == null)
            throw new InvalidOperationException(
                $"Report definition not found: report='{reportId}' (tenant='{tenantId ?? "default"}').");

        var trdpBytes = await DownloadTrdpAsync(definition.S3Key);
        var reportDocument = DeserializeReport(trdpBytes);

        if (origin != OperationOrigin.ResolveReportParameters)
        {
            var parameters = rawParams.ToDictionary(k => k.Key, v => (object?)v.Value);
            var dataTable = await _dataProvider.GetFlatDataAsync(
                reportName: definition.Name,
                entityKeysJson: definition.EntityKeysJson,
                parameters: parameters);

            reportDocument.DataSource = dataTable;
            _logger.LogInformation("Injected {Rows} rows into report '{Report}'.", dataTable.Rows.Count, definition.Name);
        }
        else
        {
            _logger.LogDebug("Skipping dataset injection for parameter resolution on '{Report}'.", definition.Name);
        }

        var source = new InstanceReportSource { ReportDocument = reportDocument };

        foreach (var kv in rawParams)
            source.Parameters.Add(kv.Key, kv.Value);

        return source;
    }

    private async Task<byte[]> DownloadTrdpAsync(string s3Key)
    {
        if (string.IsNullOrWhiteSpace(s3Key))
            throw new InvalidOperationException("Report definition contains empty S3Key.");

        try
        {
            using var response = await _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = s3Key
            });

            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms);
            return ms.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw new FileNotFoundException(
                $"TRDP not found in S3. Bucket='{_bucketName}', Key='{s3Key}'.", ex);
        }
    }

    private static Report DeserializeReport(byte[] trdpBytes)
    {
        try
        {
            using var stream = new MemoryStream(trdpBytes);
            var packager = new ReportPackager();
            var doc = packager.UnpackageDocument(stream);

            return doc as Report
                   ?? throw new InvalidOperationException($"Unpackaged document is not Telerik.Reporting.Report: {doc?.GetType().FullName}");
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException("Failed to deserialize TRDP from S3 payload.", ex);
        }
    }

    private static (string? tenant, string name, string? version) ParseReportId(string reportId)
    {
        string? tenant = null;
        string? version = null;

        var slashIdx = reportId.IndexOf('/');
        if (slashIdx >= 0)
        {
            tenant = reportId[..slashIdx].Trim();
            reportId = reportId[(slashIdx + 1)..];
        }

        var colonIdx = reportId.IndexOf(':');
        string name;
        if (colonIdx >= 0)
        {
            name = reportId[..colonIdx].Trim();
            version = reportId[(colonIdx + 1)..].Trim();
        }
        else
        {
            name = reportId.Trim();
        }

        if (string.IsNullOrWhiteSpace(name))
            throw new InvalidOperationException($"Invalid report identifier '{reportId}'.");

        return (
            string.IsNullOrWhiteSpace(tenant) ? null : tenant,
            name,
            string.IsNullOrWhiteSpace(version) ? null : version
        );
    }
}
