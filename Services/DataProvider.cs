using System.Data;
using Microsoft.Extensions.Logging;

namespace TelerikPOC.Services;

// ═══════════════════════════════════════════════════════════════
// Interface
// ═══════════════════════════════════════════════════════════════

/// <summary>
/// External data pipeline for Telerik Reporting.
///
/// Responsibility:
///   Accept a reportId (name:version) + runtime parameters,
///   execute the dynamic join query via FlatDatasetBuilder,
///   return a DataTable that will be injected into report.DataSource.
///
/// Telerik will NEVER touch SqlDataSource from inside the TRDP.
/// The engine becomes a pure layout renderer.
/// </summary>
public interface IDataProvider
{
    /// <summary>
    /// Returns a flat DataTable where columns are named {entityKey}_{fieldKey}.
    /// This DataTable is set directly on report.DataSource before rendering.
    /// </summary>
    Task<DataTable> GetFlatDataAsync(
        string reportName,
        string entityKeysJson,
        IDictionary<string, object?> parameters);
}

// ═══════════════════════════════════════════════════════════════
// Implementation
// ═══════════════════════════════════════════════════════════════

public sealed class DataProvider : IDataProvider
{
    private readonly FlatDatasetBuilder _builder;
    private readonly ILogger<DataProvider> _logger;

    public DataProvider(FlatDatasetBuilder builder, ILogger<DataProvider> logger)
    {
        _builder = builder ?? throw new ArgumentNullException(nameof(builder));
        _logger  = logger  ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DataTable> GetFlatDataAsync(
        string reportName,
        string entityKeysJson,
        IDictionary<string, object?> parameters)
    {
        _logger.LogInformation(
            "[DataProvider] Building dataset for report '{Name}' | params={Params}",
            reportName,
            string.Join(", ", parameters.Select(kv => $"{kv.Key}={kv.Value}")));

        var dt = await _builder.BuildAsync(entityKeysJson, parameters);

        _logger.LogInformation(
            "[DataProvider] Dataset ready: {Rows} rows for '{Name}'",
            dt.Rows.Count, reportName);

        return dt;
    }
}
