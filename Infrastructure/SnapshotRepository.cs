using System.Data;
using Dapper;
using TelerikPOC.Domain;

namespace TelerikPOC.Infrastructure;

// ═══════════════════════════════════════════════════════════════
// Interface
// ═══════════════════════════════════════════════════════════════

public interface ISnapshotRepository
{
    /// <summary>
    /// Find a snapshot by (reportName + version + parametersHash + format).
    /// Returns null on cache miss.
    /// </summary>
    Task<ReportSnapshot?> FindAsync(
        string reportName, string version,
        string parametersHash, string format);

    /// <summary>Persist a new snapshot record after upload to S3.</summary>
    Task<ReportSnapshot> CreateAsync(ReportSnapshot snapshot);

    /// <summary>Hard-delete expired snapshots (call from a background job).</summary>
    Task DeleteExpiredAsync();
}

// ═══════════════════════════════════════════════════════════════
// Implementation
// ═══════════════════════════════════════════════════════════════

public sealed class SnapshotRepository : ISnapshotRepository
{
    private readonly IDbConnection _db;

    public SnapshotRepository(IDbConnection db)
        => _db = db ?? throw new ArgumentNullException(nameof(db));

    // ── Schema (see migration 002) ─────────────────────────────────
    //
    // CREATE TABLE rpt.ReportSnapshots (
    //   Id             UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    //   ReportName     NVARCHAR(200)    NOT NULL,
    //   Version        NVARCHAR(50)     NOT NULL,
    //   ParametersHash NVARCHAR(64)     NOT NULL,  -- SHA-256 hex
    //   Format         NVARCHAR(20)     NOT NULL,  -- PDF | HTML | CSV | XLSX
    //   S3Key          NVARCHAR(500)    NOT NULL,
    //   SizeBytes      BIGINT           NOT NULL,
    //   CreatedAt      DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    //   ExpiresAt      DATETIME2        NOT NULL
    // );
    // CREATE UNIQUE INDEX UX_Snapshot_Hash
    //   ON rpt.ReportSnapshots(ReportName, Version, ParametersHash, Format)
    //   WHERE ExpiresAt > GETUTCDATE();
    // ──────────────────────────────────────────────────────────────

    public async Task<ReportSnapshot?> FindAsync(
        string reportName, string version,
        string parametersHash, string format)
    {
        const string sql = """
            SELECT TOP 1
                   Id, ReportName, Version, ParametersHash, Format,
                   S3Key, SizeBytes, CreatedAt, ExpiresAt
            FROM   rpt.ReportSnapshots
            WHERE  ReportName     = @ReportName
              AND  Version        = @Version
              AND  ParametersHash = @ParametersHash
              AND  Format         = @Format
              AND  ExpiresAt      > GETUTCDATE()
            """;

        return await _db.QueryFirstOrDefaultAsync<ReportSnapshot>(sql, new
        {
            ReportName     = reportName,
            Version        = version,
            ParametersHash = parametersHash,
            Format         = format
        });
    }

    public async Task<ReportSnapshot> CreateAsync(ReportSnapshot snapshot)
    {
        const string sql = """
            INSERT INTO rpt.ReportSnapshots
                   (Id, ReportName, Version, ParametersHash, Format,
                    S3Key, SizeBytes, CreatedAt, ExpiresAt)
            VALUES (@Id, @ReportName, @Version, @ParametersHash, @Format,
                    @S3Key, @SizeBytes, @CreatedAt, @ExpiresAt)
            """;

        await _db.ExecuteAsync(sql, snapshot);
        return snapshot;
    }

    public async Task DeleteExpiredAsync()
    {
        const string sql = "DELETE FROM rpt.ReportSnapshots WHERE ExpiresAt <= GETUTCDATE()";
        await _db.ExecuteAsync(sql);
    }
}
