using System.Data;
using Dapper;
using TelerikPOC.Domain;

namespace TelerikPOC.Infrastructure;

public interface IReportDefinitionRepository
{
    Task<ReportDefinition?> GetAsync(string name, string version, string? tenantId = null);
    Task<ReportDefinition?> GetLatestAsync(string name, string? tenantId = null);
    Task<ReportDefinition> CreateAsync(ReportDefinition definition);
    Task ArchiveAsync(Guid id);
}

public sealed class ReportDefinitionRepository : IReportDefinitionRepository
{
    private readonly IDbConnection _db;

    public ReportDefinitionRepository(IDbConnection db)
        => _db = db ?? throw new ArgumentNullException(nameof(db));

    public async Task<ReportDefinition?> GetAsync(string name, string version, string? tenantId = null)
    {
        const string sql = """
            SELECT TOP 1 *
            FROM   rpt.ReportDefinitions
            WHERE  Name     = @Name
              AND  Version  = @Version
              AND  IsActive = 1
              AND  (@TenantId IS NULL OR TenantId = @TenantId)
            ORDER  BY CreatedAt DESC
            """;

        return await _db.QueryFirstOrDefaultAsync<ReportDefinition>(sql,
            new { Name = name, Version = version, TenantId = tenantId });
    }

    public async Task<ReportDefinition?> GetLatestAsync(string name, string? tenantId = null)
    {
        const string sql = """
            SELECT TOP 1 *
            FROM   rpt.ReportDefinitions
            WHERE  Name     = @Name
              AND  IsActive = 1
              AND  (@TenantId IS NULL OR TenantId = @TenantId)
            ORDER  BY CreatedAt DESC
            """;

        return await _db.QueryFirstOrDefaultAsync<ReportDefinition>(sql,
            new { Name = name, TenantId = tenantId });
    }

    public async Task<ReportDefinition> CreateAsync(ReportDefinition definition)
    {
        definition.Id = definition.Id == Guid.Empty ? Guid.NewGuid() : definition.Id;
        definition.CreatedAt = DateTime.UtcNow;
        definition.UpdatedAt = DateTime.UtcNow;
        definition.IsActive = true;

        const string sql = """
            INSERT INTO rpt.ReportDefinitions
                   (Id, Name, Version, S3Key, TenantId, DisplayName,
                    Description, EntityKeysJson, CreatedAt, UpdatedAt, IsActive)
            VALUES (@Id, @Name, @Version, @S3Key, @TenantId, @DisplayName,
                    @Description, @EntityKeysJson, @CreatedAt, @UpdatedAt, @IsActive)
            """;

        await _db.ExecuteAsync(sql, definition);
        return definition;
    }

    public async Task ArchiveAsync(Guid id)
    {
        const string sql = """
            UPDATE rpt.ReportDefinitions
            SET    IsActive = 0, UpdatedAt = @Now
            WHERE  Id = @Id
            """;

        await _db.ExecuteAsync(sql, new { Id = id, Now = DateTime.UtcNow });
    }
}
