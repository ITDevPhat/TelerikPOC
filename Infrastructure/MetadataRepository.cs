using System.Data;
using Dapper;
using Microsoft.Extensions.Caching.Memory;
using TelerikPOC.Domain;

namespace TelerikPOC.Infrastructure;

// ═══════════════════════════════════════════════════════════════
// Interface
// ═══════════════════════════════════════════════════════════════

/// <summary>
/// Loads rpt.* metadata.  Everything is cached in IMemoryCache to avoid
/// repeated DB round-trips per render request.
/// </summary>
public interface IMetadataRepository
{
    /// <summary>All active entities, keyed by EntityKey.</summary>
    Task<IReadOnlyDictionary<string, ReportingEntity>> GetEntitiesAsync();

    /// <summary>All active fields grouped by EntityId.</summary>
    Task<IReadOnlyDictionary<int, IReadOnlyList<ReportingField>>> GetFieldsByEntityAsync();

    /// <summary>All active relationships.</summary>
    Task<IReadOnlyList<ReportingRelationship>> GetRelationshipsAsync();

    /// <summary>Force-invalidate the metadata cache (call after schema changes).</summary>
    void InvalidateCache();
}

// ═══════════════════════════════════════════════════════════════
// Implementation
// ═══════════════════════════════════════════════════════════════

public sealed class MetadataRepository : IMetadataRepository
{
    private readonly IDbConnection  _db;
    private readonly IMemoryCache   _cache;

    private const string EntitiesKey      = "rpt_entities";
    private const string FieldsByEntityKey = "rpt_fields_by_entity";
    private const string RelationshipsKey  = "rpt_relationships";
    private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(10);

    public MetadataRepository(IDbConnection db, IMemoryCache cache)
    {
        _db    = db    ?? throw new ArgumentNullException(nameof(db));
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
    }

    // ── Entities ──────────────────────────────────────────────────

    public async Task<IReadOnlyDictionary<string, ReportingEntity>> GetEntitiesAsync()
    {
        return await _cache.GetOrCreateAsync(EntitiesKey, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = CacheTtl;

            const string sql = """
                SELECT EntityId, EntityKey, DisplayName, ViewName,
                       EntityType, MaxSelectableFields, MaxGroupByFields, IsActive
                FROM   rpt.ReportingEntities
                WHERE  IsActive = 1
                """;

            var rows = await _db.QueryAsync<ReportingEntity>(sql);
            return (IReadOnlyDictionary<string, ReportingEntity>)
                   rows.ToDictionary(e => e.EntityKey, StringComparer.OrdinalIgnoreCase);
        }) ?? new Dictionary<string, ReportingEntity>();
    }

    // ── Fields ─────────────────────────────────────────────────────

    public async Task<IReadOnlyDictionary<int, IReadOnlyList<ReportingField>>> GetFieldsByEntityAsync()
    {
        return await _cache.GetOrCreateAsync(FieldsByEntityKey, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = CacheTtl;

            const string sql = """
                SELECT FieldId, EntityId, FieldKey, DisplayName,
                       DataType, FieldRole, IsFilterable, IsGroupable,
                       IsSensitive, IsActive, IsDefault
                FROM   rpt.ReportingFields
                WHERE  IsActive = 1
                ORDER  BY EntityId, FieldId
                """;

            var rows   = await _db.QueryAsync<ReportingField>(sql);
            var grouped = rows
                .GroupBy(f => f.EntityId)
                .ToDictionary(
                    g => g.Key,
                    g => (IReadOnlyList<ReportingField>)g.ToList());

            return (IReadOnlyDictionary<int, IReadOnlyList<ReportingField>>)grouped;
        }) ?? new Dictionary<int, IReadOnlyList<ReportingField>>();
    }

    // ── Relationships ──────────────────────────────────────────────

    public async Task<IReadOnlyList<ReportingRelationship>> GetRelationshipsAsync()
    {
        return await _cache.GetOrCreateAsync(RelationshipsKey, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = CacheTtl;

            const string sql = """
                SELECT RelationshipId, ParentEntityId, ParentFieldId,
                       ChildEntityId, ChildFieldId, JoinType, Cardinality,
                       Direction, HopWeight, MaxJoinDepth,
                       IsRequired, IsActive
                FROM   rpt.ReportingRelationships
                WHERE  IsActive = 1
                ORDER  BY HopWeight ASC
                """;

            var rows = await _db.QueryAsync<ReportingRelationship>(sql);
            return (IReadOnlyList<ReportingRelationship>)rows.ToList();
        }) ?? new List<ReportingRelationship>();
    }

    // ── Cache invalidation ─────────────────────────────────────────

    public void InvalidateCache()
    {
        _cache.Remove(EntitiesKey);
        _cache.Remove(FieldsByEntityKey);
        _cache.Remove(RelationshipsKey);
    }
}
