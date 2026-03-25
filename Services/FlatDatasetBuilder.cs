using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using TelerikPOC.Domain;
using TelerikPOC.Infrastructure;

namespace TelerikPOC.Services;

/// <summary>
/// Builds a flat DataTable by dynamically JOINing rpt.ReportingEntities views.
///
/// How it works:
///   1. Accept a list of EntityKeys the report needs (from ReportDefinition.EntityKeysJson)
///   2. Load entity + field metadata from IMetadataRepository (cached)
///   3. Topologically sort entities via ReportingRelationships (parent → child)
///   4. Build a single SELECT … FROM … LEFT JOIN … SQL
///   5. Apply parameters as WHERE clauses
///   6. Execute and return DataTable
///
/// Column naming convention in the returned DataTable:
///   {entityKey}_{fieldKey}  (e.g., "participant_gender", "study_studyCode")
///   TRDP field bindings must use the same convention:  =Fields.participant_gender
///
/// SQL injection protection:
///   - ViewName and FieldKey come from rpt metadata (trusted, DB-controlled)
///   - Parameter values are always bound via SqlParameter (@p0, @p1 …)
///   - Entity/field keys are never interpolated with user input
/// </summary>
public sealed class FlatDatasetBuilder
{
    private readonly IMetadataRepository _meta;
    private readonly string              _connectionString;
    private readonly ILogger<FlatDatasetBuilder> _logger;

    public FlatDatasetBuilder(
        IMetadataRepository meta,
        string connectionString,
        ILogger<FlatDatasetBuilder> logger)
    {
        _meta             = meta             ?? throw new ArgumentNullException(nameof(meta));
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _logger           = logger           ?? throw new ArgumentNullException(nameof(logger));
    }

    // ─────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Build and execute the flat query.
    /// </summary>
    /// <param name="entityKeysJson">JSON array from ReportDefinition.EntityKeysJson, e.g. ["study","participant","site"]</param>
    /// <param name="parameters">Runtime parameters used as WHERE clause filters</param>
    public async Task<DataTable> BuildAsync(
        string entityKeysJson,
        IDictionary<string, object?> parameters)
    {
        // ── 1. Parse entity keys ─────────────────────────────────────
        var entityKeys = JsonSerializer.Deserialize<string[]>(entityKeysJson)
                         ?? Array.Empty<string>();

        if (entityKeys.Length == 0)
            throw new InvalidOperationException(
                "ReportDefinition.EntityKeysJson is empty — cannot build dataset.");

        // ── 2. Load metadata (cached) ────────────────────────────────
        var allEntities      = await _meta.GetEntitiesAsync();
        var allFieldsByEntity = await _meta.GetFieldsByEntityAsync();
        var allRelationships  = await _meta.GetRelationshipsAsync();

        // ── 3. Resolve requested entities ───────────────────────────
        var entities = entityKeys
            .Select(k => allEntities.TryGetValue(k, out var e) ? e
                         : throw new InvalidOperationException($"Entity '{k}' not found in rpt.ReportingEntities"))
            .ToList();

        // ── 4. Build SQL ─────────────────────────────────────────────
        var (sql, sqlParams) = BuildSql(entities, allFieldsByEntity, allRelationships, parameters);

        _logger.LogDebug("[FlatDatasetBuilder] Executing SQL:\n{Sql}", sql);

        // ── 5. Execute ───────────────────────────────────────────────
        return await ExecuteQueryAsync(sql, sqlParams);
    }

    // ─────────────────────────────────────────────────────────────────
    // SQL construction
    // ─────────────────────────────────────────────────────────────────

    private (string sql, List<SqlParameter> sqlParams) BuildSql(
        List<ReportingEntity> entities,
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity,
        IReadOnlyList<ReportingRelationship> relationships,
        IDictionary<string, object?> parameters)
    {
        var sqlParams  = new List<SqlParameter>();
        var sb         = new StringBuilder();
        var paramIndex = 0;

        // Alias map: entityId → SQL alias (e, e1, e2 …)
        var aliases = entities
            .Select((e, i) => (e.EntityId, Alias: i == 0 ? "e0" : $"e{i}"))
            .ToDictionary(x => x.EntityId, x => x.Alias);

        // ── SELECT ──────────────────────────────────────────────────
        sb.AppendLine("SELECT");
        bool firstCol = true;

        foreach (var entity in entities)
        {
            if (!fieldsByEntity.TryGetValue(entity.EntityId, out var fields)) continue;
            var alias = aliases[entity.EntityId];

            foreach (var field in fields.Where(f => f.IsActive))
            {
                if (!firstCol) sb.AppendLine("      ,");
                // Column alias: {entityKey}_{fieldKey} — matches TRDP field bindings
                sb.Append($"      {alias}.[{field.FieldKey}]"
                         + $" AS [{entity.EntityKey}_{field.FieldKey}]");
                firstCol = false;
            }
        }

        if (firstCol)
            sb.AppendLine("      1 AS _empty"); // safety: no fields configured

        sb.AppendLine();

        // ── FROM (root entity — always first in the list) ────────────
        var root = entities[0];
        sb.AppendLine($"FROM   {root.ViewName} AS {aliases[root.EntityId]}");

        // ── JOINs ────────────────────────────────────────────────────
        // For each subsequent entity, find the relationship that connects it
        // to one of the already-included entities.
        var included = new HashSet<int> { root.EntityId };

        foreach (var entity in entities.Skip(1))
        {
            var rel = FindRelationship(relationships, entity.EntityId, included);

            if (rel == null)
            {
                _logger.LogWarning(
                    "[FlatDatasetBuilder] No relationship found connecting entity '{Key}' to the existing join tree. " +
                    "Using CROSS JOIN fallback — check rpt.ReportingRelationships.", entity.EntityKey);

                // Fallback: include without join condition (may be intentional for reference data)
                sb.AppendLine($"CROSS  JOIN {entity.ViewName} AS {aliases[entity.EntityId]}");
            }
            else
            {
                var parentAlias = aliases[rel.ParentEntityId];
                var childAlias  = aliases[rel.ChildEntityId];

                // Resolve field keys from metadata
                var parentFields = fieldsByEntity.GetValueOrDefault(rel.ParentEntityId);
                var childFields  = fieldsByEntity.GetValueOrDefault(rel.ChildEntityId);

                var parentField  = parentFields?.FirstOrDefault(f => f.FieldId == rel.ParentFieldId);
                var childField   = childFields?.FirstOrDefault(f => f.FieldId == rel.ChildFieldId);

                if (parentField == null || childField == null)
                {
                    _logger.LogWarning(
                        "[FlatDatasetBuilder] Cannot resolve join fields for relationship {Id}. Skipping.",
                        rel.RelationshipId);
                    continue;
                }

                var joinKeyword = rel.JoinType.ToUpperInvariant() switch
                {
                    "LEFT"  => "LEFT  JOIN",
                    "RIGHT" => "RIGHT JOIN",
                    _       => "INNER JOIN"
                };

                sb.AppendLine($"{joinKeyword} {entity.ViewName} AS {childAlias}");
                sb.AppendLine($"       ON  {parentAlias}.[{parentField.FieldKey}]"
                            + $" = {childAlias}.[{childField.FieldKey}]");
            }

            included.Add(entity.EntityId);
        }

        // ── WHERE (from parameters) ──────────────────────────────────
        // Parameters are matched to fields by name: "StudyId" → look for a field
        // whose DisplayName or FieldKey matches (case-insensitive).
        // Only filterable fields are allowed as WHERE conditions.
        var whereClause = BuildWhereClause(entities, fieldsByEntity, aliases,
                                           parameters, sqlParams, ref paramIndex);
        if (whereClause.Length > 0)
        {
            sb.AppendLine("WHERE");
            sb.Append(whereClause);
        }

        return (sb.ToString(), sqlParams);
    }

    // ─────────────────────────────────────────────────────────────────
    // WHERE clause builder
    // ─────────────────────────────────────────────────────────────────

    private static string BuildWhereClause(
        List<ReportingEntity> entities,
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity,
        Dictionary<int, string> aliases,
        IDictionary<string, object?> parameters,
        List<SqlParameter> sqlParams,
        ref int paramIndex)
    {
        if (parameters.Count == 0) return string.Empty;

        var conditions = new StringBuilder();
        bool firstCond = true;

        foreach (var (paramKey, paramValue) in parameters)
        {
            if (paramValue == null) continue;

            // Try to find a matching filterable field across all involved entities
            ReportingField? matchedField = null;
            ReportingEntity? matchedEntity = null;

            foreach (var entity in entities)
            {
                if (!fieldsByEntity.TryGetValue(entity.EntityId, out var fields)) continue;

                var field = fields.FirstOrDefault(f =>
                    f.IsFilterable &&
                    (string.Equals(f.FieldKey, paramKey, StringComparison.OrdinalIgnoreCase) ||
                     string.Equals(f.DisplayName, paramKey, StringComparison.OrdinalIgnoreCase)));

                if (field != null)
                {
                    matchedField  = field;
                    matchedEntity = entity;
                    break;
                }
            }

            if (matchedField == null || matchedEntity == null) continue;

            var alias    = aliases[matchedEntity.EntityId];
            var pName    = $"@p{paramIndex++}";

            if (!firstCond) conditions.AppendLine("  AND");
            conditions.Append($"      {alias}.[{matchedField.FieldKey}] = {pName}");
            sqlParams.Add(new SqlParameter(pName, paramValue));
            firstCond = false;
        }

        return conditions.ToString();
    }

    // ─────────────────────────────────────────────────────────────────
    // Relationship finder
    // ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Find the relationship that connects <paramref name="childEntityId"/>
    /// to one of the already-included entities.
    /// Prefers lower HopWeight (direct joins preferred over transitive ones).
    /// </summary>
    private static ReportingRelationship? FindRelationship(
        IReadOnlyList<ReportingRelationship> relationships,
        int childEntityId,
        HashSet<int> includedEntityIds)
    {
        return relationships
            .Where(r => r.IsActive)
            .Where(r =>
                // child entity is the one we're adding
                (r.ChildEntityId == childEntityId && includedEntityIds.Contains(r.ParentEntityId)) ||
                // BIDIRECTIONAL: also allow child→parent if direction allows
                (r.Direction == "BIDIRECTIONAL" &&
                 r.ParentEntityId == childEntityId &&
                 includedEntityIds.Contains(r.ChildEntityId)))
            .OrderBy(r => r.HopWeight)
            .FirstOrDefault();
    }

    // ─────────────────────────────────────────────────────────────────
    // Query execution
    // ─────────────────────────────────────────────────────────────────

    private async Task<DataTable> ExecuteQueryAsync(string sql, List<SqlParameter> sqlParams)
    {
        await using var conn    = new SqlConnection(_connectionString);
        await using var command = new SqlCommand(sql, conn);
        command.CommandTimeout  = 120; // 2 minutes max

        foreach (var p in sqlParams)
            command.Parameters.Add(p);

        await conn.OpenAsync();

        var dt      = new DataTable("FlatDataset");
        var adapter = new SqlDataAdapter(command);
        adapter.Fill(dt);

        _logger.LogInformation(
            "[FlatDatasetBuilder] Query returned {Rows} rows, {Cols} columns.",
            dt.Rows.Count, dt.Columns.Count);

        return dt;
    }
}
