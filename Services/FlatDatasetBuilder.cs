using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using TelerikPOC.Domain;
using TelerikPOC.Infrastructure;

namespace TelerikPOC.Services;

public sealed class FlatDatasetBuilder
{
    private readonly IMetadataRepository _meta;
    private readonly string _connectionString;
    private readonly ILogger<FlatDatasetBuilder> _logger;

    public FlatDatasetBuilder(
        IMetadataRepository meta,
        string connectionString,
        ILogger<FlatDatasetBuilder> logger)
    {
        _meta = meta ?? throw new ArgumentNullException(nameof(meta));
        _connectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentException("Connection string is required.", nameof(connectionString))
            : connectionString;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DataTable> BuildAsync(
        string entityKeysJson,
        IDictionary<string, object?> parameters)
    {
        var entityKeys = JsonSerializer.Deserialize<string[]>(entityKeysJson)
                         ?? Array.Empty<string>();

        if (entityKeys.Length == 0)
            throw new InvalidOperationException("ReportDefinition.EntityKeysJson is empty.");

        var entitiesByKey = await _meta.GetEntitiesAsync();
        var entitiesById = entitiesByKey.Values.ToDictionary(x => x.EntityId);
        var allFieldsByEntity = await _meta.GetFieldsByEntityAsync();
        var allRelationships = await _meta.GetRelationshipsAsync();

        var requiredEntities = entityKeys
            .Select(k => entitiesByKey.TryGetValue(k, out var entity)
                ? entity
                : throw new InvalidOperationException($"Entity '{k}' was not found in rpt.ReportingEntities."))
            .ToList();

        var joinPlan = BuildJoinPlan(requiredEntities, entitiesById, allRelationships, allFieldsByEntity);
        var (sql, sqlParams) = BuildSql(joinPlan, allFieldsByEntity, parameters);

        _logger.LogDebug("[FlatDatasetBuilder] Executing SQL:\n{Sql}", sql);
        return await ExecuteQueryAsync(sql, sqlParams);
    }

    private sealed record JoinStep(
        ReportingEntity Source,
        ReportingEntity Target,
        ReportingField SourceField,
        ReportingField TargetField,
        string JoinType);

    private sealed record JoinPlan(
        ReportingEntity Root,
        IReadOnlyList<ReportingEntity> OrderedEntities,
        IReadOnlyList<JoinStep> Steps);

    private sealed record GraphEdge(
        int SourceEntityId,
        int TargetEntityId,
        int SourceFieldId,
        int TargetFieldId,
        int RelationshipId,
        string JoinType,
        int Weight);

    private static JoinPlan BuildJoinPlan(
        List<ReportingEntity> requiredEntities,
        IReadOnlyDictionary<int, ReportingEntity> entitiesById,
        IReadOnlyList<ReportingRelationship> relationships,
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity)
    {
        var root = requiredEntities[0];
        var steps = new List<JoinStep>();
        var included = new HashSet<int> { root.EntityId };

        foreach (var target in requiredEntities.Skip(1))
        {
            var path = FindShortestPath(root.EntityId, target.EntityId, relationships);
            if (path.Count == 0)
                throw new InvalidOperationException($"No join path found from '{root.EntityKey}' to '{target.EntityKey}'.");

            foreach (var edge in path)
            {
                if (included.Contains(edge.TargetEntityId))
                    continue;

                if (!included.Contains(edge.SourceEntityId))
                    throw new InvalidOperationException(
                        $"Invalid join path order. Missing source entity id={edge.SourceEntityId} for target id={edge.TargetEntityId}.");

                if (!entitiesById.TryGetValue(edge.SourceEntityId, out var sourceEntity) ||
                    !entitiesById.TryGetValue(edge.TargetEntityId, out var targetEntity))
                {
                    throw new InvalidOperationException(
                        $"Entity metadata missing for join edge source={edge.SourceEntityId}, target={edge.TargetEntityId}.");
                }

                var sourceField = ResolveField(fieldsByEntity, edge.SourceEntityId, edge.SourceFieldId, edge.RelationshipId, "source");
                var targetField = ResolveField(fieldsByEntity, edge.TargetEntityId, edge.TargetFieldId, edge.RelationshipId, "target");

                steps.Add(new JoinStep(sourceEntity, targetEntity, sourceField, targetField, edge.JoinType));
                included.Add(edge.TargetEntityId);
            }
        }

        var ordered = new List<ReportingEntity> { root };
        ordered.AddRange(steps.Select(s => s.Target));

        return new JoinPlan(root, ordered, steps);
    }

    private static List<GraphEdge> FindShortestPath(
        int rootEntityId,
        int targetEntityId,
        IReadOnlyList<ReportingRelationship> relationships)
    {
        var adjacency = new Dictionary<int, List<GraphEdge>>();

        foreach (var rel in relationships.Where(r => r.IsActive))
        {
            AddEdge(new GraphEdge(
                SourceEntityId: rel.ParentEntityId,
                TargetEntityId: rel.ChildEntityId,
                SourceFieldId: rel.ParentFieldId,
                TargetFieldId: rel.ChildFieldId,
                RelationshipId: rel.RelationshipId,
                JoinType: rel.JoinType,
                Weight: rel.HopWeight + (string.Equals(rel.JoinType, "LEFT", StringComparison.OrdinalIgnoreCase) ? 1 : 0)));

            if (string.Equals(rel.Direction, "BOTH", StringComparison.OrdinalIgnoreCase))
            {
                AddEdge(new GraphEdge(
                    SourceEntityId: rel.ChildEntityId,
                    TargetEntityId: rel.ParentEntityId,
                    SourceFieldId: rel.ChildFieldId,
                    TargetFieldId: rel.ParentFieldId,
                    RelationshipId: rel.RelationshipId,
                    JoinType: rel.JoinType,
                    Weight: rel.HopWeight + (string.Equals(rel.JoinType, "LEFT", StringComparison.OrdinalIgnoreCase) ? 1 : 0)));
            }
        }

        void AddEdge(GraphEdge edge)
        {
            if (!adjacency.TryGetValue(edge.SourceEntityId, out var list))
            {
                list = new List<GraphEdge>();
                adjacency[edge.SourceEntityId] = list;
            }
            list.Add(edge);
        }

        var pq = new PriorityQueue<int, int>();
        var dist = new Dictionary<int, int> { [rootEntityId] = 0 };
        var prev = new Dictionary<int, GraphEdge>();

        pq.Enqueue(rootEntityId, 0);

        while (pq.TryDequeue(out var current, out _))
        {
            if (current == targetEntityId)
                break;

            if (!adjacency.TryGetValue(current, out var nextEdges))
                continue;

            foreach (var edge in nextEdges)
            {
                var nextCost = dist[current] + edge.Weight;
                if (!dist.TryGetValue(edge.TargetEntityId, out var existing) || nextCost < existing)
                {
                    dist[edge.TargetEntityId] = nextCost;
                    prev[edge.TargetEntityId] = edge;
                    pq.Enqueue(edge.TargetEntityId, nextCost);
                }
            }
        }

        if (!prev.ContainsKey(targetEntityId))
            return [];

        var path = new List<GraphEdge>();
        var cursor = targetEntityId;

        while (prev.TryGetValue(cursor, out var edge))
        {
            path.Insert(0, edge);
            cursor = edge.SourceEntityId;
        }

        return path;
    }

    private static ReportingField ResolveField(
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity,
        int entityId,
        int fieldId,
        int relationshipId,
        string side)
    {
        if (!fieldsByEntity.TryGetValue(entityId, out var fields))
            throw new InvalidOperationException($"No fields found for entity id={entityId}.");

        return fields.FirstOrDefault(f => f.FieldId == fieldId)
               ?? throw new InvalidOperationException(
                   $"Cannot resolve {side} field id={fieldId} for relationship id={relationshipId}, entity id={entityId}.");
    }

    private static (string sql, List<SqlParameter> sqlParams) BuildSql(
        JoinPlan plan,
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity,
        IDictionary<string, object?> parameters)
    {
        var sqlParams = new List<SqlParameter>();
        var sb = new StringBuilder();
        var paramIndex = 0;

        var aliases = plan.OrderedEntities
            .DistinctBy(e => e.EntityId)
            .Select((entity, idx) => new { entity.EntityId, Alias = $"e{idx}" })
            .ToDictionary(x => x.EntityId, x => x.Alias);

        sb.AppendLine("SELECT");
        var selectedColumns = new List<string>();

        foreach (var entity in plan.OrderedEntities.DistinctBy(e => e.EntityId))
        {
            if (!fieldsByEntity.TryGetValue(entity.EntityId, out var fields))
                continue;

            var alias = aliases[entity.EntityId];
            selectedColumns.AddRange(fields
                .Where(f => f.IsActive)
                .Select(f => $"      {alias}.[{f.FieldKey}] AS [{entity.EntityKey}_{f.FieldKey}]"));
        }

        if (selectedColumns.Count == 0)
            selectedColumns.Add("      1 AS [_empty]");

        sb.AppendLine(string.Join(",\n", selectedColumns));
        sb.AppendLine($"FROM   {plan.Root.ViewName} AS {aliases[plan.Root.EntityId]}");

        foreach (var step in plan.Steps)
        {
            var sourceAlias = aliases[step.Source.EntityId];
            var targetAlias = aliases[step.Target.EntityId];
            var joinKeyword = string.Equals(step.JoinType, "LEFT", StringComparison.OrdinalIgnoreCase)
                ? "LEFT JOIN"
                : "INNER JOIN";

            sb.AppendLine($"{joinKeyword} {step.Target.ViewName} AS {targetAlias}");
            sb.AppendLine($"       ON {sourceAlias}.[{step.SourceField.FieldKey}] = {targetAlias}.[{step.TargetField.FieldKey}]");
        }

        var whereClause = BuildWhereClause(plan.OrderedEntities, fieldsByEntity, aliases, parameters, sqlParams, ref paramIndex);
        if (!string.IsNullOrWhiteSpace(whereClause))
        {
            sb.AppendLine("WHERE");
            sb.AppendLine(whereClause);
        }

        return (sb.ToString(), sqlParams);
    }

    private static string BuildWhereClause(
        IReadOnlyList<ReportingEntity> entities,
        IReadOnlyDictionary<int, IReadOnlyList<ReportingField>> fieldsByEntity,
        IReadOnlyDictionary<int, string> aliases,
        IDictionary<string, object?> parameters,
        List<SqlParameter> sqlParams,
        ref int paramIndex)
    {
        if (parameters.Count == 0)
            return string.Empty;

        var conditions = new List<string>();

        foreach (var (paramKey, paramValue) in parameters)
        {
            if (paramValue == null)
                continue;

            ReportingField? matchedField = null;
            ReportingEntity? matchedEntity = null;

            foreach (var entity in entities.DistinctBy(e => e.EntityId))
            {
                if (!fieldsByEntity.TryGetValue(entity.EntityId, out var fields))
                    continue;

                var field = fields.FirstOrDefault(f =>
                    f.IsFilterable &&
                    (string.Equals(f.FieldKey, paramKey, StringComparison.OrdinalIgnoreCase) ||
                     string.Equals(f.DisplayName, paramKey, StringComparison.OrdinalIgnoreCase)));

                if (field == null)
                    continue;

                matchedField = field;
                matchedEntity = entity;
                break;
            }

            if (matchedField == null || matchedEntity == null)
                continue;

            var pName = $"@p{paramIndex++}";
            sqlParams.Add(new SqlParameter(pName, paramValue));
            conditions.Add($"      {aliases[matchedEntity.EntityId]}.[{matchedField.FieldKey}] = {pName}");
        }

        return string.Join("\n  AND\n", conditions);
    }

    private async Task<DataTable> ExecuteQueryAsync(string sql, List<SqlParameter> sqlParams)
    {
        await using var conn = new SqlConnection(_connectionString);
        await using var command = new SqlCommand(sql, conn);
        command.CommandTimeout = 120;

        foreach (var p in sqlParams)
            command.Parameters.Add(p);

        await conn.OpenAsync();

        var dt = new DataTable("FlatDataset");
        var adapter = new SqlDataAdapter(command);
        adapter.Fill(dt);

        _logger.LogInformation("[FlatDatasetBuilder] Query returned {Rows} rows, {Cols} columns.", dt.Rows.Count, dt.Columns.Count);
        return dt;
    }
}
