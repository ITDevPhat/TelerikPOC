namespace TelerikPOC.Domain;

public sealed class ReportingEntity
{
    public int EntityId { get; set; }
    public string EntityKey { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string ViewName { get; set; } = string.Empty;
    public string EntityType { get; set; } = string.Empty;
    public int MaxSelectableFields { get; set; }
    public int MaxGroupByFields { get; set; }
    public bool IsActive { get; set; }
}
