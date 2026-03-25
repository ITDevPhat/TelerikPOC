namespace TelerikPOC.Domain;

public sealed class ReportDefinition
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public string S3Key { get; set; } = string.Empty;
    public string? TenantId { get; set; }
    public string? DisplayName { get; set; }
    public string? Description { get; set; }
    public string EntityKeysJson { get; set; } = "[]";
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public bool IsActive { get; set; }
}
