namespace TelerikPOC.Domain;

public sealed class ReportingField
{
    public int FieldId { get; set; }
    public int EntityId { get; set; }
    public string FieldKey { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public string FieldRole { get; set; } = string.Empty;
    public bool IsFilterable { get; set; }
    public bool IsGroupable { get; set; }
    public bool IsSensitive { get; set; }
    public bool IsActive { get; set; }
    public bool IsDefault { get; set; }
}
