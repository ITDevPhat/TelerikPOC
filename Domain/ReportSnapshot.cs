namespace TelerikPOC.Domain;

public sealed class ReportSnapshot
{
    public Guid Id { get; set; }
    public string ReportName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public string ParametersHash { get; set; } = string.Empty;
    public string Format { get; set; } = string.Empty;
    public string S3Key { get; set; } = string.Empty;
    public long SizeBytes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime ExpiresAt { get; set; }
}
