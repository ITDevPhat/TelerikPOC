namespace TelerikPOC.Domain;

public sealed class ReportingRelationship
{
    public int RelationshipId { get; set; }
    public int ParentEntityId { get; set; }
    public int ParentFieldId { get; set; }
    public int ChildEntityId { get; set; }
    public int ChildFieldId { get; set; }
    public string JoinType { get; set; } = "INNER";
    public string Cardinality { get; set; } = string.Empty;
    public string Direction { get; set; } = "FORWARD";
    public int HopWeight { get; set; }
    public bool FilterPropagation { get; set; }
    public int MaxJoinDepth { get; set; }
    public bool IsRequired { get; set; }
    public bool IsActive { get; set; }
}
