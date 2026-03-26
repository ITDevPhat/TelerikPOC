using Amazon;
using Amazon.S3;
using Microsoft.AspNetCore.Builder;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.ComponentModel.DataAnnotations;
using System.Data;
using Telerik.Reporting.Cache.Interfaces;
using Telerik.Reporting.Services;
using Telerik.Reporting.Services.AspNetCore;
using TelerikPOC.Infrastructure;
using TelerikPOC.Resolvers;
using TelerikPOC.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddCors(o => o.AddPolicy("AllowAll", p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));
builder.Services.AddMemoryCache();

builder.Services
    .AddOptions<ReportingRuntimeOptions>()
    .Bind(builder.Configuration.GetSection("ReportingRuntime"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

var reportingDb = builder.Configuration.GetConnectionString("ReportingDb")
    ?? throw new InvalidOperationException("ConnectionStrings:ReportingDb is required.");

builder.Services.AddSingleton(_ => reportingDb);

builder.Services.AddSingleton<IAmazonS3>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var options = sp.GetRequiredService<IOptions<ReportingRuntimeOptions>>().Value;
    var region = RegionEndpoint.GetBySystemName(options.AwsRegion);

    var key = cfg["AWS:AccessKey"];
    var secret = cfg["AWS:SecretKey"];

    return string.IsNullOrWhiteSpace(key)
        ? new AmazonS3Client(region)
        : new AmazonS3Client(key, secret, region);
});

builder.Services.AddScoped<IDbConnection>(_ => new SqlConnection(reportingDb));

builder.Services.AddScoped<IReportDefinitionRepository, ReportDefinitionRepository>();
builder.Services.AddScoped<IMetadataRepository, MetadataRepository>();

builder.Services.AddScoped<FlatDatasetBuilder>(sp => new FlatDatasetBuilder(
    meta: sp.GetRequiredService<IMetadataRepository>(),
    connectionString: reportingDb,
    logger: sp.GetRequiredService<ILogger<FlatDatasetBuilder>>()));

builder.Services.AddScoped<IDataProvider, DataProvider>();

builder.Services.AddScoped<S3ReportSourceResolver>(sp =>
{
    var options = sp.GetRequiredService<IOptions<ReportingRuntimeOptions>>().Value;
    return new S3ReportSourceResolver(
        s3: sp.GetRequiredService<IAmazonS3>(),
        bucketName: options.BucketName,
        definitionRepo: sp.GetRequiredService<IReportDefinitionRepository>(),
        dataProvider: sp.GetRequiredService<IDataProvider>(),
        logger: sp.GetRequiredService<ILogger<S3ReportSourceResolver>>());
});

builder.Services.TryAddSingleton<IReportServiceConfiguration>(sp =>
{
    var env = sp.GetRequiredService<IHostEnvironment>();
    var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
    var options = sp.GetRequiredService<IOptions<ReportingRuntimeOptions>>().Value;

    IStorage storage = env.IsDevelopment() && options.AllowLocalStorageInDevelopment
        ? new LoggingStorage(
            basePath: Path.Combine(env.ContentRootPath, "Cache"),
            logFolder: Path.Combine(env.ContentRootPath, "CacheLogs"))
        : new S3Storage(
            s3: sp.GetRequiredService<IAmazonS3>(),
            bucketName: options.BucketName,
            prefix: options.TelerikStoragePrefix);

    return new ReportServiceConfiguration
    {
        HostAppId = options.HostAppId,
        Storage = storage,
        ReportSourceResolver = new ScopedResolverAdapter(scopeFactory),
        ClientSessionTimeout = options.ClientSessionTimeout,
        ReportSharingTimeout = options.ReportSharingTimeout,
    };
});

var app = builder.Build();

if (!app.Environment.IsDevelopment())
    app.UseExceptionHandler("/error");

app.UseStaticFiles();
app.UseRouting();
app.UseCors("AllowAll");
app.MapControllers();
app.Run();

public sealed class ScopedResolverAdapter : IReportSourceResolver
{
    private readonly IServiceScopeFactory _scopeFactory;

    public ScopedResolverAdapter(IServiceScopeFactory scopeFactory)
        => _scopeFactory = scopeFactory;

    public Telerik.Reporting.ReportSource Resolve(
        string report,
        OperationOrigin origin,
        IDictionary<string, object> currentParameterValues)
    {
        using var scope = _scopeFactory.CreateScope();
        var resolver = scope.ServiceProvider.GetRequiredService<S3ReportSourceResolver>();
        return resolver.Resolve(report, origin, currentParameterValues);
    }
}

public sealed class ReportingRuntimeOptions
{
    [Required]
    public string BucketName { get; set; } = string.Empty;

    [Required]
    public string AwsRegion { get; set; } = string.Empty;

    [Required]
    public string HostAppId { get; set; } = "ReportingPOC";

    [Required]
    public string TelerikStoragePrefix { get; set; } = "telerik/session-cache";

    [Range(1, 1440)]
    public int ClientSessionTimeout { get; set; } = 15;

    [Range(0, 1440)]
    public int ReportSharingTimeout { get; set; } = 10;

    public bool AllowLocalStorageInDevelopment { get; set; }
}
