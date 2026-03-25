

using Amazon;
using Amazon.S3;
using Microsoft.AspNetCore.Builder;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using Telerik.Reporting.Cache.File;
using Telerik.Reporting.Cache.Interfaces;
using Telerik.Reporting.Services;
using Telerik.Reporting.Services.AspNetCore;
using TelerikPOC.Infrastructure;
using TelerikPOC.Resolvers;
using TelerikPOC.Services;

var builder = WebApplication.CreateBuilder(args);

// ═════════════════════════════════════════════════════════════════
// INFRASTRUCTURE
// ═════════════════════════════════════════════════════════════════

builder.Services.AddControllers();
builder.Services.AddMemoryCache();              // for IMetadataRepository cache
builder.Services.AddCors(o => o.AddPolicy("AllowAll",
    p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

// ── AWS S3 ────────────────────────────────────────────────────────

builder.Services.AddSingleton<IAmazonS3>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var region = RegionEndpoint.GetBySystemName(cfg["AWS:Region"] ?? "ap-southeast-1");
    var key = cfg["AWS:AccessKey"];
    var secret = cfg["AWS:SecretKey"];

    return string.IsNullOrWhiteSpace(key)
        ? new AmazonS3Client(region)                        // Production: IAM Role
        : new AmazonS3Client(key, secret, region);          // Dev: explicit credentials
});

// ── Database ──────────────────────────────────────────────────────
// Scoped: new connection per request (Dapper pattern)

builder.Services.AddScoped<IDbConnection>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var conn = new SqlConnection(
        cfg.GetConnectionString("ReportingDb")
        ?? throw new InvalidOperationException("ConnectionStrings:ReportingDb required"));
    return conn;
});

// ═════════════════════════════════════════════════════════════════
// REPOSITORIES
// ═════════════════════════════════════════════════════════════════

builder.Services.AddScoped<IReportDefinitionRepository, ReportDefinitionRepository>();
builder.Services.AddScoped<ISnapshotRepository, SnapshotRepository>();

// MetadataRepository is scoped (uses IDbConnection) but its RESULTS are
// singleton-cached via IMemoryCache — best of both worlds.
builder.Services.AddScoped<IMetadataRepository, MetadataRepository>();

// ═════════════════════════════════════════════════════════════════
// SERVICES
// ═════════════════════════════════════════════════════════════════

// FlatDatasetBuilder needs the raw connection string (uses SqlConnection directly
// for SqlDataAdapter which requires an open connection over its lifetime)
builder.Services.AddScoped<FlatDatasetBuilder>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var meta = sp.GetRequiredService<IMetadataRepository>();
    var logger = sp.GetRequiredService<ILogger<FlatDatasetBuilder>>();
    var cs = cfg.GetConnectionString("ReportingDb")
                 ?? throw new InvalidOperationException("ConnectionStrings:ReportingDb required");

    return new FlatDatasetBuilder(meta, cs, logger);
});

builder.Services.AddScoped<IDataProvider, DataProvider>();

// SnapshotService: scoped because it depends on ISnapshotRepository (scoped)
builder.Services.AddScoped<ISnapshotService>(sp =>
{
    var s3 = sp.GetRequiredService<IAmazonS3>();
    var cfg = sp.GetRequiredService<IConfiguration>();
    var repo = sp.GetRequiredService<ISnapshotRepository>();
    var logger = sp.GetRequiredService<ILogger<SnapshotService>>();
    var bucket = cfg["AWS:BucketName"]
                 ?? throw new InvalidOperationException("AWS:BucketName required");

    return new SnapshotService(s3, bucket, repo, logger);
});

// ═════════════════════════════════════════════════════════════════
// RESOLVERS
// ═════════════════════════════════════════════════════════════════

builder.Services.AddScoped<S3ReportSourceResolver>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var bucket = cfg["AWS:BucketName"]
                 ?? throw new InvalidOperationException("AWS:BucketName required");

    return new S3ReportSourceResolver(
        s3: sp.GetRequiredService<IAmazonS3>(),
        bucketName: bucket,
        definitionRepo: sp.GetRequiredService<IReportDefinitionRepository>(),
        dataProvider: sp.GetRequiredService<IDataProvider>(),
        snapshotService: sp.GetRequiredService<ISnapshotService>(),
        logger: sp.GetRequiredService<ILogger<S3ReportSourceResolver>>());
});

// RenderService: scoped (chains scoped dependencies)
builder.Services.AddScoped<RenderService>(sp =>
{
    // Wrap scoped resolver in an adapter usable from the singleton IReportServiceConfiguration
    var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
    var resolver = new ScopedResolverAdapter(scopeFactory);
    return new RenderService(
        resolver: resolver,
        snapshotService: sp.GetRequiredService<ISnapshotService>(),
        logger: sp.GetRequiredService<ILogger<RenderService>>());
});

// ═════════════════════════════════════════════════════════════════
// TELERIK IReportServiceConfiguration
// ═════════════════════════════════════════════════════════════════

builder.Services.TryAddSingleton<IReportServiceConfiguration>(sp =>
{
    var env = sp.GetRequiredService<IHostEnvironment>();
    var cfg = sp.GetRequiredService<IConfiguration>();
    var s3 = sp.GetRequiredService<IAmazonS3>();
    var bucket = cfg["AWS:BucketName"]
                 ?? throw new InvalidOperationException("AWS:BucketName required");

    // Telerik's internal IStorage: short-lived session/chunk cache
    // This is SEPARATE from snapshot cache — do not confuse the two layers.
    IStorage telerikStorage = env.IsDevelopment()
        ? new LoggingStorage(
            basePath: Path.Combine(env.ContentRootPath, "Cache"),
            logFolder: Path.Combine(env.ContentRootPath, "CacheLogs"))
        : new S3Storage(s3, bucket, "telerik/session-cache");

    // Resolver adapter: resolves S3ReportSourceResolver from a fresh DI scope
    // per Telerik call (required because resolver is scoped, config is singleton)
    var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
    var resolver = new ScopedResolverAdapter(scopeFactory);

    return new ReportServiceConfiguration
    {
        HostAppId = "ReportingPOC",
        Storage = telerikStorage,
        ReportSourceResolver = resolver,
        ClientSessionTimeout = 15,
        ReportSharingTimeout = 0,
    };
});

// ═════════════════════════════════════════════════════════════════
// PIPELINE
// ═════════════════════════════════════════════════════════════════

var app = builder.Build();

if (!app.Environment.IsDevelopment())
    app.UseExceptionHandler("/error");

app.UseStaticFiles();
app.UseRouting();
app.UseCors("AllowAll");
app.MapControllers();

app.Run();

// ═════════════════════════════════════════════════════════════════
// ScopedResolverAdapter
// Bridges scoped S3ReportSourceResolver into singleton context.
// ═════════════════════════════════════════════════════════════════

public sealed class ScopedResolverAdapter : IReportSourceResolver
{
    private readonly IServiceScopeFactory _factory;

    public ScopedResolverAdapter(IServiceScopeFactory factory)
        => _factory = factory;

    public Telerik.Reporting.ReportSource Resolve(
        string report,
        OperationOrigin origin,
        System.Collections.Generic.IDictionary<string, object> currentParameterValues)
    {
        using var scope = _factory.CreateScope();
        var inner = scope.ServiceProvider.GetRequiredService<S3ReportSourceResolver>();
        return inner.Resolve(report, origin, currentParameterValues);
    }
}



//using Microsoft.AspNetCore.Builder;
//using Microsoft.AspNetCore.Hosting;
//using Microsoft.Extensions.Configuration;
//using Microsoft.Extensions.DependencyInjection;
//using Microsoft.Extensions.DependencyInjection.Extensions;
//using Microsoft.Extensions.Hosting;
//using System;
//using System.IO;

//using Telerik.Reporting.Cache.File;
//using Telerik.Reporting.Services;
//using Telerik.Reporting.Services.AspNetCore;
//using Telerik.Reporting.Cache.File;


//EnableTracing();

//var builder = WebApplication.CreateBuilder(args);

//// =====================
//// Add services
//// =====================

//// Controllers + Razor (nếu cần UI)
//builder.Services.AddControllers();
//builder.Services.AddRazorPages();

//// CORS (bắt buộc cho Web Report Viewer)
//builder.Services.AddCors(options =>
//{
//    options.AddPolicy("AllowAll",
//        p => p.AllowAnyOrigin()
//              .AllowAnyMethod()
//              .AllowAnyHeader());
//});

//// =====================
//// Telerik Reporting config (CHUẨN)
//// =====================
////builder.Services.TryAddSingleton<IReportServiceConfiguration>(sp =>
////{
////    var env = sp.GetRequiredService<IWebHostEnvironment>();

////    return new ReportServiceConfiguration
////    {
////        ReportingEngineConfiguration = sp.GetRequiredService<IConfiguration>(),

////        HostAppId = "TelerikReportingRestService",

////        Storage = new FileStorage(),

////        ReportSourceResolver = new TypeReportSourceResolver()
////            .AddFallbackResolver(
////                new UriReportSourceResolver(
////                    Path.Combine(env.ContentRootPath, "Reports")
////                )
////            )
////    };
////});


//builder.Services.TryAddSingleton<IReportServiceConfiguration>(sp =>
//{
//    var env = sp.GetRequiredService<IWebHostEnvironment>();
//    var config = sp.GetRequiredService<IConfiguration>();

//    var cachePath = Path.Combine(env.ContentRootPath, "Cache");
//    var logPath = Path.Combine(env.ContentRootPath, "CacheLogs");

//    Directory.CreateDirectory(cachePath);
//    Directory.CreateDirectory(logPath);

//    return new ReportServiceConfiguration
//    {
//        ReportingEngineConfiguration = config,

//        HostAppId = "TelerikReportingRestService",

//        Storage = new LoggingStorage(cachePath, logPath),

//        ReportSourceResolver = new TypeReportSourceResolver()
//            .AddFallbackResolver(
//                new UriReportSourceResolver(
//                    Path.Combine(env.ContentRootPath, "Reports")
//                )
//            ),

//        // ========================
//        // QUAN TRỌNG THEO DOC
//        // ========================
//        ClientSessionTimeout = 15,   // phút
//        ReportSharingTimeout = 0     // bật reuse nếu > 0
//    };
//});

//var app = builder.Build();

//// =====================
//// Middleware pipeline
//// =====================

//if (!app.Environment.IsDevelopment())
//{
//    app.UseExceptionHandler("/Home/Error");
//}

//app.UseStaticFiles(); // phục vụ file tĩnh (viewer html/js)

//app.UseRouting();

//app.UseCors("AllowAll");

//app.MapControllers();

//app.Run();


//// =====================
//// Helpers
//// =====================

//static void EnableTracing()
//{
//    // Bật nếu cần debug Telerik
//    // System.Diagnostics.Trace.Listeners.Add(
//    //     new System.Diagnostics.TextWriterTraceListener(File.CreateText("log.txt"))
//    // );
//    // System.Diagnostics.Trace.AutoFlush = true;
//}

//static IConfiguration ResolveSpecificReportingConfiguration(IWebHostEnvironment environment)
//{
//    var reportingConfigFileName = Path.Combine(environment.ContentRootPath, "reportingAppSettings.json");

//    return new ConfigurationBuilder()
//        .AddJsonFile(reportingConfigFileName, optional: true)
//        .Build();
//}