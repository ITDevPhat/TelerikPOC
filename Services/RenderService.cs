using Microsoft.Extensions.Logging;
using Telerik.Reporting.Services;

namespace TelerikPOC.Services;

public sealed class RenderService
{
    private readonly IReportSourceResolver _resolver;
    private readonly ISnapshotService _snapshotService;
    private readonly ILogger<RenderService> _logger;

    public RenderService(
        IReportSourceResolver resolver,
        ISnapshotService snapshotService,
        ILogger<RenderService> logger)
    {
        _resolver = resolver;
        _snapshotService = snapshotService;
        _logger = logger;
    }
}
