using System.Diagnostics;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = 429; // Too Many Requests

    // POLICY 1: Fixed Window — max 3 requests per 10 seconds
    options.AddFixedWindowLimiter("fixed", o =>
    {
        o.PermitLimit = 3;
        o.Window = TimeSpan.FromSeconds(10);
        o.QueueLimit = 0; // reject immediately, no queue
    });

    // POLICY 2: Sliding Window — max 3 requests per 10 seconds, smoother
    options.AddSlidingWindowLimiter("sliding", o =>
    {
        o.PermitLimit = 3;
        o.Window = TimeSpan.FromSeconds(10);
        o.SegmentsPerWindow = 5; // splits window into 5 segments for smoothness
        o.QueueLimit = 0;
    });

    // POLICY 3: Token Bucket — bucket of 5, refills 1 token/second
    options.AddTokenBucketLimiter("token-bucket", o =>
    {
        o.TokenLimit = 5;           // burst up to 5
        o.ReplenishmentPeriod = TimeSpan.FromSeconds(1);
        o.TokensPerPeriod = 1;      // refill 1 per second
        o.QueueLimit = 0;
    });

    // POLICY 4: Concurrency — max 2 running at the same time
    options.AddConcurrencyLimiter("concurrency", o =>
    {
        o.PermitLimit = 2;   // only 2 concurrent requests allowed
        o.QueueLimit = 0;    // 3rd request gets 429 immediately
    });
});

var app = builder.Build();
app.UseRateLimiter();

app.MapGet("/", () => "Hi this is Ping Ping");

// ---------------------------------------------------------------
// Simulates 3 I/O operations (e.g. DB calls, HTTP calls)
// each taking 300ms
// ---------------------------------------------------------------
static async Task<string> FetchUserAsync()
{
    await Task.Delay(300);
    return $"User [thread:{Thread.CurrentThread.ManagedThreadId}]";
}

static async Task<string> FetchOrdersAsync()
{
    await Task.Delay(300);
    return $"Orders [thread:{Thread.CurrentThread.ManagedThreadId}]";
}

static async Task<string> FetchRecommendationsAsync()
{
    await Task.Delay(300);
    return $"Recommendations [thread:{Thread.CurrentThread.ManagedThreadId}]";
}

// ---------------------------------------------------------------
// DEMO 1: Sequential — total time ≈ 300 + 300 + 300 = 900ms
// Each await waits for the previous to finish before starting
// ---------------------------------------------------------------
app.MapGet("/async/sequential", async () =>
{
    var sw = Stopwatch.StartNew();
    var threadStart = Thread.CurrentThread.ManagedThreadId;

    var user = await FetchUserAsync();
    var orders = await FetchOrdersAsync();
    var recs = await FetchRecommendationsAsync();

    sw.Stop();

    return new
    {
        pattern = "sequential",
        elapsed_ms = sw.ElapsedMilliseconds,
        thread_at_start = threadStart,
        thread_at_end = Thread.CurrentThread.ManagedThreadId,
        note = "total ≈ 900ms because each call waits for the previous",
        results = new[] { user, orders, recs }
    };
});

// ---------------------------------------------------------------
// DEMO 2: Parallel — total time ≈ 300ms
// All 3 tasks are started before any is awaited
// ---------------------------------------------------------------
app.MapGet("/async/parallel", async () =>
{
    var sw = Stopwatch.StartNew();
    var threadStart = Thread.CurrentThread.ManagedThreadId;

    // Start all 3 — none are awaited yet, they run concurrently
    var userTask = FetchUserAsync();
    var ordersTask = FetchOrdersAsync();
    var recsTask = FetchRecommendationsAsync();

    await Task.WhenAll(userTask, ordersTask, recsTask);

    sw.Stop();

    return new
    {
        pattern = "parallel",
        elapsed_ms = sw.ElapsedMilliseconds,
        thread_at_start = threadStart,
        thread_at_end = Thread.CurrentThread.ManagedThreadId,
        note = "total ≈ 300ms because all 3 run at the same time",
        results = new[] { userTask.Result, ordersTask.Result, recsTask.Result }
    };
});

// ---------------------------------------------------------------
// DEMO 3: Thread IDs — proves different threads resume your code
// Thread ID may change across awaits — the state machine handles this
// ---------------------------------------------------------------
app.MapGet("/async/threads", async () =>
{
    var log = new List<string>();

    log.Add($"[start]   thread: {Thread.CurrentThread.ManagedThreadId}");

    await Task.Delay(50);
    log.Add($"[after 1] thread: {Thread.CurrentThread.ManagedThreadId}");

    await Task.Delay(50);
    log.Add($"[after 2] thread: {Thread.CurrentThread.ManagedThreadId}");

    await Task.Delay(50);
    log.Add($"[after 3] thread: {Thread.CurrentThread.ManagedThreadId}");

    log.Add("notice: thread IDs may differ — proves the state machine resumes on any free thread");

    return log;
});

app.MapGet("/threadpool", () =>
 {
     ThreadPool.GetMinThreads(out int minWorker, out int minIO);
     ThreadPool.GetMaxThreads(out int maxWorker, out int maxIO);
     ThreadPool.GetAvailableThreads(out int availWorker, out int availIO);
 
     return new
     {
         min_threads = minWorker,
         max_threads = maxWorker,
         available_threads = availWorker,
         in_use = maxWorker - availWorker
     };
 });

// ---------------------------------------------------------------
// DEMO 4a: ThreadPool STARVATION — blocks 50 real threads
// Hit /threadpool while this is running — watch in_use spike
// ---------------------------------------------------------------
app.MapGet("/async/starve", async () =>
{
    var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(() =>
    {
        Thread.Sleep(5000); // blocks a real thread — the sin
    }));

    await Task.WhenAll(tasks);

    return new { result = "done", note = "50 threads were blocked for 5 seconds each" };
});

// ---------------------------------------------------------------
// DEMO 4b: ThreadPool HEALTHY — 50 concurrent ops, no blocked threads
// Hit /threadpool while this is running — in_use stays low
// ---------------------------------------------------------------
app.MapGet("/async/healthy", async () =>
{
    var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(async () =>
    {
        await Task.Delay(5000); // releases thread while waiting — the right way
    }));

    await Task.WhenAll(tasks);

    return new { result = "done", note = "50 operations ran, threads were free the whole time" };
});

// ---------------------------------------------------------------
// DEMO 5a: NO cancellation — ignores client disconnect
// Hit with curl, press Ctrl+C — server keeps logging for 10 seconds
// ---------------------------------------------------------------
app.MapGet("/async/no-cancel", async () =>
{
    Console.WriteLine("[no-cancel] started");

    for (int i = 1; i <= 10; i++)
    {
        await Task.Delay(1000); // simulates DB/HTTP work each second
        Console.WriteLine($"[no-cancel] step {i}/10 — still running...");
    }

    Console.WriteLine("[no-cancel] finished");
    return new { result = "done", note = "ran all 10 steps even if client disconnected" };
});

// ---------------------------------------------------------------
// DEMO 5b: WITH cancellation — stops when client disconnects
// Hit with curl, press Ctrl+C — server stops immediately
// ---------------------------------------------------------------
app.MapGet("/async/cancel", async (CancellationToken ct) =>
{
    Console.WriteLine("[cancel] started");

    try
    {
        for (int i = 1; i <= 10; i++)
        {
            await Task.Delay(1000, ct); // cancellation token passed — stops if client disconnects
            Console.WriteLine($"[cancel] step {i}/10");
        }

        Console.WriteLine("[cancel] finished");
        return new { result = "done", note = "completed all 10 steps" };
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("[cancel] client disconnected — stopped immediately, resources freed");
        return new { result = "cancelled", note = "stopped as soon as client disconnected" };
    }
});

// ---------------------------------------------------------------
// RATE LIMITER DEMOS
// ---------------------------------------------------------------

// Fixed window: max 3 requests per 10 seconds
app.MapGet("/rate/fixed", () =>
{
    return new { policy = "fixed-window", message = "request allowed", time = DateTime.UtcNow };
}).RequireRateLimiting("fixed");

// Sliding window: max 3 requests per 10 seconds, no burst at boundary
app.MapGet("/rate/sliding", () =>
{
    return new { policy = "sliding-window", message = "request allowed", time = DateTime.UtcNow };
}).RequireRateLimiting("sliding");

// Token bucket: burst 5, then 1 per second
app.MapGet("/rate/token-bucket", () =>
{
    return new { policy = "token-bucket", message = "request allowed", time = DateTime.UtcNow };
}).RequireRateLimiting("token-bucket");

// Concurrency: max 2 running simultaneously — simulates heavy DB query
app.MapGet("/rate/concurrency", async (CancellationToken ct) =>
{
    Console.WriteLine($"[concurrency] request started — {DateTime.UtcNow:HH:mm:ss}");
    await Task.Delay(3000, ct); // simulate 3 second heavy query
    Console.WriteLine($"[concurrency] request finished — {DateTime.UtcNow:HH:mm:ss}");
    return new { policy = "concurrency", message = "heavy query done", time = DateTime.UtcNow };
}).RequireRateLimiting("concurrency");

app.Run();

