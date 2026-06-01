using System.Diagnostics;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();

var app = builder.Build();

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

app.Run();

