using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.RateLimiting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();

// RabbitMQ connection (singleton — one connection shared across the app)
builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory { HostName = "localhost", Port = 5672 };
    return factory.CreateConnectionAsync().GetAwaiter().GetResult();
});

// Background service that consumes from payment.queue
builder.Services.AddHostedService<PaymentConsumer>();

// Kafka — two independent consumer groups reading the same topic
// Demonstrates: same events, different groups, independent offsets
builder.Services.AddHostedService<KafkaNotificationConsumer>();
builder.Services.AddHostedService<KafkaAuditConsumer>();

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

// ---------------------------------------------------------------
// RABBITMQ: Publish a payment event
// POST /rabbitmq/publish
// ---------------------------------------------------------------
app.MapPost("/rabbitmq/publish", async (IConnection connection) =>
{
    await using var channel = await connection.CreateChannelAsync();

    var payment = new
    {
        PaymentId = Guid.NewGuid(),
        Amount = Random.Shared.Next(100, 10000),
        Currency = "USD",
        PublishedAt = DateTime.UtcNow
    };

    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payment));

    await channel.BasicPublishAsync(
        exchange: "events",
        routingKey: "payment.processed",
        body: body);

    Console.WriteLine($"[publisher] Published PaymentId={payment.PaymentId}, Amount={payment.Amount}");

    return new { published = true, payment };
});

// ---------------------------------------------------------------
// KAFKA: Publish a payment event to Kafka topic
// POST /kafka/publish?clientId=ABC
// clientId is the partition key — same clientId → same partition → ordered
// ---------------------------------------------------------------
app.MapPost("/kafka/publish", async (string? clientId) =>
{
    var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
    using var producer = new ProducerBuilder<string, string>(config).Build();

    var key = clientId ?? Guid.NewGuid().ToString();
    var payload = JsonSerializer.Serialize(new
    {
        PaymentId = Guid.NewGuid(),
        ClientId = key,
        Amount = Random.Shared.Next(100, 10000),
        PublishedAt = DateTime.UtcNow
    });

    var result = await producer.ProduceAsync("payment-events", new Message<string, string>
    {
        Key = key,      // same key → same partition → ordered
        Value = payload
    });

    Console.WriteLine($"[kafka-producer] ClientId={key} → partition {result.Partition.Value}, offset {result.Offset.Value}");

    return new
    {
        published = true,
        clientId = key,
        partition = result.Partition.Value,
        offset = result.Offset.Value
    };
});

// Create Kafka topic before consumers start — avoids "Unknown topic" error on startup
app.Lifetime.ApplicationStarted.Register(() =>
{
    try
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = "payment-events",
            NumPartitions = 3,
            ReplicationFactor = 1
        }]).GetAwaiter().GetResult();
        Console.WriteLine("[kafka] Topic 'payment-events' created (3 partitions)");
    }
    catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        Console.WriteLine("[kafka] Topic 'payment-events' already exists");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[kafka] Topic creation failed: {ex.Message}");
    }
});

app.Run();

// ---------------------------------------------------------------
// RABBITMQ CONSUMER — runs as background service
// Sets up exchanges, queues, bindings and consumes continuously
// Randomly fails to demonstrate retry + DLQ
// ---------------------------------------------------------------
public class PaymentConsumer : BackgroundService
{
    private readonly IConnection _connection;
    private const int MaxRetries = 3;
    private readonly Random _random = new();

    public PaymentConsumer(IConnection connection) => _connection = connection;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await using var channel = await _connection.CreateChannelAsync(cancellationToken: ct);

        // Exchanges
        await channel.ExchangeDeclareAsync("events",       ExchangeType.Topic,  durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync("events.retry", ExchangeType.Direct, durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync("events.dlx",   ExchangeType.Direct, durable: true, cancellationToken: ct);

        // Main queue — failed messages → retry exchange
        await channel.QueueDeclareAsync("payment.queue", durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange",    "events.retry" },
                { "x-dead-letter-routing-key", "payment.retry" }
            }, cancellationToken: ct);

        // Retry queue — waits 10s then sends back to main exchange
        await channel.QueueDeclareAsync("payment.retry.queue", durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                { "x-message-ttl",             10000 },
                { "x-dead-letter-exchange",    "events" },
                { "x-dead-letter-routing-key", "payment.processed" }
            }, cancellationToken: ct);

        // Dead letter queue — poison messages end up here
        await channel.QueueDeclareAsync("payment.dlq", durable: true, exclusive: false, autoDelete: false,
            cancellationToken: ct);

        // Bindings
        await channel.QueueBindAsync("payment.queue",       "events",       "payment.processed", cancellationToken: ct);
        await channel.QueueBindAsync("payment.retry.queue", "events.retry", "payment.retry",     cancellationToken: ct);
        await channel.QueueBindAsync("payment.dlq",         "events.dlx",   "payment.dlq",       cancellationToken: ct);

        await channel.BasicQosAsync(0, 1, false, ct);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var body    = Encoding.UTF8.GetString(ea.Body.ToArray());
            var retries = GetRetryCount(ea.BasicProperties);

            Console.WriteLine($"\n[consumer] attempt {retries + 1}/{MaxRetries + 1} — {body}");

            bool fail = _random.Next(100) < 60; // 60% failure rate

            if (fail && retries < MaxRetries)
            {
                Console.WriteLine($"[consumer] ❌ Failed. Retry in 10s... ({retries + 1}/{MaxRetries})");
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
            }
            else if (fail && retries >= MaxRetries)
            {
                Console.WriteLine($"[consumer] 💀 Max retries hit. Sending to DLQ.");
                await channel.BasicPublishAsync("events.dlx", "payment.dlq", body: ea.Body);
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            }
            else
            {
                Console.WriteLine($"[consumer] ✅ Processed successfully.");
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            }
        };

        await channel.BasicConsumeAsync("payment.queue", autoAck: false, consumer: consumer, cancellationToken: ct);

        Console.WriteLine("[consumer] Listening on payment.queue...");
        await Task.Delay(Timeout.Infinite, ct);
    }

    private static int GetRetryCount(IReadOnlyBasicProperties props)
    {
        if (props.Headers != null && props.Headers.TryGetValue("x-death", out var xDeath))
            if (xDeath is List<object> deaths && deaths.Count > 0)
                if (deaths[0] is IDictionary<string, object> d && d.TryGetValue("count", out var c))
                    return Convert.ToInt32(c);
        return 0;
    }
}

// ---------------------------------------------------------------
// KAFKA CONSUMER: notification-service group
// Demonstrates MANUAL COMMIT + retry + DLQ pattern
//
// AUTO COMMIT danger:
//   receive message → Kafka marks done → handler crashes → message LOST
//
// MANUAL COMMIT:
//   receive → process → SUCCESS → commit (tell Kafka "I'm done")
//                     → FAIL    → retry up to 3x → DLQ topic
//   On restart: resume from last committed offset — no data loss
// ---------------------------------------------------------------
public class KafkaNotificationConsumer : BackgroundService
{
    private const int MaxRetries = 3;
    private const string DlqTopic = "payment-events.dlq";

    protected override Task ExecuteAsync(CancellationToken ct)
    {
        return Task.Run(() =>
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "notification-service",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false   // ← MANUAL COMMIT: we decide when offset is saved
            };

            // Producer needed to publish failed messages to DLQ topic
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

            consumer.Subscribe("payment-events");
            Console.WriteLine("[kafka-notification] Subscribed (manual commit mode)");

            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? msg = null;
                try
                {
                    msg = consumer.Consume(ct);
                    Console.WriteLine($"[kafka-notification] partition={msg.Partition.Value} offset={msg.Offset.Value} clientId={msg.Message.Key}");

                    // Simulate random failure — 30% chance to fail (for demo)
                    ProcessWithRetry(msg.Message.Value, msg.Message.Key);

                    // ✅ SUCCESS: commit offset — Kafka now knows this message is done
                    // On restart, consumer will read from offset+1 (next message)
                    consumer.Commit(msg);
                    Console.WriteLine($"  ✅ committed offset {msg.Offset.Value}");
                }
                catch (OperationCanceledException) { break; }
                catch (RetryExhaustedException)
                {
                    // ❌ All retries failed — send to DLQ topic (another Kafka topic)
                    // Still commit the offset — otherwise we loop this poison message forever
                    Console.WriteLine($"  ☠ sending to DLQ: {msg!.Message.Key}");
                    dlqProducer.Produce(DlqTopic, new Message<string, string>
                    {
                        Key = msg.Message.Key,
                        Value = msg.Message.Value
                    });
                    consumer.Commit(msg);  // ← commit even on DLQ — move past poison message
                    Console.WriteLine($"  ✅ committed offset {msg.Offset.Value} (after DLQ)");
                }
                catch (Exception ex)
                {
                    // Unexpected error — do NOT commit, message will be redelivered on restart
                    Console.WriteLine($"  ⚠ unexpected error, offset NOT committed: {ex.Message}");
                }
            }

            consumer.Close();
        }, ct);
    }

    private static readonly Random _rng = new();

    private static void ProcessWithRetry(string payload, string key)
    {
        for (int attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                // Simulate 40% failure rate to show retry in action
                if (_rng.NextDouble() < 0.4)
                    throw new Exception("transient failure (simulated)");

                Console.WriteLine($"  → notification sent for {key} (attempt {attempt})");
                return; // success
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ↩ attempt {attempt}/{MaxRetries} failed: {ex.Message}");
                if (attempt == MaxRetries)
                    throw new RetryExhaustedException();

                Thread.Sleep(200 * attempt); // simple backoff: 200ms, 400ms
            }
        }
    }
}

public class RetryExhaustedException : Exception { }

// ---------------------------------------------------------------
// KAFKA CONSUMER: audit-service group
// Reads SAME payment-events topic — completely independent offset
// Proves: two groups, same topic, no interference
// ---------------------------------------------------------------
public class KafkaAuditConsumer : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken ct)
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "audit-service",          // different group — own offset
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe("payment-events");

            Console.WriteLine("[kafka-audit] Subscribed to payment-events (group: audit-service)");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var msg = consumer.Consume(ct);
                    Console.WriteLine($"[kafka-audit] partition={msg.Partition.Value} offset={msg.Offset.Value} clientId={msg.Message.Key}");
                    Console.WriteLine($"  → writing audit log for: {msg.Message.Value}");
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.WriteLine($"[kafka-audit] error: {ex.Message}"); }
            }

            consumer.Close();
        }, ct);
    }
}

