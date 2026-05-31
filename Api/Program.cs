var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();
app.MapGet("/",() => "Hi this is Ping Ping");

app.Run();
