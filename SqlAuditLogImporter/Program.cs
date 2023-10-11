using Microsoft.EntityFrameworkCore;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Microsoft.SqlServer.XEvent.Linq;

using Serilog;

using System.Collections.Concurrent;

using SqlAuditLogImporter;
using Microsoft.Extensions.Configuration;


var builder = Host.CreateDefaultBuilder(args).ConfigureLogging((context, logging) => {
    logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Warning);
});

var config = new ConfigurationBuilder()
   .SetBasePath(Directory.GetCurrentDirectory())
   .AddJsonFile("appsettings.json", optional: false)
   .Build();

string backupPath = config.GetValue<string>(nameof(backupPath));
string connectionString = config.GetValue<string>(nameof(connectionString));

builder.ConfigureServices((ctx, services) => {
    services.AddDbContext<AppDbContext>(options => options.UseSqlServer(connectionString, sqlOptions => sqlOptions.EnableRetryOnFailure(5)), ServiceLifetime.Transient);
});

Log.Logger = new LoggerConfiguration().WriteTo
    .Console()
    .WriteTo.File($"log{DateTime.UtcNow:yyyy-MM-ddTHH-mm-ss}.txt")
    .CreateLogger();

var app = builder.Build();
using var scope = app.Services.CreateScope();
var services = scope.ServiceProvider;

try {

    var appContext = services.GetRequiredService<AppDbContext>();
    appContext.Database.EnsureCreated();
    appContext = null;

    ConcurrentQueue<string> _fileQueue = new();

    var dir = new DirectoryInfo(backupPath);

    if (!dir.Exists) {
        Log.Error("Directory doesn't exist");
        Console.ReadLine();
        return;
    }

    async Task GetFilesRecursive(DirectoryInfo dir) {
        var files = dir.GetFiles().Where(x => x.Extension == ".xel").Select(x => x.FullName);
        foreach (var file in files) {
            Log.Information("Found file " + file);
            _fileQueue.Enqueue(file);
        }
        foreach (var subDir in dir.GetDirectories()) {
            await GetFilesRecursive(subDir);
        }
    }

    await GetFilesRecursive(dir);

    var threadCount = Environment.ProcessorCount;

    Action[] actions = new Action[threadCount];

    DateTime start = DateTime.Now;
    int totalRecords = 0;
    for (int i = 0; i < threadCount; i++) {
        actions[i] = async () => {
            var context = services.GetRequiredService<AppDbContext>();
            context.Database.SetCommandTimeout(300);
            while (_fileQueue.TryDequeue(out string? file)) {
                Log.Information($"Starting file {file}");
                var evData = new QueryableXEventData(file);
                var enumerator = evData.GetEnumerator();

                List<LogDTO> logs = new();
                int logsInFile = 0;
                while (enumerator.MoveNext()) {
                    var ev = enumerator.Current;
                    context.Logs.Add(new LogDTO(ev.Fields));
                    logsInFile++;
                }
                await context.SaveChangesAsync();

                evData = null;

                Log.Information($"{logsInFile} events added from file {file}");

                //Add a new suffix onto the file so it will be ignored if we have to run again
                File.Move(file, file+".done");

                totalRecords += logsInFile;
                TimeSpan span = DateTime.Now.Subtract(start);
                Log.Information($"{span.Hours}:{span.Minutes}:{span.Seconds} elapsed - {totalRecords} total events processed");
            }
            Log.Information($"Thread complete");
        };
    }

    Parallel.Invoke(actions);
} catch (Exception e) {
    Log.Error($"An error occurred :: {e}");
}

Console.ReadLine();
