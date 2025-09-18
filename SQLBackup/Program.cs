using System;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SQLBackup.synchronous;

namespace SQLBackup
{
    public class Program
    {
        private static void Main(string[] args)
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            // Set up DI
            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(configuration);
            services.AddTransient<IDbConnection>(sp =>
                new SqlConnection(configuration.GetConnectionString("DefaultConnection")));
            services.AddTransient<Db_Table_Details>();

            var serviceProvider = services.BuildServiceProvider();

            // Resolve and use your service
            var dbTableDetails = serviceProvider.GetRequiredService<Db_Table_Details>();
            // Example usage:
            // var tables = dbTableDetails.GetTableNames();
            // foreach (var table in tables) Console.WriteLine(table);

            Console.WriteLine("Hello, World!");
        }
    }
}
