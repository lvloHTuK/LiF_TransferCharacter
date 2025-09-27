using BackgroundService.Consumers;
using BackgroundService.Data;
using BackgroundService.Settings;
using MassTransit;
using MassTransit.Configuration;
using MassTransit.Serialization;
using Microsoft.EntityFrameworkCore;
using MySql.Data.EntityFrameworkCore;
using Pomelo.EntityFrameworkCore.MySql;
using Microsoft.Extensions.Options;
using BackgroundService.Services;

namespace BackgroundService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var queueNames = new List<string?>();

            for (int i = 0; i < 10; i++)
            {
                queueNames.Add(configuration["MassTransitSettings:QueueNames:Queue_" + i]);
            }

            return Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    var connectionString = configuration.GetConnectionString("CustomerDatabase");
                    services.AddDbContext<MariaDbContext>(options => options.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)));
                    services.AddScoped<IChangeService, ChangeService>();
                    services.AddMassTransit(x =>
                    {
                        x.AddConsumer<EventConsumer>();
                        x.UsingRabbitMq((context, cfg) =>
                        {
                            ConfigureRmq(cfg, configuration);
                            RegisterEndPoints(cfg, queueNames, context);
                        });
                    });
                    services.AddHostedService<MasstransitService>();
                });
        }

        /// <summary>
        /// Конфигурирование RMQ.
        /// </summary>
        /// <param name="configurator"> Конфигуратор RMQ. </param>
        /// <param name="configuration"> Конфигурация приложения. </param>
        private static void ConfigureRmq(IRabbitMqBusFactoryConfigurator configurator, IConfiguration configuration)
        {
            var rmqSettings = configuration.Get<ApplicationSettings>().RmqSettings;
            configurator.Host(rmqSettings.Host,
                rmqSettings.VHost,
                h =>
                {
                    h.Username(rmqSettings.Login);
                    h.Password(rmqSettings.Password);
                });
        }
        
        /// <summary>
        /// регистрация эндпоинтов
        /// </summary>
        /// <param name="configurator"></param>
        private static void RegisterEndPoints(IRabbitMqBusFactoryConfigurator configurator, List<string?> queueNames, IBusRegistrationContext context)
        {
            foreach (var queueName in queueNames)
            {
                if (!string.IsNullOrWhiteSpace(queueName))
                {
                    configurator.UseRawJsonDeserializer(RawSerializerOptions.All, isDefault: true);
                    configurator.ReceiveEndpoint(queueName, e =>
                    {
                        e.ConfigureConsumer<EventConsumer>(context);
                        e.UseMessageRetry(r =>
                        {
                            r.Incremental(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
                        });
                    });
                }
            }
        }
    }
}