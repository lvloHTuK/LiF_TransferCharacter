using BackgroundService.Dto;
using BackgroundService.Services;
using CommonNamespace;
using MassTransit;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Renci.SshNet.Messages.Authentication;

namespace BackgroundService.Consumers
{
    class EventConsumer : IConsumer<MessageDto>
    {

        private readonly IChangeService _changeService;
        private readonly IPublishEndpoint _publishEndpoint;

        public EventConsumer(IChangeService changeService, IPublishEndpoint publishEndpoint)
        {
            _changeService = changeService;
            _publishEndpoint = publishEndpoint;
        }

        public async Task Consume(ConsumeContext<MessageDto> context)
        {
            Console.WriteLine("Value: {0}", context.Message.ServerName);
            Console.WriteLine("Value: {0}", context.Message.PlayerID);
            Console.WriteLine("Value: {0}", context.Message.SteamID);
            Console.WriteLine("Value: {0}", context.Message.TgName);
            int rowsAffected = await _changeService.ChangeAccountAsync(context.Message);
            if(rowsAffected > 0)
            {
                await _publishEndpoint.Publish<MessageTransfer>(new
                {
                    context.Message.TgName,
                    Message = "Success"
                });
                Console.WriteLine("Success");
            }
        }
    }
}