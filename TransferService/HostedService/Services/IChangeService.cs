using CommonNamespace;
using MassTransit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BackgroundService.Services
{
    internal interface IChangeService
    {
        Task<int> ChangeAccountAsync(MessageDto context);
        bool CheckSteamIDAsync(MessageDto context);
    }
}
