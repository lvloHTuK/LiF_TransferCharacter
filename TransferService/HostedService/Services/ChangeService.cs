using BackgroundService.Data;
using CommonNamespace;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using MySqlConnector;
using SqlKata;

namespace BackgroundService.Services
{
    internal class ChangeService : IChangeService
    {
        private readonly MariaDbContext _dbContext;

        public ChangeService(MariaDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task<int> ChangeAccountAsync(MessageDto context)
        {
            if (CheckSteamIDAsync(context))
            {
                var parametrs = new[] { new MySqlConnector.MySqlParameter("@id", context.SteamID), new MySqlConnector.MySqlParameter("@CharacterID", context.PlayerID) };
                string sql = $"UPDATE `{context.ServerName}`.`character` SET `AccountID`=@id WHERE  `ID`=@CharacterID;";
                int rowsAffected = await _dbContext.Database.ExecuteSqlRawAsync(sql, parametrs);

                return rowsAffected;
            }

            return 0;
        }

        public bool CheckSteamIDAsync(MessageDto context)
        {
            string sql = $"SELECT ID FROM `{context.ServerName}`.`account` WHERE ID = @param";
            var steamID = _dbContext.Database.SqlQueryRaw<int>(sql, new MySqlConnector.MySqlParameter("@param", context.SteamID)).ToList();

            if(steamID.Count() == 1)
            {
                return true;
            }

            return false;
        }
    }
}
