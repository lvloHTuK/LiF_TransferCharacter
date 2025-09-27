using CommonNamespace;
using Microsoft.EntityFrameworkCore;


namespace BackgroundService.Data
{
    internal class MariaDbContext : DbContext
    {
        public MariaDbContext(DbContextOptions<MariaDbContext> options) : base(options) { }

        public virtual DbSet<Account>? account { get; set; }

        /*protected override void OnModelCreating(ModelBuilder builder)
        {
            builder.Entity<MessageDto>(b =>
            {
                
            });
        }*/
    }
}
