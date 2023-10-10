using Microsoft.EntityFrameworkCore;

using System.Reflection;

namespace SqlAuditLogImporter;
public class AppDbContext : DbContext {
	public AppDbContext(DbContextOptions<AppDbContext> options)
		: base(options) {
	}

	public AppDbContext() {
    }
    public DbSet<LogDTO> Logs => Set<LogDTO>();

	protected override void OnModelCreating(ModelBuilder modelBuilder) {
		base.OnModelCreating(modelBuilder);
		modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
	}
}
