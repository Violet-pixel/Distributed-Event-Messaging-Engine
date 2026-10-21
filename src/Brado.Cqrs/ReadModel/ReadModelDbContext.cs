using Microsoft.EntityFrameworkCore;

namespace Brado.Cqrs.ReadModel;

public sealed class ReadModelOptions
{
    public const string Section = "ReadModel";

    public string Provider { get; set; } = "Sqlite";
    public string ConnectionString { get; set; } = "Data Source=data/brado-read.db";
}

public sealed class OrderSummaryView
{
    public Guid OrderId { get; set; }
    public string CustomerRef { get; set; } = "";
    public string Currency { get; set; } = "BRL";
    public string Status { get; set; } = "Draft";
    public int ItemCount { get; set; }
    public long TotalCents { get; set; }
    public string? PaymentRef { get; set; }
    public string? TrackingCode { get; set; }
    public string? FailureReason { get; set; }
    public long Version { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}

public sealed class OrderLineView
{
    public long Id { get; set; }
    public Guid OrderId { get; set; }
    public string Sku { get; set; } = "";
    public int Quantity { get; set; }
    public long UnitPriceCents { get; set; }
}

public sealed class DailySalesView
{
    public DateOnly Day { get; set; }
    public int PaidOrders { get; set; }
    public long RevenueCents { get; set; }
    public int Refunds { get; set; }
    public long RefundedCents { get; set; }
}

public sealed class ProjectionCheckpoint
{
    public string Projection { get; set; } = "";
    public long Position { get; set; }
    public DateTime UpdatedAt { get; set; }
}

public sealed class ReadModelDbContext(DbContextOptions<ReadModelDbContext> options) : DbContext(options)
{
    public DbSet<OrderSummaryView> Orders => Set<OrderSummaryView>();
    public DbSet<OrderLineView> OrderLines => Set<OrderLineView>();
    public DbSet<DailySalesView> DailySales => Set<DailySalesView>();
    public DbSet<ProjectionCheckpoint> Checkpoints => Set<ProjectionCheckpoint>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderSummaryView>(e =>
        {
            e.ToTable("rm_orders");
            e.HasKey(x => x.OrderId);
            e.Property(x => x.CustomerRef).HasMaxLength(120);
            e.Property(x => x.Currency).HasMaxLength(3);
            e.Property(x => x.Status).HasMaxLength(32);
            e.Property(x => x.PaymentRef).HasMaxLength(80);
            e.Property(x => x.TrackingCode).HasMaxLength(80);
            e.Property(x => x.FailureReason).HasMaxLength(300);
            e.HasIndex(x => new { x.Status, x.UpdatedAt });
            e.HasIndex(x => x.UpdatedAt);
        });

        modelBuilder.Entity<OrderLineView>(e =>
        {
            e.ToTable("rm_order_lines");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedOnAdd();
            e.Property(x => x.Sku).HasMaxLength(64);
            e.HasIndex(x => x.OrderId);
        });

        modelBuilder.Entity<DailySalesView>(e =>
        {
            e.ToTable("rm_daily_sales");
            e.HasKey(x => x.Day);
        });

        modelBuilder.Entity<ProjectionCheckpoint>(e =>
        {
            e.ToTable("rm_checkpoints");
            e.HasKey(x => x.Projection);
            e.Property(x => x.Projection).HasMaxLength(100);
        });
    }
}
