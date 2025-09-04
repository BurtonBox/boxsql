/// Standard database page size in bytes.
///
/// This 8KB (8192 bytes) page size is chosen to balance:
/// - Memory efficiency: Fits well in CPU cache
/// - I/O efficiency: Good disk read/write unit size
/// - Storage overhead: Not too large to waste space for small records
///
/// This matches the default page size used by PostgreSQL and SQL Server.
pub const PAGE_SIZE: usize = 8192;
