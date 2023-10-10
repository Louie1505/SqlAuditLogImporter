using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using Microsoft.SqlServer.XEvent.Linq;

using System.Reflection;

namespace SqlAuditLogImporter;
public class Definitions {
    public const string TableName = "Imported_Logs";
    public static Dictionary<string, string> FieldMappings => new() {
        {nameof(LogDTO.Duration), "duration_milliseconds"},
        {nameof(LogDTO.ResponseRowCount), "response_rows"},
        {nameof(LogDTO.AffectedRowCount), "affected_rows"},
        {nameof(LogDTO.ObjectName), "object_name"},
        //{nameof(LogDTO.Writes), "writes"},
        //{nameof(LogDTO.CpuTime), "cpu_time"},
        //{nameof(LogDTO.DatabaseId), "database_id"},
        {nameof(LogDTO.ServerPrincipalName), "server_principal_name"},
        {nameof(LogDTO.SessionServerPrincipalName), "session_server_principal_name"},
        {nameof(LogDTO.SessionId), "session_id"},
        {nameof(LogDTO.ClientIp), "client_ip"},
        {nameof(LogDTO.ServerName), "server_instance_name"},
        {nameof(LogDTO.QueryText), "statement"},
        {nameof(LogDTO.EventTime), "event_time"},
    };

}
public class LogDTO {
    public int Id { get; set; }
    public int SessionId { get; set; }
    public string? ServerName { get; set; }
    public string? ObjectName { get; set; }
    //public int Writes { get; set; }
    //public long CpuTime { get; set; }
    //public int DatabaseId { get; set; }
    public string? ServerPrincipalName { get; set; }
    public string? SessionServerPrincipalName { get; set; }
    public string? ClientIp { get; set; }
    public DateTime EventTime { get; set; }
    public string? QueryText { get; set; }
    public long Duration { get; set; }
    public int ResponseRowCount { get; set; }
    public int AffectedRowCount { get; set; }
    public LogDTO() { }
    public LogDTO(PublishedEvent.FieldList fields) {
        foreach (var key in Definitions.FieldMappings.Keys) {
            var fieldName = Definitions.FieldMappings[key];
            if (fields.TryGetValue(fieldName, out PublishedEventField field)) {
                PropertyInfo? propInfo = typeof(LogDTO).GetProperty(key);
                if (propInfo is not null) {
                    switch (Type.GetTypeCode(propInfo.PropertyType)) {
                        default:
                        case TypeCode.String:
                            propInfo.SetValue(this, field.Value.ToString());
                            break;
                        case TypeCode.Int32:
                            propInfo.SetValue(this, int.Parse(field.Value.ToString() ?? "0"));
                            break;
                        case TypeCode.Int64:
                            propInfo.SetValue(this, long.Parse(field.Value.ToString() ?? "0"));
                            break;
                        case TypeCode.DateTime:
                            propInfo.SetValue(this, DateTime.Parse(field.Value.ToString() ?? DateTime.MinValue.ToString()));
                            break;
                    }
                }
            }
        }
    }
}

public class LogConfiguration : IEntityTypeConfiguration<LogDTO> {
    public void Configure(EntityTypeBuilder<LogDTO> builder) {
        builder.ToTable(Definitions.TableName);
        builder.HasKey(x => x.Id);
    }
}
