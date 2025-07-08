using System.Reflection;

namespace WebApi.Data
{
    public class ColumnDefinition
    {
        public string Name { get; set; } = null!;
        public string DatabaseType { get; set; } = null!;
        public Type PropertyType { get; set; } = null!;
        public PropertyInfo PropertyInfo { get; set; } = null!;
        public bool PrimaryKey { get; set; }
        public bool Identity { get; set; }
        public bool Computed { get; set; }
    }
}
