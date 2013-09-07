using System;
using System.Linq;

namespace DeepSkyBlue.Storage.Table
{
	public abstract class PocoMapper<T>
	{
		static PocoMapper()
		{
			var openType = typeof(PocoMapper<>);
			var thisType = openType.MakeGenericType(typeof(T));

			var type = openType.Assembly.GetExportedTypes().FirstOrDefault(t => t.BaseType == thisType);

			if (type == null)
			{
				throw new InvalidOperationException(string.Format("Mapper for type {0} could not be found", typeof(T)));
			}
			Instance = (PocoMapper<T>)Activator.CreateInstance(type);
		}

		public static PocoMapper<T> Instance { get; private set; }

		public abstract string GetRowKey(T poco);
		public abstract string GetPartitionKey(T poco);
	}
}