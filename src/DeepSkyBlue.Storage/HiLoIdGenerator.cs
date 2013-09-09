using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using DeepSkyBlue.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table;

namespace DeepSkyBlue.Storage
{
	public class HiLoIdGenerator
	{
		private readonly static CloudTable HiloTable = CloudStorageAccountProvider.GetCloudStorageAccount().CreateCloudTableClient().GetTableReference("hilo");
		
		private readonly string tableName;
		private readonly int chunkSize;
		private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
		private readonly object nextLock = new object();
		private long last = -1;
		private long max = -1;

		public HiLoIdGenerator(string tableName, int chunkSize)
		{
			this.tableName = tableName;
			this.chunkSize = chunkSize;
		}

		public async Task<long> NextId()
		{
			lock (nextLock)
			{
				if (last < max)
				{
					return ++last;
				}
			}
			await semaphore.WaitAsync();
			try
			{
				lock (nextLock)
				{
					if (last < max)
					{
						return ++last;
					}
				}
				for (var tries = 0; tries < 10; ++tries)
				{
					HttpStatusCode saveResultCode;
					long oldMax, newMax;
					var hiEntity = await HiloTable.RetreiveAsync(tableName, "hi");
					if (hiEntity == null)
					{
						oldMax = 0;
						newMax = chunkSize;
						hiEntity = new DynamicTableEntity(tableName, "hi");
						hiEntity["max"] = new EntityProperty(newMax);
						saveResultCode = await HiloTable.InsertAsync(hiEntity);
					}
					else
					{
						oldMax = hiEntity["max"].Int64Value.GetValueOrDefault(0);
						newMax = oldMax + chunkSize;
						hiEntity["max"] = new EntityProperty(newMax);
						saveResultCode = await HiloTable.ReplaceAsync(hiEntity);
					}
					if (saveResultCode == HttpStatusCode.Created || saveResultCode == HttpStatusCode.NoContent)
					{
						lock (nextLock)
						{
							last = oldMax;
							max = newMax;
							return ++last;
						}
					}
				}
				throw new Exception(
					string.Format(
						"Could not allocate id range for table '{0}' with chunkSize={1} due to high contention.\r\nConsider increasing the chunk size",
						tableName, chunkSize));
			}
			finally
			{
				semaphore.Release();
			}

		}
	}
}
