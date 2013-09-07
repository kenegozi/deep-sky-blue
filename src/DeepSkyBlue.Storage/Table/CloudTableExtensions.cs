using System.Net;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DeepSkyBlue.Storage.Table
{
	public static class CloudTableExtensions
	{
		public static async Task<HttpStatusCode> InsertAsync(this CloudTable table, ITableEntity entity)
		{
			var op = TableOperation.Insert(entity);
			var result = await table.ExecuteAsyncDontThrow(op);
			return (HttpStatusCode)result.HttpStatusCode;
		}

		public static Task<HttpStatusCode> ReplaceAsync(this CloudTable table, ITableEntity entity)
		{
			return ReplaceAsync(table, entity, insertOnMissing: false);
		}

		public static async Task<HttpStatusCode> ReplaceAsync(this CloudTable table, ITableEntity entity, bool insertOnMissing)
		{
			var op = insertOnMissing
				? TableOperation.InsertOrReplace(entity)
				: TableOperation.Replace(entity);
			var result = await table.ExecuteAsyncDontThrow(op);
			return (HttpStatusCode)result.HttpStatusCode;
		}

		public static Task<HttpStatusCode> MergeAsync(this CloudTable table, ITableEntity entity)
		{
			return MergeAsync(table, entity, insertOnMissing: false);
		}

		public static async Task<HttpStatusCode> MergeAsync(this CloudTable table, ITableEntity entity, bool insertOnMissing)
		{
			var op = insertOnMissing
				? TableOperation.InsertOrMerge(entity)
				: TableOperation.Merge(entity);
			var result = await table.ExecuteAsyncDontThrow(op);
			return (HttpStatusCode)result.HttpStatusCode;
		}

		public static async Task<TableResult> ExecuteAsyncDontThrow(this CloudTable table, TableOperation op)
		{
			try
			{
				var result = await table.ExecuteAsync(op);
				return result;
			}
			catch (StorageException ex)
			{
				var webex = ex.InnerException as WebException;
				if (webex == null)
				{
					throw;
				}
				var response = webex.Response as HttpWebResponse;
				if (response == null)
				{
					throw;
				}
				if (response.StatusCode == HttpStatusCode.PreconditionFailed || response.StatusCode == HttpStatusCode.Conflict)
				{
					var result = new TableResult { HttpStatusCode = (int)response.StatusCode };
					return result;
				}
				throw;
				//		((System.Net.HttpWebResponse)(((System.Net.WebException)ex.InnerException).Response)).StatusCode
			}
		}

		public static async Task<TableEntity<T>> RetreiveAsync<T>(this CloudTable table, string partitionKey, string rowKey) where T : new()
		{
			var op = TableOperation.Retrieve<TableEntity<T>>(partitionKey, rowKey);
			var result = await table.ExecuteAsync(op);

			var resultItem = result.Result as TableEntity<T>;
			if (resultItem != null)
			{
				return resultItem;
			}

			if (result.HttpStatusCode == 404)
			{
				return null;
			}

			throw new StorageException(
				string.Format("Could not retreive item '{0}','{1}' from table '{2}'. Response code was {3}", partitionKey, rowKey,
					table.Name, result.HttpStatusCode));
		}
		public static async Task<DynamicTableEntity> RetreiveAsync(this CloudTable table, string partitionKey, string rowKey)
		{
			var op = TableOperation.Retrieve(partitionKey, rowKey);
			var result = await table.ExecuteAsync(op);
			return result.Result as DynamicTableEntity;
		}

	}
}
