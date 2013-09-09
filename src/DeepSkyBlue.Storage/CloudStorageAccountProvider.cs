using System;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;

namespace DeepSkyBlue.Storage
{
	public class CloudStorageAccountProvider
	{
		public static CloudStorageAccount GetCloudStorageAccount()
		{
			string connectionString = null;
			var connectionStringEntry = ConfigurationManager.ConnectionStrings["DeepSkyBlueConnection"];
			if (connectionStringEntry != null)
			{
				connectionString = connectionStringEntry.ConnectionString;
			}
			if (string.IsNullOrEmpty(connectionString) || connectionString == "STUB")
			{
				connectionString =
					Environment.GetEnvironmentVariable("DeepSkyBlueConnection", EnvironmentVariableTarget.Process) ??
					Environment.GetEnvironmentVariable("DeepSkyBlueConnection", EnvironmentVariableTarget.User) ??
					Environment.GetEnvironmentVariable("DeepSkyBlueConnection", EnvironmentVariableTarget.Machine);
			}

			var cloud = CloudStorageAccount.Parse(connectionString);

			return cloud;
		}
	}
}