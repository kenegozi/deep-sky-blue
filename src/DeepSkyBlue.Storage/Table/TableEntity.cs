using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DeepSkyBlue.Storage.Table
{
	public class TableEntity<T> : TableEntity where T : new()
	{
		[IgnoreProperty]
		public T Entity { get; private set; }

		private static Type TypeOfT = typeof(T);
		private static readonly Dictionary<string, Tuple<Type, Action<T, object>>> Setters = new Dictionary<string, Tuple<Type, Action<T, object>>>();
		private static readonly Dictionary<string, Tuple<Type, Func<T, object>>> Getters = new Dictionary<string, Tuple<Type, Func<T, object>>>();

		static TableEntity()
		{
			foreach (var propertyInfo in TypeOfT.GetProperties())
			{
				if (propertyInfo.GetCustomAttribute<ServerIgnoreAttribute>() != null)
				{
					continue;
				}
				var setMethod = propertyInfo.GetSetMethod(nonPublic: false) ?? propertyInfo.GetSetMethod(nonPublic: true);
				var getMethod = propertyInfo.GetGetMethod();
				if (setMethod == null && getMethod == null)
				{
					continue;
				}
				var target = Expression.Parameter(typeof(T), "target");
				var value = Expression.Parameter(typeof(object), "value");
				if (setMethod != null)
				{
					var setterBody = Expression.Call(target, setMethod,
						Expression.Convert(value, propertyInfo.PropertyType));
					var setter = Expression.Lambda<Action<T, object>>(setterBody, target, value).Compile();
					Setters[propertyInfo.Name] = new Tuple<Type, Action<T, object>>(propertyInfo.PropertyType, setter);
				}
				if (getMethod != null)
				{
					Expression getterBody = Expression.Call(target, getMethod);
					if (getMethod.ReturnType != typeof(object))
					{
						getterBody = Expression.Convert(getterBody, typeof(object));
					}
					var getter = Expression.Lambda<Func<T, object>>(getterBody, target).Compile();
					Getters[propertyInfo.Name] = new Tuple<Type, Func<T, object>>(propertyInfo.PropertyType, getter);
				}
			}
		}


		public static TableEntity<T> For(T entity)
		{
			var te = new TableEntity<T>();
			te.Entity = entity;
			var mapper = PocoMapper<T>.Instance;
			te.PartitionKey = mapper.GetPartitionKey(entity);
			te.RowKey = mapper.GetRowKey(entity);
			return te;
		}


		public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
		{
			//base.ReadEntity(properties, operationContext);
			Entity = new T();
			foreach (var entry in properties)
			{
				Tuple<Type, Action<T, object>> setter;
				if (!Setters.TryGetValue(entry.Key, out setter) || setter == null)
				{
					continue;
				}
				object value = entry.Value.PropertyAsObject;
				if (setter.Item1 == typeof(DateTimeOffset))
				{
					value = entry.Value.DateTimeOffsetValue;
				}
				setter.Item2(Entity, value);
			}
		}

		public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
		{
			var properties = new Dictionary<string, EntityProperty>();
			foreach (var getter in Getters)
			{
				var value = getter.Value.Item2(Entity);
				if (value == null)
				{
					continue;
				}
				var type = getter.Value.Item1;
				EntityProperty entityProperty = null;
				if (type == typeof(bool) || type == typeof(bool?))
				{
					entityProperty = EntityProperty.GeneratePropertyForBool((bool?)value);
				}
				else if (type == typeof(int) || type == typeof(int?))
				{
					entityProperty = EntityProperty.GeneratePropertyForInt((int?)value);
				}
				else if (type == typeof(long) || type == typeof(long?))
				{
					entityProperty = EntityProperty.GeneratePropertyForLong((long?)value);
				}
				else if (type == typeof(double) || type == typeof(double?))
				{
					entityProperty = EntityProperty.GeneratePropertyForDouble((double?)value);
				}
				else if (type == typeof(float) || type == typeof(float?))
				{
					entityProperty = EntityProperty.GeneratePropertyForDouble((float?)value);
				}
				else if (type == typeof(DateTime) || type == typeof(DateTime?))
				{
					entityProperty = EntityProperty.GeneratePropertyForDateTimeOffset((DateTime?)value);
				}
				else if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
				{
					entityProperty = EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset?)value);
				}
				else if (type == typeof(Guid) || type == typeof(Guid?))
				{
					entityProperty = EntityProperty.GeneratePropertyForGuid((Guid?)value);
				}
				else if (type == typeof(byte[]))
				{
					entityProperty = EntityProperty.GeneratePropertyForByteArray(value as byte[]);
				}
				else if (type == typeof(string))
				{
					entityProperty = EntityProperty.GeneratePropertyForString(value as string);
				}

				if (entityProperty != null)
				{
					properties[getter.Key] = entityProperty;
				}
			}
			return properties;
		}
	}
}