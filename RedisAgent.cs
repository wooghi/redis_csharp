using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisClient
{
	public class RedisAgent : CoreRedisAgent
	{
		public RedisAgent(string ip, ushort port) : base(ip, port)
		{

		}

		public void Start()
		{
			Console.WriteLine("RedisAgent Start");
			this.Connect();
		}

		//ex
		public async Task<bool> HashSet(long id, int val)
		{
			return await this.Execute(async () => await this.HashSetImpl(id, val));
		}

		public async Task SetAdd(long id, int val)
		{
			var key = this.GetSomethingKey(id, val);
			await this.Execute(async () => await this.Database.SetAddAsync(key, val, CommandFlags.FireAndForget));
		}

		public string GetSomethingKey(long id, int val)
		{
			return string.Format("{0}:something:propertyName:{{{1}}}", id, val);
		}

		private async Task<bool> HashSetImpl(long id, int val)
		{
			bool result = false;
			string key = this.GetSomethingKey(id, val);

			var trans = this.Database.CreateTransaction();

			////trans.AddCondition(Condition.HashEqual(key, "finished", false));
			trans.HashIncrementAsync(key, "currentPlayerCount");

			result = await trans.ExecuteAsync();

			return result;
		}
	}
}
