using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisClient
{
	class Program
	{
		static void Main(string[] args)
		{
			var ip = "10.1.1.192";
			ushort port = 6379;

			var redisAgent = new RedisAgent(ip, port);
			redisAgent.Start();

			Task.Run(() => redisAgent.SetAdd(1, 1234)).Wait();
			Task.Run(() => redisAgent.HashSet(2, 1234)).Wait();

			Console.ReadLine();
		}
	}
}
