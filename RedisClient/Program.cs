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
			var ip = "127.0.0.1";
			ushort port = 1234;

			var redisAgent = new RedisAgent(ip, port);
			redisAgent.Start();

			Console.ReadLine();
		}
	}
}
