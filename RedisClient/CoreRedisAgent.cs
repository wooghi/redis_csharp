using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RedisClient
{
	public abstract class CoreRedisAgent
	{
		// In general, let StackExchange.Redis handle most reconnects, 
		// so limit the frequency of how often this will actually reconnect.
		private static TimeSpan reconnectMinFrequency = TimeSpan.FromSeconds(60);

		// if errors continue for longer than the below threshold, then the 
		// multiplexer seems to not be reconnecting, so re-create the multiplexer
		private static TimeSpan reconnectErrorThreshold = TimeSpan.FromSeconds(30);

		private static TimeSpan executeRetryTime = TimeSpan.FromSeconds(5);
		private EndPoint redisServerEndPoint;
		private Lazy<ConnectionMultiplexer> multiplexer;

		private DateTimeOffset lastReconnectTime = DateTimeOffset.MinValue;
		private DateTimeOffset firstError = DateTimeOffset.MinValue;
		private DateTimeOffset previousError = DateTimeOffset.MinValue;

		private object reconnectLock = new object();

		public CoreRedisAgent(string ip, ushort port)
		{
			IPAddress ipAddress;
			if (IPAddress.TryParse(ip, out ipAddress))
			{
				this.redisServerEndPoint = new IPEndPoint(ipAddress, port);
			}
			else
			{
				this.redisServerEndPoint = new DnsEndPoint(ip, port);
			}
		}

		protected ConnectionMultiplexer Connection
		{
			get
			{
				return this.multiplexer.Value;
			}
		}

		protected IDatabase Database
		{
			get
			{
				return this.Connection.GetDatabase();
			}
		}

		public void Connect()
		{
			try
			{
				this.multiplexer = this.ConnectTo();
				Console.WriteLine("[CoreRedisAgent] Connected to Redis[{0}]", this.redisServerEndPoint.ToString());
				this.Connection.ConnectionFailed += this.ConnectionFailed;
				this.Connection.ConnectionRestored += this.ConnectionRestored;
			}
			catch (ObjectDisposedException e)
			{
				Console.WriteLine("[CoreRedisAgent] Connect ObjectDisposedException [{0}]", e.ToString());
			}
		}

		protected void ConnectionRestoredInternal()
		{
		}

		protected async Task<T> Execute<T>(Func<Task<T>> func, [CallerFilePath] string file = "", [CallerMemberName] string caller = "", [CallerLineNumber] int line = 0)
		{
			while (true)
			{
				try
				{
					return await func();
				}
				catch (RedisConnectionException e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");
					this.ForceReconnect();
				}
				catch (RedisServerException e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");
					var exceptionString = e.ToString().ToLower();
					if (exceptionString.Contains("endpoint")
						&& exceptionString.Contains("serving hashslot")
						&& exceptionString.Contains("is not reachable at this point of time"))
					{
						this.ForceReconnect();
					}
				}
				catch (ObjectDisposedException e)
				{
					Console.WriteLine("[CoreRedisAgent] ObjectDisposedException {0}", e.ToString());
				}
				catch (Exception e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");
				}

				await Task.Delay(executeRetryTime);
			}
		}

		protected async Task Execute(Func<Task> func, [CallerFilePath] string file = "", [CallerMemberName] string caller = "", [CallerLineNumber] int line = 0)
		{
			while (true)
			{
				try
				{
					await func();
					return;
				}
				catch (RedisConnectionException e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");
					this.ForceReconnect();
				}
				catch (RedisServerException e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");

					var exceptionString = e.ToString().ToLower();
					if (exceptionString.Contains("endpoint")
						&& exceptionString.Contains("serving hashslot")
						&& exceptionString.Contains("is not reachable at this point of time"))
					{
						this.ForceReconnect();
					}
				}
				catch (ObjectDisposedException e)
				{
					Console.WriteLine("[CoreRedisAgent] ObjectDisposedException {0}", e.ToString());
				}
				catch (Exception e)
				{
					var lastFileName = file.Split('\\').LastOrDefault();
					Console.WriteLine($"[CoreRedisAgent] failed to {lastFileName}:{line} {caller}, Exception:{e.ToString()}");
				}

				await Task.Delay(executeRetryTime);
			}
		}

		private void ConnectionFailed(object sender, ConnectionFailedEventArgs args)
		{
			Console.WriteLine("[CoreRedisAgent] ConnectionnFailed. To[{0}]", args.EndPoint.ToString());
		}

		private void ConnectionRestored(object sender, ConnectionFailedEventArgs args)
		{
			Console.WriteLine("[CoreRedisAgent] ConnectionRestored. To[{0}]", args.EndPoint.ToString());
			this.ConnectionRestoredInternal();
		}

		private Lazy<ConnectionMultiplexer> ConnectTo()
		{
			Lazy<ConnectionMultiplexer> conn;
			int retrySeconds = 5;
			int tryCount = 0;

			while (true)
			{
				Console.WriteLine($"[CoreRedisAgent] Try to CreateMultiplexer TryCount:{++tryCount}");
				conn = this.CreateMultiplexer();
				if (conn == null)
				{
					Console.WriteLine($"[CoreRedisAgent] ConnectTo conn is null.");
					continue;
				}

				if (conn.Value == null)
				{
					Console.WriteLine($"[CoreRedisAgent] ConnectTo conn.Value is null.");
					continue;
				}

				if (conn.Value.IsConnected)
				{
					break;
				}
				else
				{
					Console.WriteLine($"[CoreRedisAgent] IsNotConnected to Redis[{this.redisServerEndPoint.ToString()}]{conn.Value.Configuration}{conn.Value.GetStatus()}");
				}

				Thread.Sleep(retrySeconds * 1000);
			}

			// https://stackexchange.github.io/StackExchange.Redis/PubSubOrder
			// pubsub message 의 순서 보장이 현재는 필요하지 않아서 disable.
			conn.Value.PreserveAsyncOrder = false;
			return conn;
		}

		private Lazy<ConnectionMultiplexer> CreateMultiplexer()
		{
			try
			{
				//// conetiontimeout 15seconds. MS recommandation
				ConfigurationOptions config = new ConfigurationOptions
				{
					EndPoints = { this.redisServerEndPoint, },
					AbortOnConnectFail = false,
					ConnectTimeout = 15 * 1000,
					AllowAdmin = true,
				};

				Console.WriteLine("[CoreRedisAgent] CreateMultiplexer connect");

				return new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(config));
			}
			catch (RedisConnectionException e)
			{
				Console.WriteLine("[CoreRedisAgent] Can't Connect to Redis[{0}] Error: {1}", this.redisServerEndPoint, e.ToString());
				return null;
			}
		}

		//https://gist.github.com/JonCole/925630df72be1351b21440625ff2671f 참고    
		private void ForceReconnect()
		{
			Console.WriteLine("[CoreRedisAgent] Enter ForceReconnect()");
			var utcNow = DateTimeOffset.UtcNow;
			var previousReconnect = this.lastReconnectTime;
			var elapsedSinceLastReconnect = utcNow - previousReconnect;

			// If mulitple threads call ForceReconnect at the same time, we only want to honor one of them.
			if (elapsedSinceLastReconnect > reconnectMinFrequency)
			{
				lock (this.reconnectLock)
				{
					utcNow = DateTimeOffset.UtcNow;
					elapsedSinceLastReconnect = utcNow - this.lastReconnectTime;

					if (this.firstError == DateTimeOffset.MinValue)
					{
						// We haven't seen an error since last reconnect, so set initial values.
						this.firstError = utcNow;
						this.previousError = utcNow;
						Console.WriteLine("[CoreRedisAgent] ForceReconnect this.firstError == DateTimeOffset.MinValue");
						return;
					}

					if (elapsedSinceLastReconnect < reconnectMinFrequency)
					{
						Console.WriteLine("[CoreRedisAgent] ForceReconnect elapsedSinceLastReconnect < reconnectMinFrequency");
						return; // Some other thread made it through the check and the lock, so nothing to do.
					}

					var elapsedSinceFirstError = utcNow - this.firstError;
					var elapsedSinceMostRecentError = utcNow - this.previousError;

					var shouldReconnect =
						elapsedSinceFirstError >= reconnectErrorThreshold   // make sure we gave the multiplexer enough time to reconnect on its own if it can
						&& elapsedSinceMostRecentError <= reconnectErrorThreshold; //make sure we aren't working on stale data (e.g. if there was a gap in errors, don't reconnect yet).

					// Update the previousError timestamp to be now (e.g. this reconnect request)
					this.previousError = utcNow;

					if (shouldReconnect)
					{
						this.firstError = DateTimeOffset.MinValue;
						this.previousError = DateTimeOffset.MinValue;

						var oldMultiplexer = this.multiplexer;
						this.CloseMultiplexer(oldMultiplexer);
						Console.WriteLine("[CoreRedisAgent] ForceReconnect try to connect");
						this.Connect();
						this.lastReconnectTime = utcNow;
						Console.WriteLine("[CoreRedisAgent] ForceReconnect End");
					}
					else
					{
						Console.WriteLine("[CoreRedisAgent] ForceReconnect skip. shouldReconnect is not true");
					}
				}
			}
		}

		private void CloseMultiplexer(Lazy<ConnectionMultiplexer> oldMultiplexer)
		{
			if (oldMultiplexer != null)
			{
				try
				{
					oldMultiplexer.Value.Close();
				}
				catch (Exception e)
				{
					// Example error condition: if accessing old.Value causes a connection attempt and that fails.
					Console.WriteLine($"[CoreRedisAgent] CloseMultiplexer failed. {e.ToString()}");
				}
			}
		}
	}
}
