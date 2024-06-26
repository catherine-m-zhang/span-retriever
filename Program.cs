using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System.Diagnostics;

namespace BasicQuery
{
    class BasicQuery
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
                {
                    Console.WriteLine("Error: Expected one argument but found {args.Length}");
                return;
                }
  
            string clusterUri = "https://aoaiagents1.westus.kusto.windows.net/";
            var kcsb = new KustoConnectionStringBuilder(clusterUri)
                .WithAadUserPromptAuthentication();

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                string database = "prod";
                string query = @"declare query_parameters(request_id:string);
                              union *
                               | where TIMESTAMP > ago(2h)
                               | where ['http.request.header.apim_request_id'] == request_id
                               | take 10";
                       
                var crp = new ClientRequestProperties();
                crp.SetParameter("request_id", args[0]);
                Console.WriteLine("requestid:" + args[0]+ "#####");

                // Check if args[0] equals a specific value
                bool isSpecificRequestId = args[0] == "83c54f28-4754-49b0-a95d-7502d533e03a";

                if (isSpecificRequestId)
                {
                    Console.WriteLine("The provided request ID matches the specific value.");
                }
                else
                {
                    Console.WriteLine("The provided request ID does not match the specific value.");
                }

                /*
                string query = @"declare query_parameters(event_type:string, daily_damage:int);
                                 StormEvents
                                 | where EventType == event_type
                                 | extend TotalDamage = DamageProperty + DamageCrops
                                 | summarize DailyDamage=sum(TotalDamage) by State, bin(StartTime, 1d)
                                 | where DailyDamage > daily_damage
                                 | order by DailyDamage desc";

                var crp = new ClientRequestProperties();
                crp.ClientRequestId = "QueryDemo" + Guid.NewGuid().ToString();
                crp.SetOption(ClientRequestProperties.OptionServerTimeout, "1m");
                crp.SetParameter("event_type", args[0]);
                crp.SetParameter("daily_damage", args[1]);
                */

                using (var response = kustoClient.ExecuteQuery(database, query, crp))
                {
                    int columnTraceId = response.GetOrdinal("env_dt_traceId");
                    string traceId = null;
                    int count = 0;

                    while (response.Read())
                    {
                        count++;
                        traceId = response.GetString(columnTraceId);
                        Console.WriteLine("TraceId - {0}",
                              response.GetString(columnTraceId));

                    }
                    if (count != 1)
                    {
                        Console.WriteLine($"Error: Expected exactly one traceId, but found {count}.");
                        return;
                    }
                    Console.WriteLine("TraceId: " + traceId);
                    string secondQuery = @"Span
                        | where env_dt_traceId has '35fad2df0cf9519d4fe405bd7a78c516'";
                    using (var secondResponse = kustoClient.ExecuteQuery(database, secondQuery, null))
                    {
                        int columnSpanId = secondResponse.GetOrdinal("env_dt_spanId");

                        while (secondResponse.Read())
                        {
                            Console.WriteLine("SpanId - {0}",
                              secondResponse.GetString(columnSpanId));
                        }
                    }
                }
            }
        }
    }
}