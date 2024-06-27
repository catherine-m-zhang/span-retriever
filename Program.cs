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
                    Console.WriteLine("Error: Expected one argument (APIM Request ID)");
                
                }
            
            string clusterUri = "https://aoaiagents1.westus.kusto.windows.net/";
            var kcsb = new KustoConnectionStringBuilder(clusterUri)
                .WithAadUserPromptAuthentication();

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                string database = "prod";
                var reqId = args[0];

                // First query to go from requestId -> traceId
                string query = $@"
                                union *
                                | where TIMESTAMP > ago(2d)
                                | where ['http.request.header.apim_request_id'] == '{reqId}'";

                using (var response = kustoClient.ExecuteQuery(database, query, null))
                {
                    int columnTraceId = response.GetOrdinal("env_dt_traceId");
                    string traceId = null;
                    int count = 0;

                    while (response.Read())
                    {
                        count++;
                        traceId = response.GetString(columnTraceId);
                    }
                    if (count != 1)
                    {
                        Console.WriteLine($"Error: Expected exactly one traceId, but found {count}.");
                        return;
                    }
                    Console.WriteLine("TraceId: " + traceId);
                    // Second query to go from traceId -> span
                    string secondQuery = $@"
                                          Span 
                                          | where env_dt_traceId has '{traceId}'";

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