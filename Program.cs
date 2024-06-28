using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Data;
using SpanRetriever1;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/getspan/{requestId}", (string requestId) =>
{
    string clusterUri = "https://aoaiagents1.westus.kusto.windows.net/";
    var kcsb = new KustoConnectionStringBuilder(clusterUri)
        .WithAadUserPromptAuthentication();
    var spanIds = new List<string>();

    using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
    {
        string database = "prod";

        // First query to go from requestId -> traceId
        string query = $@"
            union *
            | where TIMESTAMP > ago(2d)
            | where ['http.request.header.apim_request_id'] == '{requestId}'";

        using (var response = kustoClient.ExecuteQuery(database, query, null))
        {
            Console.WriteLine("Type of response: " + response.GetType().FullName);
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
                return Results.BadRequest($"Error: Expected exactly one traceId, but found {count}.");
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
                    Console.WriteLine("SpanId - {0}", secondResponse.GetString(columnSpanId));
                    var spanId = secondResponse.GetString(columnSpanId);
                    spanIds.Add(spanId);
                }
            }
        }
    }
    return Results.Ok(JsonConvert.SerializeObject(spanIds));
});

 
app.Run();
