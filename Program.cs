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
using OpenTelemetry.Proto.Trace.V1;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/getspan/{requestId}", (string requestId) =>
{
    string clusterUri = "https://aoaiagents1.westus.kusto.windows.net/";
    var kcsb = new KustoConnectionStringBuilder(clusterUri)
        .WithAadUserPromptAuthentication();
    var spanIds = new List<string>();
    var spanDataList = new List<SpanData>();
    using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
    {
        string database = "prod";
        // First query to go from requestId -> traceId
        string query = $@"
            union *
            | where TIMESTAMP > ago(7d)
            | where ['http.request.header.apim_request_id'] == '{requestId}'";

        using (var response = kustoClient.ExecuteQuery(database, query, null))
        {
            string traceId = null;
            int count = 0;

            while (response.Read())
            {
                count++;
                traceId = response["env_dt_traceId"].ToString();
            }

            if (count != 1)
            {
                return Results.BadRequest($"Error: Expected exactly one traceId, but found {count}.");
            }

            Console.WriteLine("TraceId: " + traceId);
            //List<SpanData> spanDataList;

            // Second query to go from traceId -> span
            string secondQuery = $@"
                Span 
                | where env_dt_traceId has '{traceId}'";

            using (var secondResponse = kustoClient.ExecuteQuery(database, secondQuery, null))
            {
                spanDataList = DataConverter.ConvertResponseToSpanData(secondResponse);
   
                while (secondResponse.Read())
                {
                    var spanId = secondResponse["env_dt_spanId"].ToString();
                    Console.WriteLine("SpanId - {0}", spanId);
                    spanIds.Add(spanId);
                }

            }
            //spans = DataConverter.ConvertSpanDataToSpan(spanDataList);

        }
    }
    var spans = DataConverter.ConvertSpanDataToSpan(spanDataList);
    // Return spans as JSON
    var jsonResponse = JsonConvert.SerializeObject(spans, new JsonSerializerSettings
    {
        Formatting = Formatting.Indented,
        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
    });

    return Results.Ok(jsonResponse);
    //return Results.Ok(JsonConvert.SerializeObject(spanIds));
});

 
app.Run();
