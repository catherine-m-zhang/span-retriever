using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using OpenTelemetry.Internal;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Trace;
using SpanRetriever1;
using OtlpTrace = OpenTelemetry.Proto.Trace.V1;
//not sure if should use OpenTelemetry.Trace.Status instead
using Status = OpenTelemetry.Proto.Trace.V1.Status;
using Kusto.Cloud.Platform.Msal;

namespace SpanRetriever1
{
    public class DataConverter
    {
        // Not sure if it is the correct type
        public static List<SpanData> ConvertResponseToSpanData(IDataReader response)
        {
            var spanDataList = new List<SpanData>();

            while (response.Read())
            {
                var traceIdBase64 = response["env_dt_traceId"].ToString();
                var traceIdBytes = Convert.FromBase64String(traceIdBase64);
                if (traceIdBytes.Length != 16)
                {
                    throw new ArgumentException($"TraceId must be 16 bytes long. Actual length: {traceIdBytes.Length}. Base64 value: {traceIdBase64}");
                }

                var spanIdBase64 = response["env_dt_spanId"].ToString();
                var spanIdBytes = Convert.FromBase64String(spanIdBase64);
                if (spanIdBytes.Length != 8)
                {
                    throw new ArgumentException($"SpanId must be 8 bytes long. Actual length: {spanIdBytes.Length}. Base64 value: {spanIdBase64}");
                }

                var span = new SpanData
                {

                    // Initialize identifiers
                    TraceId = Convert.FromBase64String(response["env_dt_traceId"].ToString()),
                    SpanId = Convert.FromBase64String(response["env_dt_spanId"].ToString()),
                    ParentSpanId = response.IsDBNull(response.GetOrdinal("parentId")) ? null : Convert.FromBase64String(response.GetString(response.GetOrdinal("parentId"))),
                    Name = response["name"].ToString(),
                    Kind = Convert.ToInt32(response["kind"]),
                    StartTime = DateTimeOffset.Parse(response["startTime"].ToString()),
                    EndTime = DateTimeOffset.Parse(response["env_time"].ToString()),
                    LibraryName = response["instrumentation_scope.name"].ToString(),
                    LibraryVersion = response["telemetry.sdk.version"].ToString(),
                    Status = new SpanStatus
                    {
                        StatusCode = Convert.ToInt32(response["httpStatusCode"]),
                        Description = response["statusMessage"].ToString()
                    },
                    
                };
                if (span.Kind == 1)
                {
                    AddServerSpanAttributes(span, response);
                }
                else if (span.Kind == 2)
                {
                    AddClientSpanAttributes(span, response);
                }
                else 
                {
                    AddConsumerProducerAttributes(span, response);
                }
                spanDataList.Add(span);
            }

            return spanDataList;
        }

        private static void AddClientSpanAttributes(SpanData span, IDataReader response)
        {
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.method", response["http.request.method"]));
            span.Attributes.Add(new KeyValuePair<string, object>("server.address", response["server.address"]));
            span.Attributes.Add(new KeyValuePair<string, object>("server.port", response["server.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("url.full", response["url.full"]));
            span.Attributes.Add(new KeyValuePair<string, object>("error.type", response["error.type"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.method_original", response["http.request.method"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.response.status_code", response["http.response.status_code"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.protocol.name", response["network.protocol.version"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.peer.port", response["net.peer.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.protocol.version", response["network.protocol.version"]));
            //span.Attributes.Add(new KeyValuePair<string, object>("http.response.header", new List<string> { response["http.response.header.apim_request_id"].ToString(), response["http.response.header.x_request_id"].ToString(), response["http.response.header.x_organization_request_id"].ToString(), response["http.response.header.azure_resource_request_id"].ToString() }));
            span.Attributes.Add(new KeyValuePair<string, object>("url.scheme", response["url.scheme"]));
            span.Attributes.Add(new KeyValuePair<string, object>("user_agent.original", response["user_agent.original"]));
        }
        private static void AddServerSpanAttributes(SpanData span, IDataReader response)
        {
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.method", response["http.request.method"]));
            span.Attributes.Add(new KeyValuePair<string, object>("url.full", response["url.full"]));
            span.Attributes.Add(new KeyValuePair<string, object>("url.scheme", response["url.scheme"]));
            span.Attributes.Add(new KeyValuePair<string, object>("error.type", response["error.type"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.method_original", response["http.request.method"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.response.status_code", response["http.response.status_code"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.route", response["http.route"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.protocol.name", response["network.protocol.version"]));    
            span.Attributes.Add(new KeyValuePair<string, object>("server.port", response["server.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.peer.port", response["net.peer.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.protocol.version", response["network.protocol.version"]));
            span.Attributes.Add(new KeyValuePair<string, object>("server.address", response["server.address"]));
            span.Attributes.Add(new KeyValuePair<string, object>("user_agent.original", response["user_agent.original"]));
            //span.Attributes.Add(new KeyValuePair<string, object>("http.response.header", new List<string> { response["http.response.header.apim_request_id"].ToString(), response["http.response.header.x_request_id"].ToString(), response["http.response.header.x_organization_request_id"].ToString(), response["http.response.header.azure_resource_request_id"].ToString() }));
        }

        private static void AddConsumerProducerAttributes(SpanData span, IDataReader response)
        {
            span.Attributes.Add(new KeyValuePair<string, object>("error.type", response["error.type"]));
            span.Attributes.Add(new KeyValuePair<string, object>("server.address", response["server.address"]));
            span.Attributes.Add(new KeyValuePair<string, object>("network.peer.port", response["net.peer.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("server.port", response["server.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("messaging.destination.name", response["messaging.destination.name"]));
            span.Attributes.Add(new KeyValuePair<string, object>("messaging.operation.type", response["messaging.operation"]));
            span.Attributes.Add(new KeyValuePair<string, object>("messaging.system", response["messagingSystem"]));
        }
        internal static List<Span> ConvertSpanDataToSpan(List<SpanData> spanDataList)
        {
            var spans = new List<Span>();

            foreach (var spanData in spanDataList)
            {
                var traceIdBytes = new byte[16];
                var spanIdBytes = new byte[8];

                spanData.TraceId.AsSpan().CopyTo(traceIdBytes);
                spanData.SpanId.AsSpan().CopyTo(spanIdBytes);


                var parentSpanIdString = ByteString.Empty;
                if (spanData.ParentSpanId != null)
                {
                    var parentSpanIdBytes = new byte[8];
                    spanData.ParentSpanId.AsSpan().CopyTo(parentSpanIdBytes);
                    parentSpanIdString = UnsafeByteOperations.UnsafeWrap(parentSpanIdBytes);
                }

                var otlpSpan = new Span
                {
                    Name = spanData.Name,
                    Kind = (Span.Types.SpanKind)(spanData.Kind + 1),
                    TraceId = UnsafeByteOperations.UnsafeWrap(traceIdBytes),
                    SpanId = UnsafeByteOperations.UnsafeWrap(spanIdBytes),
                    ParentSpanId = parentSpanIdString,
                    StartTimeUnixNano = (ulong)(spanData.StartTime.ToUnixTimeMilliseconds() * 1_000_000),
                    EndTimeUnixNano = (ulong)(spanData.EndTime.ToUnixTimeMilliseconds() * 1_000_000),
                    //StartTimeUnixNano = (ulong)spanData.StartTime.ToUnixTimeNanoseconds(),
                    //EndTimeUnixNano = (ulong)spanData.EndTime.ToUnixTimeNanoseconds(),
                };

                foreach (var attribute in spanData.Attributes)
                {
                    otlpSpan.Attributes.Add(new KeyValue
                    {
                        Key = attribute.Key,
                        Value = new AnyValue { StringValue = attribute.Value.ToString() }
                    });
                }

                otlpSpan.Status = new Status
                {
                    Code = (Status.Types.StatusCode)spanData.Status.StatusCode,
                    Message = spanData.Status.Description ?? string.Empty
                };

                spans.Add(otlpSpan);
            }

            return spans;
        }
    }

}


