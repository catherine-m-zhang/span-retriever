using System.Data;
using System;
using System.Collections.Generic;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using SpanRetriever1;
using OpenTelemetry.Trace;
using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Trace.V1;

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
                var span = new SpanData
                {
                    // Initialize identifiers
                    TraceId = Convert.FromBase64String(response["env_dt_traceId"].ToString()),
                    SpanId = Convert.FromBase64String(response["env_dt_spanId"].ToString()),
                    ParentSpanId = response.IsDBNull(response.GetOrdinal("parentId")) ? null : Convert.FromBase64String(response.GetString(response.GetOrdinal("parentId"))),
                    Name = response["name"].ToString(),
                    Kind = Convert.ToInt32(response["kind"]),
                    TraceState = response["traceState"].ToString(),
                    StartTime = DateTimeOffset.Parse(response["startTime"].ToString()),
                    EndTime = DateTimeOffset.Parse(response["endTime"].ToString()),
                    LibraryName = response["instrumentation_scope.name"].ToString(),
                    LibraryVersion = response["telemetry.sdk.version"].ToString(),
                    Status = new SpanStatus
                    {
                        StatusCode = Convert.ToInt32(response["httpStatusCode"]),
                        Description = response["statusMessage"].ToString()
                    },
                    Flags = Convert.ToUInt32(response["flags"]),
                    Resource = new Resource()
                };

                // Add resource attributes
                AddResourceAttributes(span.Resource, response);

                // Add HTTP span attributes
                AddSpanAttributes(span, response);

                spanDataList.Add(span);
            }

            return spanDataList;
        }

        private static void AddResourceAttributes(Resource resource, IDataReader response)
        {
            resource.Attributes = new List<KeyValuePair<string, object>>
            {
                new KeyValuePair<string, object>("cloud.region", response["cloud.region"]),
                new KeyValuePair<string, object>("deployment.environment", response["deployment.environment"]),
                new KeyValuePair<string, object>("service.name", response["service.name"]),
                new KeyValuePair<string, object>("service.namespace", response["service.namespace"]),
                new KeyValuePair<string, object>("service.instance.id", response["service.instance.id"]),
                new KeyValuePair<string, object>("telemetry.sdk.version", response["telemetry.sdk.version"]),
                new KeyValuePair<string, object>("host.id", response["net.host.name"]),
                new KeyValuePair<string, object>("host.name", response["net.host.name"]),
            };
        }

        private static void AddSpanAttributes(SpanData span, IDataReader response)
        {
            // Common HTTP attributes
            span.Attributes.Add(new KeyValuePair<string, object>("http.method", response["httpMethod"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.url", response["httpUrl"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.target", response["http.target"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.host", response["http.host"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.scheme", response["http.scheme"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.flavor", response["http.flavor"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.user_agent", response["http.user_agent"]));

            // HTTP request and response attributes
            span.Attributes.Add(new KeyValuePair<string, object>("http.request_content_length", response["http.request_content_length"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.response_content_length", response["http.response_content_length"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.status_code", response["httpStatusCode"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.server_name", response["http.server_name"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.route", response["http.route"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.client_ip", response["http.client_ip"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.header.apim_request_id", response["http.request.header.apim_request_id"]));
            span.Attributes.Add(new KeyValuePair<string, object>("http.request.header.x_request_id", response["http.request.header.x_request_id"]));

            // Network attributes
            span.Attributes.Add(new KeyValuePair<string, object>("net.peer.ip", response["net.peer.ip"]));
            span.Attributes.Add(new KeyValuePair<string, object>("net.peer.port", response["net.peer.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("net.host.port", response["net.host.port"]));
            span.Attributes.Add(new KeyValuePair<string, object>("net.transport", response["net.transport"]));
        }

        public static List<Span> ConvertSpanDataToSpan(List<SpanData> spanDataList)
        {
            var spans = new List<Span>();

            foreach (var spanData in spanDataList)
            {
                var traceIdBytes = new byte[16];
                var spanIdBytes = new byte[8];

                spanData.TraceId.CopyTo(traceIdBytes, 0);
                spanData.SpanId.CopyTo(spanIdBytes, 0);

                var parentSpanIdString = ByteString.Empty;
                if (spanData.ParentSpanId != null)
                {
                    var parentSpanIdBytes = new byte[8];
                    spanData.ParentSpanId.CopyTo(parentSpanIdBytes, 0);
                    parentSpanIdString = UnsafeByteOperations.UnsafeWrap(parentSpanIdBytes);
                }

                var otlpSpan = new Span
                {
                    Name = spanData.Name,
                    Kind = (Span.Types.SpanKind)(spanData.Kind + 1),
                    TraceId = UnsafeByteOperations.UnsafeWrap(traceIdBytes),
                    SpanId = UnsafeByteOperations.UnsafeWrap(spanIdBytes),
                    ParentSpanId = parentSpanIdString,
                    TraceState = spanData.TraceState ?? string.Empty,
                    StartTimeUnixNano = (ulong)spanData.StartTime.ToUnixTimeNanoseconds(),
                    EndTimeUnixNano = (ulong)spanData.EndTime.ToUnixTimeNanoseconds(),
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

                otlpSpan.Flags = spanData.Flags;

                spans.Add(otlpSpan);
            }

            return spans;
        }
    }

}


