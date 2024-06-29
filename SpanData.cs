using System.Data;

namespace SpanRetriever1
{
    public class SpanData
    {
        // Identifiers
        public byte[] TraceId { get; set; } = new byte[16];
        public byte[] SpanId { get; set; } = new byte[8];
        public byte[]? ParentSpanId { get; set; }

        // Span Metadata
        public string Name { get; set; } = string.Empty;
        public int Kind { get; set; } // Use appropriate enum or mapping for SpanKind
      
        // Timing Information
        public DateTimeOffset StartTime { get; set; }
        public DateTimeOffset EndTime { get; set; } // End time of the span
        public TimeSpan Duration => EndTime - StartTime; // Duration is calculated

        // Instrumentation Library
        public string LibraryName { get; set; } = string.Empty;
        public string? LibraryVersion { get; set; }

        // Attributes
        public List<KeyValuePair<string, object>> Attributes { get; set; } = new();

        // Status
        public SpanStatus Status { get; set; } = new SpanStatus();

    }

    public class SpanStatus
    {
        public int StatusCode { get; set; } // Use appropriate enum or mapping for StatusCode
        public string? Description { get; set; }
    }


}