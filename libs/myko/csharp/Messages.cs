using System.Text.Json.Serialization;
using MykoSdk.Events;

namespace MykoSdk.Messages;

[JsonConverter(typeof(MykoMessageConverter))]
public abstract class MykoMessage
{
    public abstract string Event { get; }
    public abstract object Data { get; }
}

public class MykoEventMessage : MykoMessage
{
    public override string Event => "ws:m:event";
    public override object Data { get; }

    public MykoEventMessage(object eventData)
    {
        Data = eventData;
    }
}

// Custom JSON converter to match the Rust format: {"event": "ws:m:event", "data": {...}}
public class MykoMessageConverter : JsonConverter<MykoMessage>
{
    public override MykoMessage Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException("Reading MykoMessage is not implemented yet");
    }

    public override void Write(Utf8JsonWriter writer, MykoMessage value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("event", value.Event);
        writer.WritePropertyName("data");
        JsonSerializer.Serialize(writer, value.Data, options);
        writer.WriteEndObject();
    }
}
