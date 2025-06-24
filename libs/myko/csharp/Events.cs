using System.Text.Json.Serialization;

namespace MykoSdk.Events;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EventType
{
    SET,
    DEL
}

public class Event<T>
{
    [JsonPropertyName("tx")]
    public string TransactionId { get; set; } = string.Empty;

    [JsonPropertyName("itemType")]
    public string ItemType { get; set; } = string.Empty;

    [JsonPropertyName("createdAt")]
    public string CreatedAt { get; set; } = string.Empty;

    [JsonPropertyName("sourceId")]
    public string? SourceId { get; set; }

    [JsonPropertyName("item")]
    public T Item { get; set; } = default!;

    [JsonPropertyName("changeType")]
    public EventType ChangeType { get; set; }

    public static Event<T> FromItem(T item, EventType changeType, string transactionId)
    {
        return new Event<T>
        {
            Item = item,
            ChangeType = changeType,
            TransactionId = transactionId,
            ItemType = typeof(T).Name,
            CreatedAt = DateTime.UtcNow.ToString("O")
        };
    }
}
