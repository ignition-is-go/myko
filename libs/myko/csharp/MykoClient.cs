using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Websocket.Client;
using MykoSdk.Events;
using MykoSdk.Messages;

namespace MykoSdk.Client;

public enum ConnectionStatus
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Error
}

public class MykoClient : IDisposable
{
    private readonly ILogger<MykoClient>? _logger;
    private WebsocketClient? _client;
    private readonly Channel<object> _eventChannel;
    private readonly ChannelWriter<object> _eventWriter;
    private readonly ChannelReader<object> _eventReader;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<object?>> _pendingRequests;
    private ConnectionStatus _connectionStatus = ConnectionStatus.Disconnected;

    public event EventHandler<ConnectionStatus>? ConnectionStatusChanged;

    // Event for incoming messages that other components can subscribe to
    public event EventHandler<string>? MessageReceived;

    public MykoClient(ILogger<MykoClient>? logger = null)
    {
        _logger = logger;
        _eventChannel = Channel.CreateUnbounded<object>();
        _eventWriter = _eventChannel.Writer;
        _eventReader = _eventChannel.Reader;
        _cancellationTokenSource = new CancellationTokenSource();
        _pendingRequests = new ConcurrentDictionary<string, TaskCompletionSource<object?>>();
    }

    public async Task ConnectAsync(string url)
    {
        _logger?.LogInformation("Connecting to {Url}", url);
        
        SetConnectionStatus(ConnectionStatus.Connecting);

        _client?.Dispose();
        _client = new WebsocketClient(new Uri(url))
        {
            // The ReconnectTimeout might have been causing periodic disconnections
            // Remove it to prevent automatic reconnection attempts based on timeout
            // Only reconnect when there's an actual connection error
            ErrorReconnectTimeout = TimeSpan.FromSeconds(5)
        };

        _client.MessageReceived.Subscribe(OnMessageReceived);
        _client.ReconnectionHappened.Subscribe(OnReconnectionHappened);
        _client.DisconnectionHappened.Subscribe(OnDisconnectionHappened);

        await _client.Start();
        
        // Start processing events
        _ = Task.Run(ProcessEventsAsync, _cancellationTokenSource.Token);
    }

    private void OnMessageReceived(ResponseMessage message)
    {
        if (string.IsNullOrEmpty(message.Text))
            return;
            
        try
        {
            var jsonDocument = JsonDocument.Parse(message.Text);
            // Notify subscribers of the raw message
            MessageReceived?.Invoke(this, message.Text);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error parsing message: {Message}", message.Text);
        }
    }

    private void OnReconnectionHappened(ReconnectionInfo info)
    {
        _logger?.LogInformation("Reconnection happened: {Type}", info.Type);
        SetConnectionStatus(ConnectionStatus.Connected);
    }

    private void OnDisconnectionHappened(DisconnectionInfo info)
    {
        _logger?.LogWarning("Disconnection happened: {Type}, CloseStatus: {CloseStatus}", 
            info.Type, info.CloseStatus);
        SetConnectionStatus(ConnectionStatus.Disconnected);
    }

    private void SetConnectionStatus(ConnectionStatus status)
    {
        if (_connectionStatus != status)
        {
            _connectionStatus = status;
            ConnectionStatusChanged?.Invoke(this, status);
        }
    }

    public ConnectionStatus GetConnectionStatus() => _connectionStatus;

    public async Task SendEventAsync<T>(Event<T> eventData)
    {
        if (_client == null || _connectionStatus != ConnectionStatus.Connected)
        {
            throw new InvalidOperationException("Client is not connected");
        }

        // Wrap the event in a MykoMessage envelope like the Rust SDK does
        var mykoMessage = new MykoEventMessage(eventData);

        var json = JsonSerializer.Serialize(mykoMessage, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        _client.Send(json);
        await Task.CompletedTask;
    }

    public async Task AwaitConnectionAsync()
    {
        while (_connectionStatus != ConnectionStatus.Connected)
        {
            await Task.Delay(100);
        }
    }

    private async Task ProcessEventsAsync()
    {
        await foreach (var eventData in _eventReader.ReadAllAsync(_cancellationTokenSource.Token))
        {
            try
            {
                // Process events from the channel
                _logger?.LogDebug("Processing event: {Event}", eventData);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error processing event");
            }
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _client?.Dispose();
        _eventWriter.Complete();
        _cancellationTokenSource.Dispose();
    }
}
