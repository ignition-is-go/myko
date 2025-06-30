using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using System.Threading.Tasks;
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
        _logger?.LogInformation("🔗 Connecting to WebSocket at {Url}", url);
        
        SetConnectionStatus(ConnectionStatus.Connecting);

        _client?.Dispose();
        _client = new WebsocketClient(new Uri(url))
        {
            // Disable automatic reconnection timeouts that might cause disconnects
            ReconnectTimeout = null,
            
            // Only reconnect on actual errors, with a short delay
            ErrorReconnectTimeout = TimeSpan.FromSeconds(5),
            
            // Enable reconnection for connection issues
            IsReconnectionEnabled = true
        };

        _client.MessageReceived.Subscribe(OnMessageReceived);
        _client.ReconnectionHappened.Subscribe(OnReconnectionHappened);
        _client.DisconnectionHappened.Subscribe(OnDisconnectionHappened);

        _logger?.LogDebug("WebSocket client configured - ReconnectTimeout: null, ErrorReconnectTimeout: 5s, IsReconnectionEnabled: true");

        await _client.Start();
        _logger?.LogInformation("WebSocket client started");
        
        // Start processing events
        _ = Task.Run(ProcessEventsAsync, _cancellationTokenSource.Token);
        _logger?.LogDebug("Event processing task started");
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
        _logger?.LogInformation("WebSocket reconnection happened - Type: {Type}", info.Type);
        _logger?.LogDebug("Reconnection details - Type: {Type}", info.Type);
        
        SetConnectionStatus(ConnectionStatus.Connected);
    }

    private void OnDisconnectionHappened(DisconnectionInfo info)
    {
        _logger?.LogWarning("WebSocket disconnection happened - Type: {Type}, CloseStatus: {CloseStatus}, CloseStatusDescription: {Description}", 
            info.Type, info.CloseStatus, info.CloseStatusDescription);
        _logger?.LogDebug("Disconnection details - Type: {Type}, CloseStatus: {CloseStatus}, CloseStatusDescription: {Description}", 
            info.Type, info.CloseStatus, info.CloseStatusDescription);
        
        SetConnectionStatus(ConnectionStatus.Disconnected);
    }

    private void SetConnectionStatus(ConnectionStatus status)
    {
        if (_connectionStatus != status)
        {
            var previousStatus = _connectionStatus;
            _connectionStatus = status;
            
            _logger?.LogInformation("Connection status changed: {PreviousStatus} -> {NewStatus}", 
                previousStatus, status);
            
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
        _logger?.LogInformation("Disposing MykoClient");
        
        _cancellationTokenSource.Cancel();
        _client?.Dispose();
        _eventWriter.Complete();
        _cancellationTokenSource.Dispose();
        
        _logger?.LogDebug("MykoClient disposed");
    }
}
