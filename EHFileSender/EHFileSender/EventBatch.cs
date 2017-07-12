using Microsoft.Azure.EventHubs;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EHFileSender
{
    public class EventBatch
    {
        private readonly EventHubClient _client;
        private readonly bool _simulateSending;
        private bool _busy;


        public EventBatch(EventHubClient client, bool simulateSending)
        {
            _client = client;
            _busy = false;
            _simulateSending = simulateSending;
        }

        public bool Busy
        {
            get
            {
                return _busy;
            }
        }

        public async Task<int> SendPacket(EventData[] eventData)
        {
            _busy = true;
            if (_simulateSending)
            {
                await Task.Delay(10000);
            }
            else
            {
                await _client.SendAsync(eventData).ContinueWith((t) => _busy = false);
            }
            return eventData.Length;
        }
    }
}
