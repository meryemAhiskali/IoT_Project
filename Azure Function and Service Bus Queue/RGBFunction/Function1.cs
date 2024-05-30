using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace FunctionApp6
{
    public static class Function1
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        private static bool? previousLedStatus = null;
        private static DateTime lastUpdate = DateTime.MinValue;
        private static readonly TimeSpan debounceTime = TimeSpan.FromSeconds(1); // Debounce duration (1 second)

        public class TelemetryMessage
        {
            public string applicationId { get; set; }
            public string deviceId { get; set; }
            public DateTime enqueuedTime { get; set; }
            public dynamic enrichments { get; set; }
            public MessageProperties messageProperties { get; set; }
            public string messageSource { get; set; }
            public string schema { get; set; }
            public Telemetry telemetry { get; set; }
            public string templateId { get; set; }
        }

        public class MessageProperties
        {
            public string iothub_creation_time_utc { get; set; }
        }

        public class Telemetry
        {
            public int led_status { get; set; }
        }

        public class Device
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Type { get; set; }
            public bool Status { get; set; }
            public int RoomId { get; set; }
        }

        public class ApiResponse
        {
            public int PageNumber { get; set; }
            public int PageSize { get; set; }
            public bool Succeeded { get; set; }
            public string Message { get; set; }
            public string[] Errors { get; set; }
            public Device[] Data { get; set; }
        }

        [FunctionName("Function1")]
        public static async Task Run([ServiceBusTrigger("iotqueue", Connection = "Connection")] string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

            // Log the raw message
            log.LogInformation($"Raw message: {myQueueItem}");

            // Deserialize the incoming message with error handling
            TelemetryMessage telemetryMessage = null;
            try
            {
                telemetryMessage = JsonConvert.DeserializeObject<TelemetryMessage>(myQueueItem);
                log.LogInformation($"Deserialized message: {JsonConvert.SerializeObject(telemetryMessage)}");
            }
            catch (JsonException jsonEx)
            {
                log.LogError($"JSON deserialization error: {jsonEx.Message}");
                log.LogError($"Stack Trace: {jsonEx.StackTrace}");

                // Log the problematic part of the JSON
                try
                {
                    var jObject = JObject.Parse(myQueueItem);
                    var problematicProperty = jObject.SelectToken("telemetry.doorPosition");
                    log.LogError($"Problematic property: {problematicProperty?.ToString() ?? "null"}");
                }
                catch (Exception ex)
                {
                    log.LogError($"Error while parsing JSON for problematic property: {ex.Message}");
                }

                return;
            }

            // Check if telemetry is null
            if (telemetryMessage?.telemetry == null)
            {
                log.LogError("Telemetry data is null. Message is invalid.");
                return;
            }

            // Determine current door status based on doorPosition value
            bool currentLedStatus = telemetryMessage.telemetry.led_status == 1;

            // Update previous door status and check debounce
            await semaphore.WaitAsync();
            try
            {
                if (previousLedStatus.HasValue && previousLedStatus.Value == currentLedStatus &&
                    (DateTime.UtcNow - lastUpdate) < debounceTime)
                {
                    log.LogInformation("Door status has not changed or debounce time not passed. No update necessary.");
                    return;
                }

                previousLedStatus = currentLedStatus;
                lastUpdate = DateTime.UtcNow;
            }
            finally
            {
                semaphore.Release();
            }

            // Fetch the devices
            var getDevicesUrl = "https://softwarebackenddeployment2.azurewebsites.net/api/v1/Device/GetAllDevices?PageNumber=1&PageSize=10";
            log.LogInformation($"Fetching devices from URL: {getDevicesUrl}");
            var getDevicesResponse = await httpClient.GetAsync(getDevicesUrl);

            if (!getDevicesResponse.IsSuccessStatusCode)
            {
                log.LogError($"Failed to fetch devices. Status Code: {getDevicesResponse.StatusCode}");
                return;
            }

            var devicesContent = await getDevicesResponse.Content.ReadAsStringAsync();
            var apiResponse = JsonConvert.DeserializeObject<ApiResponse>(devicesContent);

            if (apiResponse?.Data == null || apiResponse.Data.Length == 0)
            {
                log.LogError("No devices found.");
                return;
            }

            // Find the last device with type "Door"
            var latestLamp = apiResponse.Data
                .Where(d => d.Type == "Lamp")
                .OrderBy(d => d.Id)
                .LastOrDefault();

            if (latestLamp == null)
            {
                log.LogError("Failed to find the latest lamp device.");
                return;
            }

            log.LogInformation($"{latestLamp}");
            // Prepare the API request


            var updateDeviceCommand = new
            {
                Id = latestLamp.Id,
                Name = latestLamp.Name,
                Status = currentLedStatus
            };

            var jsonContent = JsonConvert.SerializeObject(updateDeviceCommand);
            log.LogInformation($"Update command JSON: {jsonContent}");
            var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            // Call the update API
            var updateUrl = $"https://softwarebackenddeployment2.azurewebsites.net/api/v1/Device/UpdateDevice?id={latestLamp.Id}";
            log.LogInformation($"Sending PUT request to URL: {updateUrl}");
            var response = await httpClient.PutAsync(updateUrl, content);

            var responseContent = await response.Content.ReadAsStringAsync();
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Failed to update data. Status Code: {response.StatusCode}, Response: {responseContent}");
                response.EnsureSuccessStatusCode();
            }

            log.LogInformation("Data updated successfully.");
        }
    }
}
