using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace DurableFunctionsDebug
{
    public static class Durables
    {
        [FunctionName("DurableTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient durableClient,
            ILogger log)
        {
            // This works:
            // http://localhost:7071/api/DurableTrigger?payloadSize=32766 
            // note: since JSON serialization adds quotes, the max character count can be 32768 - 2 = 32766.

            // This however does not:
            // http://localhost:7071/api/DurableTrigger?payloadSize=33000
            // note: this goes over the max string length limit in Table Storage (strings are always UTF-16 in tables, so max 64kb == 32k characters)

            // The magic error barrier sits at 45150, and above that things start to work again:
            // http://localhost:7071/api/DurableTrigger?payloadSize=45149

            // Summarized:
            // When serialized payload size is <= 32768, everything works.
            // When serialized payload size is between 32769 .. 45150 the IDurableClient.StartNewAsync throws.
            // When serialized payload size is >= 45151, it works again.
            // HOWEVER: In all above cases the OrchestratorFunc still gets called, and receives the proper payload.

            log.LogInformation("Starting orchestration");

            var dataLength = int.Parse(req.Query["payloadSize"]);
            var dataObject = string.Concat(Enumerable.Range(0, dataLength).Select(x => "x"));

            // Wrapping it to a try-catch to print out the actual inner error.
            try
            {
                await durableClient.StartNewAsync(nameof(OrchestratorFunc), Guid.NewGuid().ToString(), dataObject);
            }
            catch (Microsoft.WindowsAzure.Storage.StorageException e)
            {
                log.LogError("The exception extended error message was: " + e.RequestInformation.ExtendedErrorInformation.ErrorMessage);
                throw;
            }
            

            return new OkResult();
        }

        [FunctionName(nameof(OrchestratorFunc))]
        public static async Task OrchestratorFunc([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var data = context.GetInput<string>();
            log.LogInformation($"Data received, string length {data.Length}");
        }
    }
}
