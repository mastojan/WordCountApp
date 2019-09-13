using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Classifier.Web.Hubs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json.Linq;

namespace FrontEndSvc.Controllers
{
    [Produces("application/json")]
    [Route("api/WordCount")]
    public class FrontEndSvcController : Controller
    {
        private readonly HttpClient httpClient;
        private readonly StatelessServiceContext context;
        private readonly FabricClient fabricClient;
        private readonly IHubContext<ResultHub> hubContext;

        public FrontEndSvcController(HttpClient httpClient, StatelessServiceContext context, FabricClient fabricClient, IHubContext<ResultHub> hubContext)
        {
            this.httpClient = httpClient;
            this.context = context;
            this.fabricClient = fabricClient;
            this.hubContext = hubContext;
        }

        // Post: api/WordCount
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] string sourceText)
        {
            ServiceEventSource.Current.ServiceMessage(this.context, "Received param: " + sourceText);

            Uri serviceName = FrontEndSvc.GetOrchestratorStatefulSvcName(this.context);
            Uri proxyAddress = this.GetProxyAddress(serviceName);
            int partitionKey = 0;

            string proxyUrl = $"{proxyAddress}/api/Orchestrator/jobs?PartitionKey={partitionKey}&PartitionKind=Int64Range";

            StringContent postContent = new StringContent('"' + sourceText + '"', Encoding.UTF8, "application/json");
            postContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using (HttpResponseMessage response = await this.httpClient.PostAsync(proxyUrl, postContent))
            {
                return new ContentResult()
                {
                    StatusCode = (int)response.StatusCode,
                    Content = await response.Content.ReadAsStringAsync()
                };
            }
            
        }

        [HttpPost("complete")]
        public async Task<IActionResult> PostCompletedJob([FromBody] JToken wordCloudData)
        {
            await this.hubContext.Clients.All.SendAsync("jobComplete", wordCloudData);
            return new OkResult();
        }

        private Uri GetProxyAddress(Uri serviceName)
        {
            return new Uri($"http://localhost:19081{serviceName.AbsolutePath}");
        }
    }
}