using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using System.Net.Http;
using System.Net;
using Newtonsoft.Json;
using System.Text;

namespace ReducerStatelessSvc
{

    internal class KVPair
    {
    }

    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class ReducerStatelessSvc : StatelessService
    {
        public ReducerStatelessSvc(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[]
            {
                new ServiceInstanceListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, "ServiceEndpoint", (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                                    .UseKestrel()
                                    .ConfigureServices(
                                        services => services
                                            .AddSingleton<StatelessServiceContext>(serviceContext))
                                    .UseContentRoot(Directory.GetCurrentDirectory())
                                    .UseStartup<Startup>()
                                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.None)
                                    .UseUrls(url)
                                    .Build();
                    }))
            };
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using (HttpClient httpClient = new HttpClient())
                    {
                        string proxyUrl = $"http://localhost:19081/WordCount/OrchestratorStatefulSvc/api/Orchestrator/completeJobs";

                        using (HttpResponseMessage httpResponse = await httpClient.GetAsync($"{proxyUrl}?PartitionKey=0&PartitionKind=Int64Range"))
                        {
                            if (httpResponse.StatusCode != HttpStatusCode.OK)
                            {
                                continue;
                            }

                            var responseContent = await httpResponse.Content.ReadAsStringAsync();
                            List<Dictionary<string, int>> mappedItems = JsonConvert.DeserializeObject<List<Dictionary<string, int>>>(responseContent);

                            Dictionary<string, int> completeCount = new Dictionary<string, int>();

                            foreach (var item in mappedItems)
                            {
                                foreach (var kvPair in item)
                                {
                                    if (completeCount.ContainsKey(kvPair.Key))
                                    {
                                        completeCount[kvPair.Key] += kvPair.Value;
                                    }
                                    else
                                    {
                                        completeCount[kvPair.Key] = kvPair.Value;
                                    }
                                }
                            }

                            var serializedOutput = JsonConvert.SerializeObject(completeCount);
                            using (HttpResponseMessage putTaskResponse = await httpClient.PutAsync($"{proxyUrl}?PartitionKey=0&PartitionKind=Int64Range", new StringContent(serializedOutput, UnicodeEncoding.UTF8, "application/json")))
                            {
                                if (putTaskResponse.StatusCode != HttpStatusCode.OK)
                                {
                                    // Error
                                    ServiceEventSource.Current.ServiceMessage(this.Context, "Task completion failed");
                                }

                            }

                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

                    }
                }
                catch(TaskCanceledException e)
                {
                    ServiceEventSource.Current.ServiceMessage(this.Context, "ReducerStatelessSvc.RunAsync cancelled: " + e.StackTrace);
                }
            }
        }
    }
}
