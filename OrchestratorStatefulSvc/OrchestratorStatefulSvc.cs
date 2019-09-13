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
using Microsoft.ServiceFabric.Data.Collections;

namespace OrchestratorStatefulSvc
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class OrchestratorStatefulSvc : StatefulService
    {
        public OrchestratorStatefulSvc(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[]
            {
                new ServiceReplicaListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                                    .UseKestrel()
                                    .ConfigureServices(
                                        services => services
                                            .AddSingleton<StatefulServiceContext>(serviceContext)
                                            .AddSingleton<IReliableStateManager>(this.StateManager))
                                    .UseContentRoot(Directory.GetCurrentDirectory())
                                    .UseStartup<Startup>()
                                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseUniqueServiceUrl)
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
                    var jobsDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<Guid, JobObject>>("jobsDictionary");
                    var tasksQueue = await this.StateManager.GetOrAddAsync<IReliableConcurrentQueue<MapTask>>("tasksQueue");
                    var allTasks = await this.StateManager.GetOrAddAsync<IReliableDictionary<Guid, MapTask>>("allTasks");

                    using (ITransaction tx = this.StateManager.CreateTransaction())
                    {
                        var enumerator = (await allTasks.CreateEnumerableAsync(tx)).GetAsyncEnumerator();
                        while(await enumerator.MoveNextAsync(cancellationToken))
                        {
                            if(enumerator.Current.Value.Output == null)
                            {
                                await tasksQueue.EnqueueAsync(tx, enumerator.Current.Value);
                            }
                        }
                        await tx.CommitAsync();
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(3000), cancellationToken);

                }
                catch(TaskCanceledException e)
                {
                    ServiceEventSource.Current.ServiceMessage(this.Context, "OrchestratorStatefulSvc cancelled: " + e.StackTrace);
                    break;
                }
            }
        }
    }

}
