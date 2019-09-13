namespace OrchestratorStatefulSvc.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [Route("api/Orchestrator")]
    public class OrchestratorStatefulSvcController : Controller
    {
        private readonly IReliableStateManager stateManager_;
        private readonly StatefulServiceContext context_;

        //Move to config?
        private const int TASK_SIZE = 1024;

        public OrchestratorStatefulSvcController(IReliableStateManager stateManager, StatefulServiceContext context)
        {
            this.stateManager_ = stateManager;
            this.context_ = context;
        }

        // POST api/Orchestrator/jobs
        [HttpPost("jobs")]
        public async Task<IActionResult> PostNewJob([FromBody] string sourceText)
        {

            ServiceEventSource.Current.ServiceMessage(this.context_, "Orchestrator param: " + sourceText);
            CancellationToken ct = new CancellationToken();

            var jobsDictionary = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, JobObject>>("jobsDictionary");
            var tasksQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<MapTask>>("tasksQueue");

            JobObject job = new JobObject(sourceText, TASK_SIZE);

            using (ITransaction tx = this.stateManager_.CreateTransaction())
            {
                try
                {
                    await jobsDictionary.AddAsync(tx, job.Uuid, job, TimeSpan.FromSeconds(1), ct);
                }
                catch(Exception e)
                {
                    ServiceEventSource.Current.ServiceMessage(this.context_, e.Message);
                }
                foreach(var task in job.Tasks)
                {
                    await tasksQueue.EnqueueAsync(tx, task, ct);
                }

                await tx.CommitAsync();
                return new OkResult();
            }
        }

        // GET api/Orchestrator/jobs
        [HttpGet("jobs")]
        public async Task<IActionResult> GetTaskFromQueue()
        {
            ServiceEventSource.Current.ServiceMessage(this.context_, "GET api/Orchestrator/jobs called");
            var tasksQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<MapTask>>("tasksQueue");
            using (var tx = this.stateManager_.CreateTransaction())
            {
                var conditionalValue = await tasksQueue.TryDequeueAsync(tx);
                await tx.CommitAsync();

                if(conditionalValue.HasValue)
                {
                    return Json(conditionalValue.Value);
                }
                else
                {
                    return new NotFoundResult();
                }
            }
        }

        // PUT api/Orchestrator/jobs/{jobUuid}
        [HttpPut("jobs/{uuid}")]
        public async Task<IActionResult> Put(Guid uuid, [FromBody] MapTask task)
        {
            //ServiceEventSource.Current.ServiceMessage(this.context_, $"PUT api/Orchestrator/jobs/{uuid} called, task: " + Newtonsoft.Json.JsonConvert.SerializeObject(task));
            var jobsDictionary = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, JobObject>>("jobsDictionary");
            var completeJobsQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<string>>("completeJobsQueue");
            bool isJobCompleted = false;
            JobObject job = new JobObject();

            using (ITransaction tx = this.stateManager_.CreateTransaction())
            {
                var conditionalValue = await jobsDictionary.TryGetValueAsync(tx, uuid);
                if(conditionalValue.HasValue)
                {
                    job = conditionalValue.Value;
                    job.SetTaskCompleted(task);
                    isJobCompleted = job.IsComplete;

                    if(job.IsComplete)
                    {
                        ServiceEventSource.Current.ServiceMessage(this.context_, $"Job completed");

                        // Job is complete, remove it, send the result to FrontEndSvc for now
                        var removeConditional = await jobsDictionary.TryRemoveAsync(tx, job.Uuid);
                        if(!removeConditional.HasValue)
                        {
                            // Retry, I guess?
                        }


                        List<Dictionary<string, int>> counts = job.Tasks.Select(t => t.Output).ToList();
                        string serialized = JsonConvert.SerializeObject(counts);
                        await completeJobsQueue.EnqueueAsync(tx, serialized);

                    }
                    else
                    {
                        ServiceEventSource.Current.ServiceMessage(this.context_, $"Job not yet completed, only a task");
                        await jobsDictionary.SetAsync(tx, job.Uuid, job);
                    }
                    
                }

                await tx.CommitAsync();
            }
            return new OkResult();
        }

        [HttpGet("completeJobs")]
        public async Task<IActionResult> GetReduceJob()
        {
            var completeJobsQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<string>>("completeJobsQueue");
            using (var tx = this.stateManager_.CreateTransaction())
            {
                var conditionalValue = await completeJobsQueue.TryDequeueAsync(tx);
                await tx.CommitAsync();

                if (conditionalValue.HasValue)
                {
                    return new ContentResult()
                    {
                        StatusCode = new OkResult().StatusCode,
                        Content = conditionalValue.Value
                    };
                }
                else
                {
                    return new NotFoundResult();
                }
            }
        }

        [HttpPut("completeJobs")]
        public async Task<IActionResult> PutReduceJob([FromBody] JToken wordCloudData)
        {
            using (HttpClient httpClient = new HttpClient())
            {
                string proxyUrl = $"http://localhost:19081/WordCount/FrontEndSvc/api/WordCount/complete";
                // Contact FrontEndSvc and send result
                using (HttpResponseMessage returnResponse = await httpClient.PostAsync(proxyUrl, new StringContent(wordCloudData.ToString(), UnicodeEncoding.UTF8, "application/json")))
                {
                    if (returnResponse.StatusCode != HttpStatusCode.OK)
                    {
                        // Error
                        ServiceEventSource.Current.ServiceMessage(this.context_, "Job completion failed");
                    }

                    return new OkResult();

                }
            }
        }

    }
}