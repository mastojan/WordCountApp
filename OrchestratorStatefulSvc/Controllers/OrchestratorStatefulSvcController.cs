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
            var allTasks = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, MapTask>>("allTasks");

            using (ITransaction tx = this.stateManager_.CreateTransaction())
            {

                Guid jobGuid = System.Guid.NewGuid();
                JobObject job = new JobObject(jobGuid);
                MapTask task;

                int cursorPos = 0;
                while (cursorPos + TASK_SIZE < sourceText.Length)
                {
                    int nextCursorPos = cursorPos + TASK_SIZE;
                    while (sourceText[nextCursorPos] != ' ')
                    {
                        nextCursorPos--;
                    }

                    task = new MapTask(jobGuid, sourceText.Substring(cursorPos, nextCursorPos - cursorPos));
                    task.State = MapTask.StateType.InQueue;
                    job.Tasks.Add(task.Uuid, false);

                    await allTasks.AddAsync(tx, task.Uuid, task);
                    await tasksQueue.EnqueueAsync(tx, task, ct);

                    cursorPos = nextCursorPos + 1;
                }
                task = new MapTask(jobGuid, sourceText.Substring(cursorPos));
                task.State = MapTask.StateType.InQueue;
                job.Tasks.Add(task.Uuid, false);

                await allTasks.AddAsync(tx, task.Uuid, task);
                await tasksQueue.EnqueueAsync(tx, task, ct);

                try
                {
                    await jobsDictionary.AddAsync(tx, job.Uuid, job, TimeSpan.FromSeconds(1), ct);
                }
                catch(Exception e)
                {
                    ServiceEventSource.Current.ServiceMessage(this.context_, e.Message);
                }
                

                await tx.CommitAsync();
                return new OkResult();
            }
        }

        // GET api/Orchestrator/jobs
        [HttpGet("jobs")]
        public async Task<IActionResult> GetTaskFromQueue()
        {
            var allTasks = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, MapTask>>("allTasks");
            var tasksQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<MapTask>>("tasksQueue");
            using (var tx = this.stateManager_.CreateTransaction())
            {
                var conditionalValue = await tasksQueue.TryDequeueAsync(tx);

                if(conditionalValue.HasValue)
                {
                    MapTask task = conditionalValue.Value;
                    task.State = MapTask.StateType.Processing;
                    await allTasks.SetAsync(tx, task.Uuid, task);
                    await tx.CommitAsync();
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
            var jobsDictionary = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, JobObject>>("jobsDictionary");
            var completeJobsQueue = await this.stateManager_.GetOrAddAsync<IReliableConcurrentQueue<string>>("completeJobsQueue");
            var allTasks = await this.stateManager_.GetOrAddAsync<IReliableDictionary<Guid, MapTask>>("allTasks");

            using (ITransaction tx = this.stateManager_.CreateTransaction())
            {
                var jobConditionalValue = await jobsDictionary.TryGetValueAsync(tx, uuid);
                if(jobConditionalValue.HasValue)
                {
                    JobObject job = jobConditionalValue.Value;
                    job.SetTaskCompleted(task.Uuid);

                    if(job.IsComplete)
                    {
                        // Job is complete, remove it, send the result to FrontEndSvc for now
                        var removeConditional = await jobsDictionary.TryRemoveAsync(tx, job.Uuid);
                        if(!removeConditional.HasValue)
                        {
                            // Retry, I guess?
                        }

                        var counts = new List<Dictionary<string, int>>();

                        // Current task's Output field is null when taken from allTasks dictionary
                        foreach (var taskUuid in job.Tasks.Keys)
                        {
                            MapTask t = (await allTasks.TryRemoveAsync(tx, taskUuid)).Value;

                            counts.Add(t.Output ?? task.Output);
                        }

                        string serialized = JsonConvert.SerializeObject(counts);
                        await completeJobsQueue.EnqueueAsync(tx, serialized);

                    }
                    else
                    {
                        task.State = MapTask.StateType.Completed;
                        await jobsDictionary.SetAsync(tx, job.Uuid, job);
                        await allTasks.SetAsync(tx, task.Uuid, task);
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