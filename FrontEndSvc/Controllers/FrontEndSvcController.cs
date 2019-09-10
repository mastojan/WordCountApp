using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FrontEndSvc.Controllers
{
    [Produces("application/json")]
    [Route("api/WordCount")]
    public class FrontEndSvcController : Controller
    {
        private readonly HttpClient httpClient;
        private readonly StatelessServiceContext context;
        private readonly FabricClient fabricClient;

        public FrontEndSvcController(HttpClient httpClient, StatelessServiceContext context, FabricClient fabricClient)
        {
            this.httpClient = httpClient;
            this.context = context;
            this.fabricClient = fabricClient;
        }

        // Post: api/WordCount
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] string sourceText)
        {
            ServiceEventSource.Current.ServiceMessage(this.context, "Received param: " + sourceText);
            MatchCollection collection = Regex.Matches(sourceText, @"\b\w+\b");
            Dictionary<string, int> counts = new Dictionary<string, int>();
            foreach(Match match in collection)
            {
                string s = match.Value.ToLower();
                if(counts.ContainsKey(s))
                {
                    counts[s]++;
                }
                else
                {
                    counts[s] = 1;
                }

            }

            return Json(counts);
        }
    }
}