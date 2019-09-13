using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace OrchestratorStatefulSvc
{
    [DataContract]
    public class MapTask
    { 

        public MapTask()
        {
            // For Serialization
        }
        public MapTask(Guid parentUuid, int index, string input)
        {
            this.ParentJobUuid = parentUuid;
            this.Index = index;
            this.Input = input;
        }

        [DataMember]
        public int Index { get; set; }
        [DataMember]
        public string Input { get; set; }
        [DataMember]
        public Dictionary<string, int> Output { get; set; }
        [DataMember]
        public Guid ParentJobUuid { get; set; }
    }
}
