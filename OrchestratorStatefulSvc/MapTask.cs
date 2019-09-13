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

        public MapTask(Guid parentUuid, string input)
        {
            this.ParentJobUuid = parentUuid;
            this.Input = input;
            this.Uuid = System.Guid.NewGuid();
        }


        [DataMember]
        public Guid Uuid { get; set; }
        [DataMember]
        public string Input { get; set; }
        [DataMember]
        public Dictionary<string, int> Output { get; set; }
        [DataMember]
        public Guid ParentJobUuid { get; set; }
    }
}
