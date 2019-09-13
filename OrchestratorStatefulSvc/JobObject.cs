using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace OrchestratorStatefulSvc
{
    // Set as immutable?
    [DataContract]
    public class JobObject
    {
     
        public JobObject(Guid uuid)
        {
            Uuid = uuid;
            tasks_ = new Dictionary<Guid, bool>();
            complete_ = false;
        }


        public void SetTaskCompleted(Guid taskUuid)
        {
            tasks_[taskUuid] = true;
            complete_ = tasks_.All(kv => kv.Value);
        }

        [DataMember]
        private Guid uuid_;

        [DataMember]
        private Dictionary<Guid, bool> tasks_;

        [DataMember]
        private bool complete_;

        
        public Guid Uuid { get => uuid_; set => uuid_ = value; }
        public bool IsComplete { get => complete_; set => complete_ = value;}
        public Dictionary<Guid, bool> Tasks { get => tasks_; set => tasks_ = value; }

        
    }
}
