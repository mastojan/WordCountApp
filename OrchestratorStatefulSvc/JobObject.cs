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
        public JobObject()
        {
            // For Serialization
        }

        public JobObject(string sourceText, int taskSize)
        {
            Uuid = System.Guid.NewGuid();
            tasks_ = new List<MapTask>();
            complete_ = false;

            int cursorPos = 0;
            while(cursorPos + taskSize < sourceText.Length)
            {
                int nextCursorPos = cursorPos + taskSize;
                while(sourceText[nextCursorPos] != ' ')
                {
                    nextCursorPos--;
                }
                MapTask task = new MapTask(Uuid, tasks_.Count, sourceText.Substring(cursorPos, nextCursorPos - cursorPos));
                tasks_.Add(task);
                cursorPos = nextCursorPos + 1;
            }
            tasks_.Add(new MapTask(Uuid, tasks_.Count, sourceText.Substring(cursorPos)));

        }


        public void SetTaskCompleted(MapTask completedTask)
        {
            tasks_[completedTask.Index].Output = completedTask.Output;
            complete_ = tasks_.All(task => task.Output != null);
        }

        [DataMember]
        private Guid uuid_;

        [DataMember]
        private List<MapTask> tasks_;

        [DataMember]
        private bool complete_;

        
        public Guid Uuid { get => uuid_; set => uuid_ = value; }
        public bool IsComplete { get => complete_; set => complete_ = value;}
        public List<MapTask> Tasks { get => tasks_; set => tasks_ = value; }

        
    }
}
