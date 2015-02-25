/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	io "io/ioutil"
	"net"
	"strconv"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	org "github.com/wallyqs/org-go"
)

const (
	MIN_CPUS_PER_TASK = 1
	MIN_MEM_PER_TASK  = 128
)

var (
	orgFile = flag.String("f", "", "Org mode file to run")
)

type CommandScheduler struct {
	tasksLaunched int
	tasksFinished int
	blocks        []*org.OrgSrcBlock
}

func (sched *CommandScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	fmt.Println("[REGIST] Framework Registered with Master ", masterInfo)
}

func (sched *CommandScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	fmt.Println("[REGIS2] Framework Re-Registered with Master ", masterInfo)
}

func (sched *CommandScheduler) Disconnected(sched.SchedulerDriver) {}

func (sched *CommandScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

	// We will get many resource offerings,
	// but sometimes the resources being offered will not be enough
	// so we will need to implement backing off in case that happens.
	for _, offer := range offers {

		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		fmt.Println("[OFFER ] offerId =", offer.Id.GetValue(), ", cpus =", cpus, ", mem =", mems)
		if cpus < MIN_CPUS_PER_TASK {
			fmt.Println("[OFFER ] Not enough cpu!")
			continue
		}

		if mems < MIN_MEM_PER_TASK {
			fmt.Println("[OFFER ] Not enough mem!")
			continue
		}

		var tasks []*mesos.TaskInfo

		for _, src := range sched.blocks {
			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			// Should build the command properly depending of the runtime
			// Currenty only bash supported, but good enough
			// since I can just call the runtime from there
			cmd := src.RawContent

			fmt.Println("[OFFER ] Executing this code block:", src.Name, src.Headers)

			// The code block specifies the resources it should allocate
			//
			taskCpus := MIN_CPUS_PER_TASK
			if src.Headers[":cpus"] != "" {
				taskCpus, _ = strconv.Atoi(src.Headers[":cpus"])
			}

			taskMem := MIN_MEM_PER_TASK
			if src.Headers[":mem"] != "" {
				taskMem, _ = strconv.Atoi(src.Headers[":mem"])
			}

			task := &mesos.TaskInfo{
				Name:    proto.String("ob-mesos-" + taskId.GetValue()),
				TaskId:  taskId,
				SlaveId: offer.SlaveId,
				// Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(taskCpus)),
					util.NewScalarResource("mem", float64(taskMem)),
				},
				Command: &mesos.CommandInfo{
					Value: proto.String(cmd),
				},
			}
			fmt.Printf("[OFFER ] Prepared to launch task:%s with offer %s \n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
		}
		fmt.Println("[OFFER ] Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *CommandScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	fmt.Println("[STATUS] task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= len(sched.blocks) {
		fmt.Println("[STATUS] All code blocks have been ran. Done.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		fmt.Println(
			"[STATUS] Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		fmt.Println("[STATUS] Stopping all tasks.")
		driver.Abort()
	}
}

func (sched *CommandScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *CommandScheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

func (sched *CommandScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}

func (sched *CommandScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (sched *CommandScheduler) Error(driver sched.SchedulerDriver, err string) {
	fmt.Println("[ERROR ] Scheduler received error:", err)
}

func init() {
	flag.Parse()
	fmt.Println("Initializing the Command Scheduler...")
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		fmt.Println(err)
	}
	if len(addr) < 1 {
		fmt.Printf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

func main() {

	// Parse Org mode file first and get the code blocks that will be run
	//
	fmt.Println("Reading Org mode file: ", *orgFile)
	contents, err := io.ReadFile(*orgFile)
	if err != nil {
		fmt.Printf("Problem reading the file: %v \n", err)
	}

	root := org.Preprocess(string(contents))
	tokens := org.Tokenize(string(contents), root)

	blocks := make([]*org.OrgSrcBlock, 0)
	for _, t := range tokens {
		switch o := t.(type) {
		case *org.OrgSrcBlock:
			blocks = append(blocks, o)
		}
	}

	// The Mesos part
	//
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""), // covered by the mesos-go bindings
		Name: proto.String("Org Babel Scheduler"),
	}

	bindingAddress := parseIP(root.Settings["ADDRESS"])

	// Here we would pass the code blocks list
	//
	config := sched.DriverConfig{
		Scheduler: &CommandScheduler{
			tasksLaunched: 0,
			tasksFinished: 0,
			blocks:        blocks,
		},
		Framework:      fwinfo,
		Master:         root.Settings["MASTER"],
		BindingAddress: bindingAddress,
	}
	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		fmt.Println("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		fmt.Printf("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}

}
