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
	"net"
	"strconv"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
)

var (
	address   = flag.String("address", "127.0.0.1", "Binding address")
	master    = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	taskCount = flag.Int("task-count", 5, "Total task count to run.")
	jobCmd    = flag.String("cmd", "while true; do echo 'hello world'; date; sleep 1; done", "Command to execute")
)

type CommandScheduler struct {
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

func (sched *CommandScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	fmt.Println("Framework Registered with Master ", masterInfo)
}

func (sched *CommandScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	fmt.Println("Framework Re-Registered with Master ", masterInfo)
}

func (sched *CommandScheduler) Disconnected(sched.SchedulerDriver) {}

func (sched *CommandScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

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

		fmt.Println("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched < sched.totalTasks &&
			CPUS_PER_TASK <= remainingCpus &&
			MEM_PER_TASK <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			task := &mesos.TaskInfo{
				Name:    proto.String("go-cmd-task-" + taskId.GetValue()),
				TaskId:  taskId,
				SlaveId: offer.SlaveId,
				// Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUS_PER_TASK),
					util.NewScalarResource("mem", MEM_PER_TASK),
				},
				Command: &mesos.CommandInfo{
					Value: proto.String(*jobCmd),
				},
			}
			fmt.Printf("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= CPUS_PER_TASK
			remainingMems -= MEM_PER_TASK
		}
		fmt.Println("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *CommandScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	fmt.Println("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		fmt.Println("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		fmt.Println(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
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
	fmt.Println("Scheduler received error:", err)
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

	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Go Command Scheduler"),
	}

	bindingAddress := parseIP(*address)

	config := sched.DriverConfig{
		Scheduler: &CommandScheduler{
			tasksLaunched: 0,
			tasksFinished: 0,
			totalTasks:    *taskCount,
		},
		Framework:      fwinfo,
		Master:         *master,
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
