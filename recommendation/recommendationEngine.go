
// This package consists of all the data structure required for defining a task.
// Tasks are set of Actions.
// The actions can have list of rules.
// The recommendation engine will parse these rules and recommend the action if rules meets the criteria.
// Multiple rules can be added inside an action and like wise multiple actions can be added inside a task.
package recommendation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"strconv"

	cron "github.com/robfig/cron/v3"

	"github.com/maplelabs/opensearch-scaling-manager/cluster"
	"github.com/maplelabs/opensearch-scaling-manager/cluster_sim"
	"github.com/maplelabs/opensearch-scaling-manager/config"
	"github.com/maplelabs/opensearch-scaling-manager/logger"
	"github.com/maplelabs/opensearch-scaling-manager/provision"
	

)

var log logger.LOG

var ctx = context.Background()

// A global variable to keep track of cronJob details
var cronJobList []*cron.Cron

// Input:
//
// Description:
//		Initialize the recommendation module.
//
// Return:

func init() {
	log.Init("logger")
	log.Info.Println("Recommendation module initialized")
}

// Inputs:
//		simFlag (bool): A flag to check if the task needs to be evaluated from Opensearch data or simulated data.
//		pollingInterval (int): Time in seconds which is the interval between each metric is pushed into the index.
//
// Caller:
//		Object of TaskDetails
//
// Description:
//              EvaluateTask will go through all the tasks one by one. and
//              It check if the task are meeting the criteria based on rules and operator.
//              If the task is meeting the criteria then it will push the task to recommendation queue.
//
// Return:
//		([]map[string]string): Returns an array of the recommendations.

func EvaluateTask(t *config.TaskDetails, pollingInterval int, simFlag, isAccelerated bool, t config.TaskDetails)[]map[string]string {
	var recommendationArray []map[string]string
	var isRecommendedTask bool
	for _, v := range t.Tasks {
		// v := (MyTask)(v)
		var rulesResponsibleMap = make(map[string]string)
		isRecommendedTask, rulesResponsibleMap[v.TaskName] = v.GetNextTask(pollingInterval, simFlag, isAccelerated)
		log.Debug.Println(rulesResponsibleMap)
		if isRecommendedTask {
			PushToRecommendationQueue(v)
			recommendationArray = append(recommendationArray, rulesResponsibleMap)
		} else {
			log.Debug.Println(fmt.Sprintf("The %s task is not recommended as rules are not satisfied", v.TaskName))
		}
	}
	return recommendationArray
}

// Inputs:
//		simFlag (bool): A flag to check if the task needs to collect stats from Opensearch data or simulated data.
//		pollingInterval (int): Time in seconds which is the interval between each metric is pushed into the index.
//
// Caller: Object of Task
// Description:
//
//              GetNextTask will get the Task and iterate through all the rules inside the task.
//              Based on the operator it will check if it should iterate through all the rules or not.
//              It will call GetNextRule while iterating through the rules.
//              Based on the result GetNextTask will check if a task can be recommended or not.
//
// Return:
//
//              (bool, string): Return if a task can be recommended or not(bool) and string which says the rules responsible for that recommendation.

func GetNextTask(pollingInterval int, simFlag, isAccelerated bool, t config.Task) (bool, string) {
	var isRecommendedTask bool = true
	var isRecommendedRule bool
	var rulesResponsible string
	var err error

	scaleRegexString := `(scale_up|scale_down)_by_([0-9]+)`
	scaleRegex := regexp.MustCompile(scaleRegexString)

	subMatch := scaleRegex.FindStringSubmatch(t.TaskName)

	taskOperation := subMatch[1]

	var rules []string
	for _, v := range t.Rules {
		// Here we can add go routine.
		// So that all the rules getMetrics will be fetched in concurrent way
		// There is a possibility that each rule is taking time.
		// What if in the case of AND the non matching rule is present at the last.
		// What if in the case of OR the matching rule is present at the last.
		// v := (MyRule)(v)
		isRecommendedRule, err = GetNextRule(taskOperation, pollingInterval, simFlag, isAccelerated, v)
		if err != nil {
			log.Warn.Println(fmt.Sprintf("%s for the rule: %v", err, v))
		}
		if isRecommendedRule {
			if v.Stat == "AVG" {
				rules = append(rules, fmt.Sprintf("%s-%s-%f-%d", v.Metric, v.Stat, v.Limit, v.DecisionPeriod))
			} else {
				rules = append(rules, fmt.Sprintf("%s-%s-%f-%d-%d", v.Metric, v.Stat, v.Limit, v.Occurrences, v.DecisionPeriod))
			}

		}
		if t.Operator == "OR" && isRecommendedRule ||
			t.Operator == "AND" && !isRecommendedRule {
			break
		}
	}
	isRecommendedTask = isRecommendedRule
	if len(rules) > 1 {
		rulesResponsible = strings.Join(rules, "_and_")
	} else if len(rules) == 1 {
		rulesResponsible = rules[0]
	}
	return isRecommendedTask, rulesResponsible
}

// Input:
//		taskOperation (string); Recommended operation
//		simFlag (bool): A flag to check if the task needs to collect stats from Opensearch data or simulated data.
//              pollingInterval (int): Time in seconds which is the interval between each metric is pushed into the index.
//
// Caller:
//		Object of Rule
//
// Description:
//		GetNextRule will fetch the metrics based on the rules MetricName and Stats using GetMetrics
//		Then it will evaluate if the rule is meeting the criteria or not using EvaluateRule
//
// Return:
// 		(bool, error): Return if a rule is meeting the criteria or not(bool) and error if any

func GetNextRule(taskOperation string, pollingInterval int, simFlag, isAccelerated bool, r config.Rule) (bool, error) {
	cluster, err := GetMetrics(pollingInterval, simFlag, isAccelerated, r)
	if err != nil {
		return false, err
	}
	isRecommended := EvaluateRule(cluster, taskOperation, pollingInterval, r)
	log.Debug.Println(r)
	log.Debug.Println(isRecommended)
	return isRecommended, nil
}

// Input:
//		simFlag (bool): A flag to check if the task needs to collect stats from Opensearch data or simulated data.
//		pollingInterval (int): Time in seconds which is the interval between each metric is pushed into the index.
//
// Caller:
//		Object of Rule
//
// Description:
//              GetMetrics will be getting the metrics for a metricName based on its stats
//              If the stat is Avg then it will call GetClusterAvg which will provide MetricViolatedCountCluster struct.
//              If the stat is Count or Term then it will call GetClusterCount which will provide MetricViolatedCountCluster struct.
//              At last it marshal the structure such that uniform data can be used across multiple methods.
//
// Return:
//              ([]byte, error): Return marshal form of either MetricStatsCluster or MetricViolatedCountCluster struct([]byte) and error if any

func GetMetrics(pollingInterval int, simFlag, isAccelerated bool, r config.Rule) ([]byte, error) {
	var clusterStats cluster.MetricStats
	var clusterCount cluster.MetricViolatedCount
	var clusterMetric []byte
	var jsonErr error
	var err error
	var invalidDatapoints bool

	if r.Stat == "AVG" {
		if simFlag {
			clusterStats, err = cluster_sim.GetClusterAvg(r.Metric, r.DecisionPeriod, isAccelerated)
		} else {
			clusterStats, invalidDatapoints, err = cluster.GetClusterAvg(ctx, r.Metric, r.DecisionPeriod, pollingInterval)
		}

		if err != nil || invalidDatapoints {
			if invalidDatapoints {
				err = errors.New("Not enough data points")
			}
			return clusterMetric, err
		}
		clusterMetric, jsonErr = json.MarshalIndent(clusterStats, "", "\t")
		log.Debug.Println(clusterStats)
		if jsonErr != nil {
			log.Panic.Println("Error converting struct to json: ", jsonErr)
			panic(jsonErr)
		}
	} else if r.Stat == "COUNT" || r.Stat == "TERM" {
		if simFlag {
			clusterCount, err = cluster_sim.GetClusterCount(r.Metric, r.DecisionPeriod, r.Limit, isAccelerated)
		} else {
			clusterCount, invalidDatapoints, err = cluster.GetClusterCount(ctx, r.Metric, r.DecisionPeriod, pollingInterval, r.Limit)
		}

		if err != nil || invalidDatapoints {
			if invalidDatapoints {
				err = errors.New("Not enough data points")
			}
			return clusterMetric, err
		}
		clusterMetric, jsonErr = json.MarshalIndent(clusterCount, "", "\t")
		log.Debug.Println(clusterCount)
		if jsonErr != nil {
			log.Panic.Println("Error converting struct to json: ", jsonErr)
			panic(jsonErr)
		}
	}

	return clusterMetric, nil
}

// Input:
//		clusterMetric ([]byte): Marshal struct containing clusterMetric details based on stats.
//		taskOperation (string); Task recommended
//
// Caller:
//		Object of Rule
//
// Description:
//		EvaluateRule will be compare the collected metric and mentioned rule
//		It will then decide if rules are meeting the criteria or not and return the result.
//
// Return:
//              (bool): Return whether a rule is meeting the criteria or not.

func EvaluateRule(clusterMetric []byte, taskOperation string, pollingInterval int, r config.Rule) bool {
	log.Debug.Println(taskOperation)
	if r.Stat == "AVG" {
		var clusterStats cluster.MetricStats
		err := json.Unmarshal(clusterMetric, &clusterStats)
		if err != nil {
			log.Panic.Println("Error converting struct to json: ", err)
			panic(err)
		}
		if taskOperation == "scale_up" && clusterStats.Avg > r.Limit ||
			taskOperation == "scale_down" && clusterStats.Avg < r.Limit {
			return true
		} else {
			return false
		}
	} else if r.Stat == "COUNT" || r.Stat == "TERM" {
		var clusterStats cluster.MetricViolatedCount
		err := json.Unmarshal(clusterMetric, &clusterStats)
		if err != nil {
			log.Panic.Println("Error converting struct to json: ", err)
			panic(err)
		}
		if r.Stat == "COUNT" {
			occurence, err := strconv.ParseFloat(strings.Replace(r.Occurrences, "%", "", -1), 64)
			if err != nil {
				log.Error.Println("Error reading the float from percentage value given in config")
				return false
			}
			if strings.Contains(r.Occurrences, "%") {
				counts := (r.DecisionPeriod * 60) / pollingInterval
				if counts != 0 {
					if taskOperation == "scale_up" && float64((clusterStats.ViolatedCount*100)/(counts)) >= occurence {
						return true
					} else if taskOperation == "scale_down" && float64((clusterStats.ViolatedCount*100)/(counts)) <= occurence {
						return true
					}
				} else {
					log.Error.Println("Divide by zero error. (Decision period/pollingInterval) ")
					return false
				}
			} else {
				if taskOperation == "scale_up" && float64(clusterStats.ViolatedCount) > occurence ||
					taskOperation == "scale_down" && float64(clusterStats.ViolatedCount) < occurence {
					return true
				} else {
					return false
				}
			}
		} else if r.Stat == "TERM" {
			if taskOperation == "scale_up" && clusterStats.ViolatedCount > 0 ||
				taskOperation == "scale_down" && clusterStats.ViolatedCount == 0 {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

//	Input:
//
// 	Caller:
// 		Object of TaskDetails
//
// 	Description:
// 		Parser over the Tasks and seperates metric and event based tasks
//
// 	Return:
// 		(*TaskDetails): Pointer to metric and event based Tasks

func ParseTasks(taskDetails config.TaskDetails) (*config.TaskDetails, *config.TaskDetails) {
	var metricTaskDetails = new(config.TaskDetails)
	var eventTaskDetails = new(config.TaskDetails)

	for _, task := range taskDetails.Tasks {
		task := task

		if task.Operator == "EVENT" {
			eventTaskDetails.Tasks = append(eventTaskDetails.Tasks, task)
		} else {
			metricTaskDetails.Tasks = append(metricTaskDetails.Tasks, task)
		}
	}

	return metricTaskDetails, eventTaskDetails
}

// Input:
//
//	cronTasks ([]]recommendation.Task): List of tasks to be added to Cron Job
//	state (*provision.State): A pointer to the state struct which is state maintained in OS document
//	clusterCfg (config.ClusterDetails): Cluster Level config details
//	usrCfg (config.UserConfig): User defined config for application behavior
//
// Description:
//
//		At each polling interval creates the cron jobs based on the config file. It removes the Cron Jobs that were
//	 added in previous polling interval and creates required jobs. It will use the list of tasks (cronTasks) to
//		schedule and create cron job.
//
// Return:
func CreateCronJob(state *provision.State, eventTasks *config.TaskDetails, config.ClusterDetails, userCfg config.UserConfig, t *time.Time) {
	for _, cronJob := range cronJobList {
		for _, jobs := range cronJob.Entries() {
			cronJob.Remove(jobs.ID)
		}
	}

	cronJobList = nil

	for _, cronTask := range eventTasks.Tasks {
		cronTask := cronTask
		cronJob := cron.New()
		for _, rules := range cronTask.Rules {
			rules := rules
			cronJob.AddFunc(rules.SchedulingTime, func() {
				provision.TriggerCron(rules.NumNodesRequired, t, state, clusterCfg, userCfg, rules.SchedulingTime, cronTask.TaskName)
			})
			cronJobList = append(cronJobList, cronJob)
		}
		cronJob.Start()
	}
}

// Input:
//
// Caller:
//		Object of Task
// Description:
//		PushToRecommendationQueue will be pushing the task which matches the criteria to recommendation queue.
//
// Return:

func PushToRecommendationQueue(task config.Task) {
	log.Info.Println(fmt.Sprintf("The %s task is recommended and will be pushed to the queue", task.TaskName))
}
