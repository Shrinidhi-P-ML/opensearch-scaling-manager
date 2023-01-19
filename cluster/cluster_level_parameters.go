// This package provide the data structure needed to get the metrics.
// There are two kind of metrics:
//
//	Cluster metrics: This data structure will provide cluster level metrics.
//	Node metrics: This data structure will provide node level metrics.
//
// The cluster metrics contains list of the node metrics collected over all the nodes present in a cluster.
// The package contains a struct called MetricStatsCluster which will calculate the statistics over a period of time.
// The package contains a struct called MetricViolatedCountCluster which will calculate the violated count over a period of time.
// The structs be used by recommendation module.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"scaling_manager/logger"
	"strconv"

	opensearch "github.com/opensearch-project/opensearch-go"
	esapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

var log logger.LOG

// Input:
//
// Description:
//
//	Initialize the logger module.
//
// Return:
func init() {
	log.Init("logger")
	log.Info.Println("Main module initialized")
}

// This struct will contain node metrics for a node in the OpenSearch cluster.
type Node struct {
	// NodeId indicates a unique ID of the node given by OpenSearch.
	NodeId string
	// NodeName indicates human-readable identifier for a particular instance of OpenSearch which is a configurable input.
	NodeName string
	// HostIp indicates the IP address of the node.
	HostIp string
	// IsMater indicates if the node is a master node.
	IsMaster bool
	// IsData indicates if the node is a data node.
	IsData bool
	// CpuUtil indicates the overall CPU Utilization in percentage for a node.
	CpuUtil float32
	// MemUtil indicates the overall Memory Utilization in percentage for a node.
	RamUtil float32
	// HeapUtil indicates the overall Java Heap Utilization in percentage for a node.
	HeapUtil float32
	// DiskUtil indicates the overall Disk Utilization in percentage for a node.
	DiskUtil float32
	// NumShards Number of shards present on a node.
	NumShards int
}

// This struct will contain the static metrics of the cluster.
type ClusterStatic struct {
	// ClusterName indicates the Cluster name for the OpenSearch cluster.
	ClusterName string `yaml:"cluster_name" validate:"required,isValidName"`
	// IpAddress indicate the master node IP for the OpenSearch cluster.
	IpAddress string `yaml:"ip_address" validate:"required,ip"`
	// CloudType indicate the type of the cloud service where the OpenSearch cluster is deployed.
	CloudType string `yaml:"cloud_type" validate:"required,oneof=AWS GCP AZURE"`
	// BaseNodeType indicate the instance type of the node.
	// This parameters depends on the cloud service.
	BaseNodeType string `yaml:"base_node_type" validate:"required"`
	// NumCpusPerNode indicates the number of the CPU core running on a node in a cluster.
	NumCpusPerNode int `yaml:"number_cpus_per_node" validate:"required,min=1"`
	// RAMPerNodeInGB indicates the RAM size in GB running on a node in a cluster.
	RAMPerNodeInGB int `yaml:"ram_per_node_in_gb" validate:"required,min=1"`
	// DiskPerNodeInGB indicates the Disk size in GB running on a node in a cluster.
	DiskPerNodeInGB int `yaml:"disk_per_node_in_gb" validate:"required,min=1"`
	// NumMaxNodesAllowed indicates the number of maximum allowed node present in the cluster.
	// Based on this value we will determine whether to scale out further or not.
	NumMaxNodesAllowed int `yaml:"number_max_nodes_allowed" validate:"required,min=1"`
}

// This struct will contain the dynamic metrics of the cluster.
type ClusterDynamic struct {
	// NumNodes indicates the number of nodes present in the OpenSearch cluster at any time.
	NumNodes int
	//	ClusterStatus indicates the present state of a cluster.
	//	red: One or more primary shards are unassigned, so some data is unavailable.
	//		This can occur briefly during cluster startup as primary shards are assigned.
	//	yellow: All primary shards are assigned, but one or more replica shards are unassigned.
	//		If a node in the cluster fails, some data could be unavailable until that node is repaired.
	//	green: All shards are assigned.
	ClusterStatus string
	// NumActiveShards indicates the total number of active primary and replica shards.
	NumActiveShards int
	// NumActivePrimaryShards indicates the number of active primary shards.
	NumActivePrimaryShards int
	// NumInitializingShards indicates the number of shards that are under initialization.
	NumInitializingShards int
	// NumUnassignedShards indicats the number of shards that are not allocated.
	NumUnassignedShards int
	// NumRelocatingShards indicates the number of shards that are under relocation.
	NumRelocatingShards int
	// NumMasterNodes indicates the number of master eligible nodes present in the cluster.
	NumMasterNodes int
	// NumActiveDataNodes indicates the number of active data nodes present in the cluster.
	NumActiveDataNodes int
}

// This struct will provide the overall cluster metrcis for a OpenSearch cluster.
type Cluster struct {
	// ClusterStatic indicates the static set of data present for a cluster.
	ClusterStatic ClusterStatic
	// ClusterDyanamic indicates the dynamic set of data present for a cluster.
	ClusterDynamic ClusterDynamic
	// NodeList indicates node metrics for all the nodes.
	NodeList []Node
}

// This struct used by the recommendation engine to find the statistics of a metrics for a given period.(CPU, MEM, HEAP, DISK).
type MetricStats struct {
	// Avg indicates the average for a metric for a time period.
	Avg float32
	// Min indicates the minimum value for a metric for a time period.
	Min float32
	// Max indicates the maximum value for a metric for a time period.
	Max float32
}

// This struct contains statistics for a metric on a node for an evaluation period.
type MetricStatsNode struct {
	// MetricStats indicates statistics for a metric on a node.
	MetricStats
	// HostIp indicates the IP Address for a host
	HostIp string
}

// This struct contains statistics for cluster and node for an evaluation period.
type MetricStatsCluster struct {
	// MetricName indicate the metric for which the statistics is calculated for a given period
	MetricName string
	// ClusterLevel indicates statistics for a metric on a cluster for a time period.
	ClusterLevel MetricStats
	// NodeLevel indicates statistics for a metrics on all the nodes.
	NodeLevel []MetricStatsNode
}

// This struct will provide count, number of times a rule is voilated for a metric
type MetricViolatedCount struct {
	// Count indicates number of times the limit is reached calulated for a given period
	ViolatedCount int
}

// This struct will provide count, number of times a rule is voilated for a metric in a node
type MetricViolatedCountNode struct {
	// MetricViolatedCount indicates the violated count for a metric on a node.
	MetricViolatedCount
	// HostIp indicates the IP Address for a host
	HostIp string
}

// This contains the count voilated for cluster and node for an evaluation period.
type MetricViolatedCountCluster struct {
	// MetricName indicate the metric for which the count is calculated for a given period
	MetricName string
	// ClusterLevel indicates the count voilated for a metric on a cluster for a time period.
	ClusterLevel MetricViolatedCount
	// NodeLevel indicates the list of the count voilated for a metric on all the node for a time period.
	NodeLevel []MetricViolatedCountNode
}

// Input:
//
//		metricName: The Name of the metric for which the Cluster Average will be calculated(string).
//		decisionPeriod: The evaluation period for which the Average will be calculated.
//
// Description:
//
//		GetClusterAvg will use the opensearch query to find out the stats aggregation.
//		While getting stats aggregation it will pass the metricName and decisionPeriod as an input.
//		It will populate MetricStatsCluster struct and return it.
//
// Return:
//		Return populated MetricStatsCluster struct.

func getClusterAvgQuery(metricName string, decisionPeriod int) string {
	//nodesAvgQueryString:= `{"query":{"bool":{"filter":{"range":{"Timestamp":{"from": "now-`+strconv.Itoa(decisionPeriod)+`h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"node_statistics": {"terms": {"field": "HostIp.keyword","size": 100},"aggs": {`+metricName+`: { "stats": { "field":`+metricName+`} } }}}}`
	clusterAvgQueryString := `{"query": {"bool": {"filter": {"range": {"Timestamp": {"from": "now-` + strconv.Itoa(decisionPeriod) + `h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"` + metricName + `": { "stats": { "field":"` + metricName + `"} }}}`
	return clusterAvgQueryString
}

func GetClusterAvg(metricName string, decisionPeriod int, ctx context.Context, osClient *opensearch.Client) (MetricStats, []byte) {
	//Create an object of MetricStatsCluster to populate and return
	var metricStats MetricStats

	//Get the query and convert to json
	var jsonQuery = []byte(getClusterAvgQuery(metricName, decisionPeriod))

	indexName := []string{"monitor-stats-1"}

	//create a search request and pass the query
	searchQuery, err := esapi.SearchRequest{
		Index: indexName,
		Body:  bytes.NewReader(jsonQuery),
	}.Do(ctx, osClient)
	if err != nil {
		fmt.Println("Cannot fetch cluster average: ", err)
		return metricStats, []byte(err.Error())
	}
	//Interface to dump the response
	var queryResultInterface map[string]interface{}

	//decode the response into the interface
	decodeErr := json.NewDecoder(searchQuery.Body).Decode(&queryResultInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeErr)
		return metricStats, []byte(err.Error())
	}

	fmt.Println("Response Map")
	fmt.Println(queryResultInterface)
	//Parse the interface and populate the metricStatsCluster
	metricStats.Avg = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["avg"].(float64))
	metricStats.Max = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["max"].(float64))
	metricStats.Min = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["min"].(float64))
	return metricStats, nil
}

// Input:
//
//		metricName: The Name of the metric for which the Cluster Average will be calculated(string).
//		decisionPeriod: The evaluation period for which the Average will be calculated.(int)
//		limit: The limit for the particular metric for which the count is calculated.(float32)
//
// Description:
//
//		GetClusterCount will use the opensearch query to find out the stats aggregation.
//		While getting stats aggregation it will pass the metricName, decisionPeriod and limit as an input.
//		It will populate MetricViolatedCountCluster struct and return it.
//
// Return:
//		Return populated MetricViolatedCountCluster struct.

func getClusterCountQuery(metricName string, decisionPeriod int, limit float32) string {
	clusterCountQueryString := `{"query": {"bool": {"filter":{"range": {"Timestamp": {"from": "now-` + strconv.Itoa(decisionPeriod) + `h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"` + metricName + `": { "range": { "field": "` + metricName + `" , "ranges": [{"from":` + strconv.FormatFloat(float64(limit), 'E', -1, 32) + `, "to":null}] }}}}`
	return clusterCountQueryString
}

func GetClusterCount(metricName string, decisionPeriod int, limit float32, ctx context.Context, osClient *opensearch.Client) (MetricViolatedCount, []byte) {
	var metricViolatedCount MetricViolatedCount

	//Get the query and convert to json
	var jsonQuery = []byte(getClusterCountQuery(metricName, decisionPeriod, limit))

	indexName := []string{"monitor-stats-1"}

	//create a search request and pass the query
	searchQuery, err := esapi.SearchRequest{
		Index: indexName,
		Body:  bytes.NewReader(jsonQuery),
	}.Do(ctx, osClient)
	if err != nil {
		fmt.Println("Cannot fetch cluster average: ", err)
		return metricViolatedCount, []byte(err.Error())
	}

	//Interface to dump the response
	var queryResultInterface map[string]interface{}

	//decode the response into the interface
	decodeErr := json.NewDecoder(searchQuery.Body).Decode(&queryResultInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeErr)
		return metricViolatedCount, []byte(err.Error())
	}
	fmt.Println()
	fmt.Println("Response Map: ")
	fmt.Println(queryResultInterface)
	fmt.Println()
	//Parse the interface and populate the metricStatsCluster
	metricViolatedCount.ViolatedCount = int(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["buckets"].([]interface{})[0].(map[string]interface{})["doc_count"].(float64))

	return metricViolatedCount, nil
}

// Input:
// Description:
//
//		GetClusterCurrent will fetch the node level and cluster level metrics and fill in
//		ClusterDynamic, clusterStatic and Node struct using the given config file.
//		It will return the current cluster status.
//
// Return:
//		Return populated ClusterDynamic struct.

func GetClusterCurrent(ctx context.Context, osClient *opensearch.Client) ClusterDynamic {
	var clusterCurrent ClusterDynamic

	//execute the query and get the cluster level info for recent poll
	var clusterJsonQuery = []byte(`{ "query": { "bool": { "must": [ { "match": { "StatTag": "ClusterStats" } } ] } }, "aggs": { "top_hit": { "top_hits": { "size": 1, "sort": [ { "Timestamp": { "order": "desc" } } ] } } } }`)

	//create a map to dump the respone
	//var clusterInfoInterface map[string]interface{}
	var clusterLevelInfoInterface map[string]interface{}
	indexName := []string{"monitor-stats-1"}

	clusterSearchQuery, err := esapi.SearchRequest{
		Index: indexName,
		Body:  bytes.NewReader(clusterJsonQuery),
	}.Do(ctx, osClient)
	if err != nil {
		fmt.Println("Cannot fetch cluster average: ", err)
		return clusterCurrent
	}

	decodeClusterErr := json.NewDecoder(clusterSearchQuery.Body).Decode(&clusterLevelInfoInterface)
	if decodeClusterErr != nil {
		fmt.Println("decode Error: ", decodeClusterErr)
		return clusterCurrent
	}

	fmt.Println()
	fmt.Println("ClusterLevelInfo Interface: ")
	fmt.Println(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{}))
	fmt.Println()

	//Populating cluster dynamic in cluster structure
	clusterCurrent.ClusterStatus = clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["ClusterStatus"].(string)
	clusterCurrent.NumActiveDataNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActiveDataNodes"].(float64))
	clusterCurrent.NumActivePrimaryShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActivePrimaryShards"].(float64))
	clusterCurrent.NumActiveShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActiveShards"].(float64))
	clusterCurrent.NumInitializingShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumInitializingShards"].(float64))
	clusterCurrent.NumMasterNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumMasterNodes"].(float64))
	clusterCurrent.NumNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumNodes"].(float64))
	clusterCurrent.NumRelocatingShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumRelocatingShards"].(float64))
	clusterCurrent.NumUnassignedShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumUnassignedShards"].(float64))
	return clusterCurrent
}

// Input:
//
//		decisionPeriod: The evaluation period for which the Average will be calculated.(int)
//
// Description:
//
//		GetClusterHistoricAvg will get Historic average for the cluster for all the metrics.
//		GetClusterHistoricAvg will use the stats aggregation to fetch the cluster and node level
//		Historic average for the mentioned decision period.
//
// Return:
//		Return an array of populated MetricStatsCluster struct collected for all the metrics.

func GetClusterHistoricAvg(decisonPeriod int) []MetricStatsCluster {
	var metricStatsCluster []MetricStatsCluster
	return metricStatsCluster
}

// Input:
//
//		decisionPeriod: The evaluation period for which the Average will be calculated.(int)
//		thresholdMap: The map provide mapping of metric name and the threshold for which the Count is calculated.
//
// Description:
//
//		GetClusterHistoricCount will use the opensearch query to find out the Count for which a metric crossed the threshold limit.
//		GetClusterHistoricCount will then iterate through all the metric and collect the count for all the metrics.
//		It will return the array of node level and cluster level count been voilated for all the metrics.
//
// Return:
//		Return array of populated MetricViolatedCountCluster struct.

func GetClusterHistoricCount(decisionPeriod int, thresholdMap map[string]int) []MetricViolatedCountCluster {
	var metricViolatedCount []MetricViolatedCountCluster
	return metricViolatedCount
}

// Input:
//
// Description:
//		Calls ES Api to find the current master node in the cluster
//
// Return:
//              Returns the Ip of current master node

func GetCurrentMasterIp() string {
	return "10.81.1.225"
}

// Input:
//
// Description:
//              Calls ES Api to check if the current node is the master node of the cluster
//
// Return:
//              Returns true/false based on the return of API

func CheckIfMaster() bool {
	var currentNode Node
	// For testing
	currentNode.IsMaster = true
	return currentNode.IsMaster
}

// Input:
//
// Description:
//              Calls OS Api to get the cluster id
//				ToDo: We need to add logic to fetch the cluster id from OS and return it.
// Return:
//              Returns clusterId for the cluster

func GetClusterId() string {
	var clusterId = "vcPboLtxQXyPhJMe8bn44A"
	return clusterId
}

func GetClusterDetails(ctx context.Context, osClient *opensearch.Client) Cluster {
	var cluster Cluster
	return cluster
}
