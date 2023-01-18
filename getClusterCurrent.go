package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	elasticsearch "github.com/opensearch-project/opensearch-go"
	esapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

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
	ClusterName string `yaml:"cluster_name"`
	// IpAddress indicate the master node IP for the OpenSearch cluster.
	IpAddress string `yaml:"ip_address"`
	// CloudType indicate the type of the cloud service where the OpenSearch cluster is deployed.
	CloudType string `yaml:"cloud_type"`
	// BaseNodeType indicate the instance type of the node.
	// This parameters depends on the cloud service.
	BaseNodeType string `yaml:"base_node_type"`
	// NumCpusPerNode indicates the number of the CPU core running on a node in a cluster.
	NumCpusPerNode int `yaml:"number_cpus_per_node"`
	// RAMPerNodeInGB indicates the RAM size in GB running on a node in a cluster.
	RAMPerNodeInGB int `yaml:"ram_per_node_in_gb"`
	// DiskPerNodeInGB indicates the Disk size in GB running on a node in a cluster.
	DiskPerNodeInGB int `yaml:"disk_per_node_in_gb"`
	// NumMaxNodesAllowed indicates the number of maximum allowed node present in the cluster.
	// Based on this value we will determine whether to scale out further or not.
	NumMaxNodesAllowed int `yaml:"number_max_nodes_allowed"`
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

func GetClusterCurrent(ctx context.Context, esClient *elasticsearch.Client) Cluster {
	var clusterCurrent Cluster

//execute the query and get the cluster level info for recent poll

	var jsonQuery = []byte(`{ "query": { "bool": { "must": [ { "match": { "StatTag": "NodesStats" } } ] } }, "aggs": { "nodes": { "terms": { "field": "HostIp.keyword", "size": 100 }, "aggs": { "top_hit": { "top_hits": { "size": 1, "sort": [ { "Timestamp": { "order": "desc" } } ] } } } } } }`)
	var clusterJsonQuery = []byte(`{ "query": { "bool": { "must": [ { "match": { "StatTag": "ClusterStats" } } ] } }, "aggs": { "top_hit": { "top_hits": { "size": 1, "sort": [ { "Timestamp": { "order": "desc" } } ] } } } }`)
	//create a map to dump the respone 
	var clusterInfoInterface map[string]interface{}
	var clusterLevelInfoInterface map[string]interface{}
	indexName:= []string{"monitor-stats-1"}

	//Execute the query and get the response
	searchQuery,err:= esapi.SearchRequest{
		Index: indexName,
		Body: bytes.NewReader(jsonQuery),
	}.Do(ctx,esClient)
	if err!=nil{
		fmt.Println("Cannot fetch cluster average: ",err)
		return clusterCurrent
	}

	//decode the response into the interface
	decodeErr := json.NewDecoder(searchQuery.Body).Decode(&clusterInfoInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeErr)
		return clusterCurrent
	}
	
	fmt.Println()
	fmt.Println("ClusterInfo Interface: ")
	fmt.Println(clusterInfoInterface)
	

	b, err := json.MarshalIndent(clusterInfoInterface,"","\t")
         if err != nil {
                fmt.Printf("Error: %s", err)
                 return clusterCurrent;
         }
        fmt.Println()
        fmt.Println(string(b))
        fmt.Println()


//Parse the search result and populate the structure
	bucketList:=clusterInfoInterface["aggregations"].(map[string]interface{})["nodes"].(map[string]interface{})["buckets"].([]interface{})
//	bucketList:=clusterInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})
	fmt.Println()
	fmt.Println("BucketList: ")
	fmt.Println(bucketList)
	fmt.Println()
	for i:=range bucketList{
		nodeInfo:=bucketList[i].(map[string]interface{})
		var node Node
//		node.HostIp = nodeInfo["_source"].(map[string]interface{})["HostIp"].(string)
//		node.NodeId = nodeInfo["_source"].(map[string]interface{})["NodeId"].(string)
//		node.NodeName = nodeInfo["_source"].(map[string]interface{})["NodeName"].(string)
//		node.IsMaster = nodeInfo["_source"].(map[string]interface{})["IsMaster"].(bool)
//		node.IsData = nodeInfo["_source"].(map[string]interface{})["IsData"].(bool)
//		node.CpuUtil = float32(nodeInfo["_source"].(map[string]interface{})["CpuUtil"].(float64))
//		node.RamUtil = float32(nodeInfo["_source"].(map[string]interface{})["RamUtil"].(float64))
//		node.HeapUtil = float32(nodeInfo["_source"].(map[string]interface{})["HeapUtil"].(float64))
//		node.DiskUtil = float32(nodeInfo["_source"].(map[string]interface{})["DiskUtil"].(float64))
//		node.NumShards = int(nodeInfo["_source"].(map[string]interface{})["NumShards"].(float64))
		node.HostIp = nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["HostIp"].(string)
		node.NodeId =nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NodeId"].(string)
		node.NodeName = nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NodeName"].(string)
		node.IsMaster = nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["IsMaster"].(bool)
		node.IsData = nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["IsData"].(bool)
		node.CpuUtil = float32(nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["CpuUtil"].(float64))
		node.RamUtil = float32(nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["RamUtil"].(float64))
		node.HeapUtil = float32(nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["HeapUtil"].(float64))
		node.DiskUtil = float32(nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["DiskUtil"].(float64))
		node.NumShards = int(nodeInfo["top_hit"].(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumShards"].(float64))
		clusterCurrent.NodeList = append(clusterCurrent.NodeList, node)
	}

	clusterSearchQuery,err:= esapi.SearchRequest{
		Index: indexName,
		Body: bytes.NewReader(clusterJsonQuery),
	}.Do(ctx,esClient)
	if err!=nil{
		fmt.Println("Cannot fetch cluster average: ",err)
		return clusterCurrent
	}

	decodeClusterErr := json.NewDecoder(clusterSearchQuery.Body).Decode(&clusterLevelInfoInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeClusterErr)
		return clusterCurrent
	}

	fmt.Println()
	fmt.Println("ClusterLevelInfo Interface: ")
	fmt.Println(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{}))
	fmt.Println()

	//Populating cluster dynamic in cluster structure
clusterCurrent.ClusterDynamic.ClusterStatus = clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["ClusterStatus"].(string)
	clusterCurrent.ClusterDynamic.NumActiveDataNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActiveDataNodes"].(float64))
	clusterCurrent.ClusterDynamic.NumActivePrimaryShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActivePrimaryShards"].(float64))
	clusterCurrent.ClusterDynamic.NumActiveShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumActiveShards"].(float64))
	clusterCurrent.ClusterDynamic.NumInitializingShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumInitializingShards"].(float64))
	clusterCurrent.ClusterDynamic.NumMasterNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumMasterNodes"].(float64))
	clusterCurrent.ClusterDynamic.NumNodes = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumNodes"].(float64))
	clusterCurrent.ClusterDynamic.NumRelocatingShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumRelocatingShards"].(float64))
	clusterCurrent.ClusterDynamic.NumUnassignedShards = int(clusterLevelInfoInterface["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["NumUnassignedShards"].(float64))	
	return clusterCurrent
}

func main(){
	ctx:= context.Background()

	//create a configuration that is to be passed while creating the client 
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}

	//create the client using the configuration
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Println("Elasticsearch connection error:", err)
	}
	res, err := esClient.Info()
	if err != nil {
		log.Fatalf("client.Info() ERROR:", err)
	}
	fmt.Println("Response: ", res)

	responeStruct:=GetClusterCurrent(ctx,esClient)
	fmt.Println("Response Structure: ",responeStruct)

	responseJson, err := json.MarshalIndent(responeStruct,"","\t")
         if err != nil {
                fmt.Printf("Error: %s", err)
                 return;
         }
        fmt.Println()
        fmt.Println(string(responseJson))
        fmt.Println()

}
