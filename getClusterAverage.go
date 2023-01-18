package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"encoding/json"
	"strconv"
	elasticsearch "github.com/opensearch-project/opensearch-go"
	esapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

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

func getClusterAvgQuery(metricName string, decisionPeriod int)string{
	//nodesAvgQueryString:= `{"query":{"bool":{"filter":{"range":{"Timestamp":{"from": "now-`+strconv.Itoa(decisionPeriod)+`h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"node_statistics": {"terms": {"field": "HostIp.keyword","size": 100},"aggs": {`+metricName+`: { "stats": { "field":`+metricName+`} } }}}}`
	clusterAvgQueryString:=`{"query": {"bool": {"filter": {"range": {"Timestamp": {"from": "now-`+strconv.Itoa(decisionPeriod)+`h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"`+metricName+`": { "stats": { "field":"`+metricName+`"} }}}`
	return clusterAvgQueryString
}

func GetClusterAvg(metricName string, decisionPeriod int, ctx context.Context,esClient *elasticsearch.Client) MetricStatsCluster {
	//Create an object of MetricStatsCluster to populate and return
	var metricStatsCluster MetricStatsCluster

	//Get the query and convert to json
	var jsonQuery = []byte(getClusterAvgQuery(metricName,decisionPeriod))

	indexName:= []string{"monitor-stats-1"}

	//create a search request and pass the query
	searchQuery,err:= esapi.SearchRequest{
		Index: indexName,
		Body: bytes.NewReader(jsonQuery),
	}.Do(ctx,esClient)
	if err!=nil{
		fmt.Println("Cannot fetch cluster average: ",err)
		return metricStatsCluster
	}
	//Interface to dump the response
	var queryResultInterface map[string]interface{}

	//decode the response into the interface
	decodeErr := json.NewDecoder(searchQuery.Body).Decode(&queryResultInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeErr)
		return metricStatsCluster
	}

	fmt.Println("Response Map")
	fmt.Println(queryResultInterface)
	//Parse the interface and populate the metricStatsCluster
	metricStatsCluster.MetricName = metricName
	metricStatsCluster.ClusterLevel.Avg = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["avg"].(float64))
	metricStatsCluster.ClusterLevel.Max = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["max"].(float64))
	metricStatsCluster.ClusterLevel.Min = float32(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["min"].(float64))
	return metricStatsCluster
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

	responeStruct:=GetClusterAvg("RamUtil",300,ctx,esClient)
	fmt.Println("Response Structure: ",responeStruct)
	 responseJson, err := json.MarshalIndent(responeStruct,"","\t")
    	if err != nil {
        	fmt.Printf("Error: %s", err)
        	return;
   	 }
	 fmt.Println()
	 fmt.Println()
   	 fmt.Println(string(responseJson))

}