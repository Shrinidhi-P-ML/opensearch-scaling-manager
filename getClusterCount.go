package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	elasticsearch "github.com/opensearch-project/opensearch-go"
	esapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

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

func getClusterCountQuery(metricName string, decisionPeriod int, limit float32)string{
	clusterCountQueryString:= `{"query": {"bool": {"filter":{"range": {"Timestamp": {"from": "now-`+strconv.Itoa(decisionPeriod)+`h","include_lower": true,"include_upper": true,"to": null}}}}},"aggs": {"`+metricName+`": { "range": { "field": "`+metricName+`" , "ranges": [{"from":`+strconv.FormatFloat(float64(limit), 'E', -1, 32)+`, "to":null}] }}}}`
	return clusterCountQueryString
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

func GetClusterCount(metricName string, decisionPeriod int, limit float32,ctx context.Context, esClient *elasticsearch.Client) MetricViolatedCountCluster {
	var metricViolatedCount MetricViolatedCountCluster

	//Get the query and convert to json
	var jsonQuery = []byte(getClusterCountQuery(metricName,decisionPeriod,limit))

	indexName:= []string{"monitor-stats-1"}

	//create a search request and pass the query
	searchQuery,err:= esapi.SearchRequest{
		Index: indexName,
		Body: bytes.NewReader(jsonQuery),
	}.Do(ctx,esClient)
	if err!=nil{
		fmt.Println("Cannot fetch cluster average: ",err)
		return metricViolatedCount
	}

	//Interface to dump the response
	var queryResultInterface map[string]interface{}

	//decode the response into the interface
	decodeErr := json.NewDecoder(searchQuery.Body).Decode(&queryResultInterface)
	if decodeErr != nil {
		fmt.Println("decode Error: ", decodeErr)
		return metricViolatedCount
	}
 	fmt.Println()
	fmt.Println("Response Map: ")
	fmt.Println(queryResultInterface)
	fmt.Println()
	//Parse the interface and populate the metricStatsCluster
	metricViolatedCount.MetricName = metricName
	metricViolatedCount.ClusterLevel.ViolatedCount = int(queryResultInterface["aggregations"].(map[string]interface{})[metricName].(map[string]interface{})["buckets"].([]interface{})[0].(map[string]interface{})["doc_count"].(float64))
	
	return metricViolatedCount
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

	responeStruct:=GetClusterCount("RamUtil",300,20,ctx,esClient)
	responseJson, err := json.MarshalIndent(responeStruct,"","\t")
   	 if err != nil {
        	fmt.Printf("Error: %s", err)
       		 return;
   	 }
	fmt.Println()
        fmt.Println(string(responseJson))
	fmt.Println()
	fmt.Println("Response Structure: ",responeStruct)
}
