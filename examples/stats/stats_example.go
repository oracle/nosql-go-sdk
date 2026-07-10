//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// The stats example shows how to enable Java-compatible client-side statistics
// and view the emitted JSON in terminal output.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/kvstore"
	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var (
	configMode         = flag.String("config", "cloudsim", "Configuration to use: cloud, cloudsim, or onprem.")
	configFile         = flag.String("configFile", "", "Configuration file for cloud IAM or secure on-premise.")
	profileArg         = flag.String("profile", "MORE", "Stats profile: NONE, REGULAR, MORE, or ALL.")
	interval           = flag.Int("interval", 5, "Stats interval in seconds.")
	pretty             = flag.Bool("pretty", true, "Pretty-print stats JSON.")
	enableLog          = flag.Bool("statsLog", true, "Log stats JSON to stdout.")
	insecureSkipVerify = flag.Bool(
		"insecureSkipVerify",
		false,
		"Skip TLS server certificate verification (unsafe; local testing only).",
	)
)

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <endpoint-or-region>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	profile, err := nosqldb.ParseStatsProfile(*profileArg)
	if err != nil {
		fmt.Println(err)
		return
	}

	client, err := createClient(flag.Arg(0), profile)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	if err = runWorkload(client); err != nil {
		fmt.Println(err)
	}
}

func createClient(endpoint string, profile nosqldb.StatsProfile) (*nosqldb.Client, error) {
	var provider nosqldb.AuthorizationProvider
	var err error

	switch *configMode {
	case "cloudsim":
		provider = &cloudsim.AccessTokenProvider{TenantID: "ExampleTenantId"}
	case "onprem":
		if *configFile != "" {
			provider, err = kvstore.NewAccessTokenProviderFromFile(*configFile)
		}
	case "cloud":
		if *configFile == "" {
			*configFile = "~/.oci/config"
		}
		provider, err = iam.NewSignatureProviderFromFile(*configFile, "", "", "")
	default:
		return nil, fmt.Errorf("unsupported configuration %q", *configMode)
	}
	if err != nil {
		return nil, err
	}

	cfg := nosqldb.Config{
		Mode:                  *configMode,
		AuthorizationProvider: provider,
		StatsProfile:          profile,
		StatsInterval:         time.Duration(*interval) * time.Second,
		StatsPrettyPrint:      *pretty,
		StatsEnableLog:        enableLog,
		LoggingConfig: nosqldb.LoggingConfig{
			Logger: logger.New(os.Stdout, logger.Info, false),
		},
	}
	cfg.InsecureSkipVerify = *insecureSkipVerify

	if *configMode == "cloud" {
		region, regionErr := common.StringToRegion(endpoint)
		if regionErr == nil {
			cfg.Region = region
		} else {
			cfg.Endpoint = endpoint
		}
	} else {
		cfg.Endpoint = endpoint
	}

	return nosqldb.NewClient(cfg)
}

func runWorkload(client *nosqldb.Client) error {
	tableName := "go_stats_example"
	createReq := &nosqldb.TableRequest{
		Statement: "CREATE TABLE IF NOT EXISTS " + tableName +
			" (id INTEGER, name STRING, PRIMARY KEY(id))",
		TableLimits: nosqldb.ProvisionedTableLimits(50, 50, 1),
	}
	if _, err := client.DoTableRequestAndWait(createReq, 60*time.Second, time.Second); err != nil {
		return fmt.Errorf("create table: %v", err)
	}
	fmt.Println("Created table", tableName)

	for i := 0; i < 5; i++ {
		row := &types.MapValue{}
		row.Put("id", i)
		row.Put("name", fmt.Sprintf("row-%d", i))
		if _, err := client.Put(&nosqldb.PutRequest{
			TableName: tableName,
			Value:     row,
		}); err != nil {
			return fmt.Errorf("put row %d: %v", i, err)
		}
	}
	fmt.Println("Put workload completed")

	key := &types.MapValue{}
	key.Put("id", 1)
	if _, err := client.Get(&nosqldb.GetRequest{
		TableName: tableName,
		Key:       key,
	}); err != nil {
		return fmt.Errorf("get row: %v", err)
	}
	fmt.Println("Get workload completed")

	queryReq := &nosqldb.QueryRequest{
		Statement: "SELECT * FROM " + tableName,
	}
	queryCount := 0
	for {
		queryRes, err := client.Query(queryReq)
		if err != nil {
			return fmt.Errorf("query rows: %v", err)
		}
		rows, err := queryRes.GetResults()
		if err != nil {
			return fmt.Errorf("read query rows: %v", err)
		}
		queryCount += len(rows)
		if queryReq.IsDone() {
			break
		}
	}
	fmt.Printf("Query workload completed, rows=%d\n", queryCount)

	if _, err := client.Delete(&nosqldb.DeleteRequest{
		TableName: tableName,
		Key:       key,
	}); err != nil {
		return fmt.Errorf("delete row: %v", err)
	}
	fmt.Println("Delete workload completed")

	wait := time.Duration(*interval+1) * time.Second
	fmt.Printf("Waiting %s for periodic stats output...\n", wait)
	time.Sleep(wait)

	dropReq := &nosqldb.TableRequest{Statement: "DROP TABLE IF EXISTS " + tableName}
	if _, err := client.DoTableRequestAndWait(dropReq, 60*time.Second, time.Second); err != nil {
		return fmt.Errorf("drop table: %v", err)
	}
	fmt.Println("Dropped table", tableName)

	return nil
}
