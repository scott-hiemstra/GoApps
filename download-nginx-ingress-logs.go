package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
)

/*
Change the values on lines 28-33 to match your environment.
By default, this program will download logs from the last day.
You can change the number of days back by using the -days flag.
*/

func main() {
	// Command-line flags
	var daysBack int
	flag.IntVar(&daysBack, "days", 1, "Number of days back to download logs")
	flag.Parse()

	esURL := "https://ES_URL.DOMAINNAME.COM:9200" // Change to your Elasticsearch URL
	apiKey := "YOUR_ES_API_KEY"                   // Change to your API key
	indexName := "YOUR_ES_INDEX_NAME"             // Change to your index name, can include an *
	numThreads := 4                               // Number of concurrent threads
	logStoreDir := "logdir"                       // Directory to store logs
	urlDomain := "URL_FROM_ES_url.domain_FIELD"   // URL domain to filter

	// Ensure directory exists or create it
	err := os.MkdirAll(logStoreDir, 0755)
	if err != nil {
		log.Fatalf("Error creating directory: %s", err)
	}

	// Create an HTTP header with the API key
	headers := http.Header{}
	headers.Set("Authorization", "ApiKey "+apiKey)

	// Elasticsearch client with custom HTTP headers and increased timeout
	client, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetHeaders(headers),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetHttpClient(&http.Client{
			Timeout: 60 * time.Second,
		}),
	)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Query for the specified number of days back
	daysAgo := time.Now().AddDate(0, 0, -daysBack)
	query := elastic.NewBoolQuery().
		Filter(elastic.NewRangeQuery("@timestamp").Gte(daysAgo.Format(time.RFC3339))).
		Filter(elastic.NewTermQuery("url.domain", urlDomain))

	// Estimate total hits
	countQuery := client.Count(indexName).Query(query)
	countResult, err := countQuery.Do(context.Background())
	if err != nil {
		log.Fatalf("Error estimating total hits: %s", err)
	}

	// Total number of hits
	totalHits := countResult

	// Initialize variables for pagination and concurrency
	scroll := client.Scroll(indexName).Query(query).Size(1000) // Adjust size as needed
	ctx := context.Background()
	var wg sync.WaitGroup
	hitCh := make(chan *elastic.SearchHit)
	done := make(chan struct{})
	var processedHits int64 // Track processed hits

	// Function to process hits
	processHits := func() {
		defer wg.Done()
		localProcessedHits := 0 // Track processed hits per goroutine
		for hit := range hitCh {
			localProcessedHits++
			processedHits++ // Increment global processed hits count

			// Unmarshal hit.Source into a map[string]interface{}
			var source map[string]interface{}
			if err := json.Unmarshal(hit.Source, &source); err != nil {
				log.Printf("Error unmarshaling document ID %s: %s", hit.Id, err)
				continue
			}

			// Extract timestamp and format it by hour
			timestampStr, ok := source["@timestamp"].(string)
			if !ok {
				log.Printf("Document ID %s: '@timestamp' field is not a string", hit.Id)
				continue
			}
			timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
			if err != nil {
				log.Printf("Error parsing timestamp for document ID %s: %s", hit.Id, err)
				continue
			}
			hourlyFileName := timestamp.Format("2006-01-02-15") + ".txt" // Format: YYYY-MM-DD-HH
			filePath := filepath.Join(logStoreDir, hourlyFileName)

			// Open or create the hourly file
			file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Printf("Error opening file %s: %s", filePath, err)
				continue
			}

			// Write "message" (or any other desired field) to file
			message, ok := source["message"].(string)
			if !ok {
				log.Printf("Document ID %s: 'message' field is not a string", hit.Id)
				continue
			}
			if _, err := file.WriteString(fmt.Sprintf("%s\n", message)); err != nil {
				log.Printf("Error writing to file %s: %s", filePath, err)
			}

			// Close file immediately after writing
			if err := file.Close(); err != nil {
				log.Printf("Error closing file %s: %s", filePath, err)
			}
		}
		log.Printf("Processed %d hits", localProcessedHits)
	}

	// Start workers
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go processHits()
	}

	// Process hits and send to channel
	go func() {
		defer close(hitCh)

		for {
			results, err := scroll.Do(ctx)
			if err != nil {
				log.Printf("Error scrolling: %s", err)
				break
			}

			if len(results.Hits.Hits) == 0 {
				break
			}

			for _, hit := range results.Hits.Hits {
				hitCh <- hit
			}
		}

		// Signal done
		close(done)
	}()

	// Wait for processing to complete
	go func() {
		wg.Wait()
		close(hitCh)
	}()

	// Print progress and completion message
	startTime := time.Now()
	for {
		select {
		case <-done:
			log.Printf("Data has been written to hourly files in subdirectory %s", logStoreDir)
			return
		case <-time.After(10 * time.Second):
			log.Printf("Processed %d out of %d hits in %s", processedHits, totalHits, time.Since(startTime))
		}
	}
}
