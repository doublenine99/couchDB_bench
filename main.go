package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-kivik/couchdb/v4" // The CouchDB driver
	kivik "github.com/go-kivik/kivik/v4"
)

var (
	db *kivik.DB
)

type Doc struct {
	ID         string `json:"_id"`
	Rev        string `json:"_rev,omitempty"`
	Val        string `json:"val,omitempty"`
	UpdateTime int64  `json:"update_time,omitempty"`
}

const valueSize = 100

func runAndGetAverage(rounds int, requestCnt int, fn func(int) time.Duration, measureName string) {
	total := time.Duration(0)
	for i := 0; i <= rounds; i++ {
		if i == 0 {
			// warm up
			fn(requestCnt)
			continue
		}
		total += fn(requestCnt)
	}
	log.Printf("%s average time cost: %v\n", measureName, total/time.Duration(rounds))
}

func init() {
	client, err := kivik.New("couch", "http://admin:password@128.110.218.106:5984/")
	if err != nil {
		log.Fatal(err)
	}
	db = client.DB("test_db")
	if db == nil {
		log.Fatal("db is nil")
	}
}
func main() {
	// setup(100000, true)
	rounds := 10
	// runAndGetAverage(rounds, 1, sequentialCheckAndUpdate, "sequentialCheckAndUpdate 1 key values")
	// runAndGetAverage(rounds, 10, sequentialCheckAndUpdate, "sequentialCheckAndUpdate 10 key values")
	// runAndGetAverage(rounds, 100, sequentialCheckAndUpdate, "sequentialCheckAndUpdate 100 key values")
	// runAndGetAverage(rounds, 1000, sequentialCheckAndUpdate, "sequentialCheckAndUpdate 1000 key values")
	// runAndGetAverage(rounds, 2048, sequentialCheckAndUpdate, "sequentialCheckAndUpdate 2048 key values")

	// measureing concurrent update
	// runAndGetAverage(rounds, 1, concurrentCheckAndUpdate, "concurrentCheckAndUpdate 1 key values")
	// runAndGetAverage(rounds, 10, concurrentCheckAndUpdate, "concurrentCheckAndUpdate 10 key values")
	// runAndGetAverage(rounds, 100, concurrentCheckAndUpdate, "concurrentCheckAndUpdate 100 key values")
	// runAndGetAverage(rounds, 1000, concurrentCheckAndUpdate, "concurrentCheckAndUpdate 1000 key values")
	// runAndGetAverage(rounds, 2048, concurrentCheckAndUpdate, "concurrentCheckAndUpdate 2048 key values")

	// runAndGetAverage(rounds, 1, concurrentGet, "concurrentGet 1 key values")
	// runAndGetAverage(rounds, 10, concurrentGet, "concurrentGet 10 key values")
	// runAndGetAverage(rounds, 100, concurrentGet, "concurrentGet 100 key values")
	// runAndGetAverage(rounds, 1000, concurrentGet, "concurrentGet 1000 key values")
	// runAndGetAverage(rounds, 2048, concurrentGet, "concurrentGet 2048 key values")

	// measureing bulk update
	runAndGetAverage(rounds, 1, bulkCheckAndUpdate, "bulkCheckAndUpdate 1 key values")
	runAndGetAverage(rounds, 10, bulkCheckAndUpdate, "bulkCheckAndUpdate 10 key values")
	runAndGetAverage(rounds, 100, bulkCheckAndUpdate, "bulkCheckAndUpdate 100 key values")
	// runAndGetAverage(rounds, 1000, bulkCheckAndUpdate, "bulkCheckAndUpdate 1000 key values")
	// runAndGetAverage(rounds, 2048, bulkCheckAndUpdate, "bulkCheckAndUpdate 2048 key values")

	// runAndGetAverage(rounds, 1, bulkGet, "bulkGet 1 key values")
	// runAndGetAverage(rounds, 10, bulkGet, "bulkGet 10 key values")
	// runAndGetAverage(rounds, 100, bulkGet, "bulkGet 100 key values")
	// runAndGetAverage(rounds, 1000, bulkGet, "bulkGet 1000 key values")
	// runAndGetAverage(rounds, 2048, bulkGet, "bulkGet 2048 key values")
	// createFullView()
	// measureing full records
	// runAndGetAverage(rounds, 10, fullRecords, "fullRecords 10 key values")
	// runAndGetAverage(rounds, 100, fullRecords, "fullRecords 100 key values")
	// runAndGetAverage(rounds, 1000, fullRecords, "fullRecords 1000 key values")
	// runAndGetAverage(rounds, 10000, fullRecords, "fullRecords 10000 key values")
	// runAndGetAverage(rounds, 100000, fullRecords, "fullRecords 100000 key values")
	// runAndGetAverage(rounds, 10, partialRecords, "partialRecords 10 key values")
	// runAndGetAverage(rounds, 100, partialRecords, "partialRecords 100 key values")
	// runAndGetAverage(rounds, 1000, partialRecords, "partialRecords 1000 key values")
	// runAndGetAverage(rounds, 10000, partialRecords, "partialRecords 10000 key values")
	// runAndGetAverage(rounds, 100000, partialRecords, "partialRecords 100000 key values")
	// createView()

}

var now = time.Now().Unix() + 3600

func isExpired(doc *Doc) bool {
	return true
}

func setup(docCnt int, clear bool) {
	docs := make([]interface{}, 0, docCnt)
	for i := 0; i < docCnt; i++ {
		docs = append(docs, &Doc{
			ID:         strconv.Itoa(i),
			Val:        strings.Repeat("X", valueSize),
			UpdateTime: now,
		})
	}
	_, err := db.BulkDocs(context.TODO(), docs)
	if err != nil {
		log.Fatal(err)
	}
}

func concurrentCheckAndUpdate(requestCnt int) time.Duration {
	ids := make([]string, 0, requestCnt)
	for i := 0; i < requestCnt; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	wg := sync.WaitGroup{}
	wg.Add(len(ids))

	start := time.Now()
	for _, id := range ids {
		go func(id string) {
			defer wg.Done()
			res := db.Get(context.TODO(), id)
			doc := &Doc{ID: id}
			err := res.ScanDoc(doc)
			if err != nil {
				// log.Fatal(err)
			}
			// log.Println(doc)
			if isExpired(doc) {
				doc.UpdateTime = now
				doc.Val = strings.Repeat("X", valueSize)
				_, _ = db.Put(context.TODO(), id, doc)
			}
		}(id)
	}
	wg.Wait()
	return time.Since(start)
}

func sequentialCheckAndUpdate(requestCnt int) time.Duration {
	ids := make([]string, 0, requestCnt)
	for i := 0; i < requestCnt; i++ {
		ids = append(ids, strconv.Itoa(i))
	}
	start := time.Now()
	{
		for _, id := range ids {
			res := db.Get(context.TODO(), id)
			doc := &Doc{ID: id}
			err := res.ScanDoc(doc)
			if err != nil {
				log.Fatal(err)
			}
			if isExpired(doc) {
				doc.UpdateTime = now
				doc.Val = strings.Repeat("X", valueSize)
				_, _ = db.Put(context.TODO(), id, doc)
			}
		}
	}
	return time.Since(start)
}

func bulkGet(requestCnt int) time.Duration {
	ids := make([]string, 0, requestCnt)
	for i := 0; i < requestCnt; i++ {
		ids = append(ids, strconv.Itoa(i))
	}
	bulks := make([]kivik.BulkGetReference, 0, len(ids))
	for _, id := range ids {
		bulks = append(bulks, kivik.BulkGetReference{ID: id})
	}
	start := time.Now()
	res := db.BulkGet(context.TODO(), bulks)
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
	return time.Since(start)
}

func concurrentGet(requestCnt int) time.Duration {
	ids := make([]string, 0, requestCnt)
	for i := 0; i < requestCnt; i++ {
		ids = append(ids, strconv.Itoa(i))
	}
	wg := sync.WaitGroup{}
	wg.Add(len(ids))
	start := time.Now()
	for _, id := range ids {
		go func(id string) {
			defer wg.Done()
			db.Get(context.TODO(), id)
		}(id)
	}
	wg.Wait()
	return time.Since(start)
}

func bulkCheckAndUpdate(requestCnt int) time.Duration {
	ids := make([]string, 0, requestCnt)
	for i := 0; i < requestCnt; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	start := time.Now()
	{
		bulks := make([]kivik.BulkGetReference, 0, len(ids))
		for _, id := range ids {
			bulks = append(bulks, kivik.BulkGetReference{ID: id})
		}
		docs := make([]interface{}, 0, len(ids))
		res := db.BulkGet(context.TODO(), bulks)
		if res.Err() != nil {
			log.Fatal(res.Err())
		}
		for res.Next() {
			doc := &Doc{}
			err := res.ScanDoc(doc)
			if err != nil {
				log.Fatal(err)
			}
			if isExpired(doc) {
				doc.UpdateTime = now
				doc.Val = strings.Repeat("X", valueSize)
				docs = append(docs, doc)
			}
		}
		db.BulkDocs(context.TODO(), docs)
	}
	return time.Since(start)
}

func createFullView() {
	rev, err := db.Put(context.TODO(), "_design/fullrecords", map[string]interface{}{
		"_id": "_design/fullrecords",
		"views": map[string]interface{}{
			"fullrecords_view": map[string]interface{}{
				"map": "function(doc) {  emit(doc); }",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(rev)
}

func fullRecords(requestNum int) time.Duration {
	start := time.Now()
	res := db.Query(context.TODO(), "_design/fullrecords", "_view/fullrecords_view", map[string]interface{}{
		"limit": requestNum,
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
	for res.Next() {
		res.ID()
	}
	return time.Since(start)
}

func createView() {
	rev, err := db.Put(context.TODO(), "_design/partialRecords", map[string]interface{}{
		"_id": "_design/partialRecords",
		"views": map[string]interface{}{
			"partial_view": map[string]interface{}{
				"map": "function(doc) { emit(doc._id);}",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(rev)
}

func partialRecords(requestNum int) time.Duration {
	start := time.Now()
	// defer func() { log.Println("partialRecords time cost:", time.Since(start)) }()
	// query 1000 records

	res := db.Query(context.TODO(), "_design/partialRecords", "_view/partial_view", map[string]interface{}{
		"limit": requestNum,
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
	for res.Next() {
		res.ID()
	}
	return time.Since(start)
}
