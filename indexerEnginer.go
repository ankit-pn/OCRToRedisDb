package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"log"
	"github.com/redis/go-redis/v9"
	"context"
	"sync"
	"github.com/otiai10/gosseract/v2"
	"encoding/json"
	_ "net/http/pprof"
)


type File struct {
	FileID string
	ParentFolder string
	FileData string
}



const MaxWorkers = 12
const batchSize = 5
var ctx = context.Background()
func worker(filesChan <-chan string, wg *sync.WaitGroup, rdb *redis.Client, ctx context.Context, errChan chan<- error) {
    defer wg.Done()
	// Initialize the gosseract client once per worker
	client := gosseract.NewClient()
	defer client.Close()
	client.SetLanguage("eng", "hin", "urd") // Set the languages once
    for path := range filesChan {
		fmt.Println("Processing file:", path)
		fileData, err := extractTextWithClient(client, path) // Use the persistent client
		if err != nil {
			errChan <- fmt.Errorf("error extracting text from %q: %w", path, err)
			continue
		}
		parentFolder := filepath.Dir(path)
		fullFileName := filepath.Base(path)
		fileBaseName := strings.TrimSuffix(fullFileName, filepath.Ext(fullFileName))

		file := File{
			FileID: fileBaseName,
			ParentFolder: parentFolder,
			FileData: fileData,
		}
		err = SetKey(rdb, fileBaseName, file)
		if err != nil {
			fmt.Println("Failed to set value in Redis:", err)
			return
		}
		
	}

}

func GetKey(rdb *redis.Client, key string) (string, error) {
    val, err := rdb.Get(ctx, key).Result()
    if err == redis.Nil {
        return "", nil // Key does not exist
    } else if err != nil {
        return "", err // Some other error
    }
    return val, nil
}

func SetKey(rdb *redis.Client, key string, value interface{}) error {
    // Convert your 'value' to a string that Redis can store
    jsonData, err := json.Marshal(value)
    if err != nil {
        return err
    }

    // Use the string(jsonData) as the value for the Redis set command
    return rdb.Set(ctx, key, string(jsonData), 0).Err()
}



func indexerEngine(rootPath string) {
	
	rdb:=redis.NewClient(&redis.Options{
		Addr : "localhost:6390",
		Password: "",
		DB: 0,
	})


    filesChan := make(chan string, MaxWorkers)
    var wg sync.WaitGroup
    ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, MaxWorkers)

	go func() {
        for err := range errChan {
            log.Println("Error received:", err)
        }
    }()
    // Start workers
    for i := 0; i < MaxWorkers; i++ {
        wg.Add(1)
        go worker(filesChan, &wg, rdb, ctx,errChan)
    }



    // Walking the directory structure and sending files to the channel
    walkErr := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            fmt.Printf("error accessing path %q: %v\n", path, err)
            return err
        }
		fullFileName := info.Name()
		fileBaseName := strings.TrimSuffix(fullFileName, filepath.Ext(fullFileName))
		_,err = GetKey(rdb, fileBaseName)
        if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".jpeg") && err != nil {
            filesChan <- path
        }
        return nil
    })

    if walkErr != nil {
        fmt.Printf("error walking the path %v: %v\n", rootPath, walkErr)
    }

    close(filesChan)
    wg.Wait()
	close(errChan) 
    cancel() // Cancel the context to free resources
}