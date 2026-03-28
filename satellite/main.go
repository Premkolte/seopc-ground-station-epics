package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
    	"net/http"
)

// Config
var (
	minioEndpoint   = getEnv("MINIO_ENDPOINT", "localhost:9000")
	minioAccessID   = getEnv("MINIO_ROOT_USER", "admin")
	minioSecretKey  = getEnv("MINIO_ROOT_PASSWORD", "password123")
	minioBucket     = "satellite-raw"
	watchDir        = getEnv("WATCH_DIR", "../downlink_buffer")
	kafkaBroker     = getEnv("KAFKA_BROKER", "localhost:19092")
	kafkaTopic      = "eo-events"
)

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

type Event struct {
	EventID string `json:"event_id"`
	File    string `json:"file"`
}

func main() {
	// Start Prometheus metrics server on port 8080
    	go func() {
       	 	http.Handle("/metrics", promhttp.Handler())
        	http.ListenAndServe(":8080", nil)
    	}()
	// 1. MinIO Connection
	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessID, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln("MinIO connection failed:", err)
	}

	// Ensure bucket exists (with retry loop for MinIO startup)
	ctx := context.Background()
	maxRetries := 15
	for i := 0; i < maxRetries; i++ {
		err = minioClient.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{})
		if err == nil {
			log.Printf("Bucket %s created\n", minioBucket)
			break
		}
		
		exists, errBucketExists := minioClient.BucketExists(ctx, minioBucket)
		if errBucketExists == nil && exists {
			log.Printf("Bucket %s already exists\n", minioBucket)
			err = nil
			break
		}
		
		log.Printf("Waiting for MinIO to start (attempt %d/%d)...\n", i+1, maxRetries)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalln("Failed to connect to MinIO after retries:", err)
	}

	// 2. Kafka Connection (Redpanda)
	// We'll use a writer to produce messages
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// 3. File Watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					filename := filepath.Base(event.Name)
					log.Printf("New file detected: %s", filename)

					// Allow file write to complete/settle
					time.Sleep(100 * time.Millisecond)

					// Upload to MinIO
					objectName := filename
					filePath := event.Name
					contentType := "image/jpeg" // Assuming JPG for "filename.jpg"

					info, err := minioClient.FPutObject(ctx, minioBucket, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
					if err != nil {
						log.Printf("Failed to upload %s: %v", filename, err)
						continue
					}
					log.Printf("Uploaded %s to MinIO (Size: %d)\n", objectName, info.Size)

					// Send Event to Redpanda
					evt := Event{
						EventID: fmt.Sprintf("%d", time.Now().UnixNano()),
						File:    filename,
					}
					msgBytes, _ := json.Marshal(evt)

					err = writer.WriteMessages(ctx, kafka.Message{
						Key:   []byte(evt.EventID),
						Value: msgBytes,
					})
					if err != nil {
						log.Printf("Failed to write to kafka: %v", err)
					} else {
						log.Printf("Event sent to %s: %s", kafkaTopic, string(msgBytes))
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Watcher error:", err)
			}
		}
	}()

	err = watcher.Add(watchDir)
	if err != nil {
		// Try to create the directory if it doesn't exist?
		// User said "Watch directory ../downlink_buffer"
		// Assuming it exists because I saw it in list_dir, but for robustness:
		if os.IsNotExist(err) {
			log.Printf("Directory %s does not exist, creating it...", watchDir)
             // Using simple mkdir since we are in the satellite dir usually
            // but the path is relative. Let's fix path relative to CWD or assume user runs from satellite dir.
            // For now, let's just panic as robust handling handles 'files' primarily.
            // Actually, best to just log fatal if critical dir missing or create it.
             _ = os.MkdirAll(watchDir, 0755)
             err = watcher.Add(watchDir)
             if err != nil {
                 log.Fatal(err)
             }
		} else {
             log.Fatal(err)
        }
	}
	log.Printf("Watching %s for new files...", watchDir)
	<-done
}
