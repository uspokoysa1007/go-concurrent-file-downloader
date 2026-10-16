package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type PartDownload struct {
	Index int
	Start int64
	End   int64
	Path  string
}

func downloadChunk(url string, part PartDownload, wg *sync.WaitGroup, progressChan chan<- int64) {
	defer wg.Done()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("[-] Worker %d request error: %v\n", part.Index, err)
		return
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", part.Start, part.End)
	req.Header.Set("Range", rangeHeader)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[-] Worker %d download error: %v\n", part.Index, err)
		return
	}
	defer resp.Body.Close()

	out, err := os.Create(part.Path)
	if err != nil {
		fmt.Printf("[-] Worker %d file error: %v\n", part.Index, err)
		return
	}
	defer out.Close()

	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
			progressChan <- int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}
}

func runDownload(targetURL, outputPath string, workers int) {
	fmt.Printf("[*] Querying target: %s\n", targetURL)
	resp, err := http.Head(targetURL)
	if err != nil {
		fmt.Printf("[-] Error checking URL: %v\n", err)
		return
	}

	contentLength := resp.ContentLength
	acceptRanges := resp.Header.Get("Accept-Ranges")

	fmt.Printf("[+] Content Length: %d bytes (%.2f MB)\n", contentLength, float64(contentLength)/(1024*1024))
	fmt.Printf("[+] Range Support : %s\n", acceptRanges)

	if contentLength <= 0 || acceptRanges != "bytes" {
		fmt.Println("[!] Server does not support ranges. Falling back to single-stream download...")
		workers = 1
	}

	chunkSize := contentLength / int64(workers)
	var parts []PartDownload
	tempDir, _ := os.MkdirTemp("", "godownload_*")
	defer os.RemoveAll(tempDir)

	for i := 0; i < workers; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1
		if i == workers-1 {
			end = contentLength - 1
		}
		partPath := filepath.Join(tempDir, fmt.Sprintf("part_%d.tmp", i))
		parts = append(parts, PartDownload{
			Index: i,
			Start: start,
			End:   end,
			Path:  partPath,
		})
	}

	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)
	var totalDownloaded int64
	doneChan := make(chan bool)

	// Progress tracker goroutine
	go func() {
		for bytes := range progressChan {
			totalDownloaded += bytes
			pct := float64(totalDownloaded) / float64(contentLength) * 100.0
			fmt.Printf("\r[*] Downloading: %.1f%% (%d / %d bytes)", pct, totalDownloaded, contentLength)
		}
		doneChan <- true
	}()

	startTime := time.Now()
	for _, part := range parts {
		wg.Add(1)
		go downloadChunk(targetURL, part, &wg, progressChan)
	}

	wg.Wait()
	close(progressChan)
	<-doneChan

	// Merge chunks
	fmt.Println("\n[*] Merging chunks into final output file...")
	outFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Printf("[-] Failed to create output file: %v\n", err)
		return
	}
	defer outFile.Close()

	for _, part := range parts {
		partFile, err := os.Open(part.Path)
		if err != nil {
			fmt.Printf("[-] Error reading chunk %s: %v\n", part.Path, err)
			return
		}
		io.Copy(outFile, partFile)
		partFile.Close()
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[+] Download completed successfully in %v!\n", elapsed)
	fmt.Printf("[+] Saved as: %s\n", outputPath)
}

func main() {
	urlFlag := flag.String("url", "", "Target URL to download")
	outFlag := flag.String("out", "downloaded_file.bin", "Output file path")
	concurrencyFlag := flag.Int("concurrency", 4, "Number of concurrent download threads")
	demoFlag := flag.Bool("demo", false, "Run local demo benchmark")
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("             GO CONCURRENT FILE DOWNLOADER                      ")
	fmt.Println("================================================================")

	if *demoFlag || *urlFlag == "" {
		fmt.Println("[*] Launching local HTTP server for range download demonstration...")
		mockData := make([]byte, 5*1024*1024) // 5 MB mock file
		for i := range mockData {
			mockData[i] = byte(i % 256)
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(mockData)))

			rangeHdr := r.Header.Get("Range")
			if rangeHdr != "" {
				var start, end int64
				fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
				if end >= int64(len(mockData)) {
					end = int64(len(mockData)) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(mockData)))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(mockData[start : end+1])
			} else {
				w.Write(mockData)
			}
		}))
		defer server.Close()

		runDownload(server.URL, *outFlag, *concurrencyFlag)
	} else {
		runDownload(*urlFlag, *outFlag, *concurrencyFlag)
	}
	fmt.Println("================================================================")
}
