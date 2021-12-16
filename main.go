package main

import (
	"bufio"
	"fmt"
	"github.com/nyudlts/go-aspace"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type ResourceInfo struct {
	ID          int
	RepoID      int
	Identifiers string
	Title       string
	EADID       string
}

var resourceInfo = []ResourceInfo{}
var client *aspace.ASClient
var err error
var workers = 4
var resourceMap map[string]ResourceInfo

func main() {
	client, err = aspace.NewClient("/etc/sysconfig/go-aspace.yml", "prod", 20)
	if err != nil {
		panic(err)
	}
	//getResourceInfo()
	exportMarc()
}

func exportMarc() {
	resourceMap = make(map[string]ResourceInfo)
	input, _ := os.Open("resources.tsv")
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()

		split := strings.Split(line, "\t")

		repoID, _ := strconv.Atoi(split[0])
		resourceID, _ := strconv.Atoi(split[1])

		resourceInfo := ResourceInfo{
			ID:          resourceID,
			RepoID:      repoID,
			Identifiers: split[2],
			Title:       split[3],
		}
		resourceMap[resourceInfo.Identifiers] = resourceInfo

	}

	resourceListFile, _ := os.Open("resources.txt")
	scanner = bufio.NewScanner(resourceListFile)
	for scanner.Scan() {
		resourceID := scanner.Text()
		r, err := FindResourceInfo(resourceID)
		if err != nil {
			fmt.Println(resourceID, err.Error())
			continue
		}

		endpoint := fmt.Sprintf("/repositories/%d/resources/marc21/%d.xml", r.RepoID, r.ID)

		if err != nil {
			fmt.Println(resourceID, err.Error())
			continue
		}

		if r.EADID == "" {
			r.EADID = r.Identifiers
		}

		fmt.Println("exporting", r.EADID)
		marcBytes, err := client.GetEndpoint(endpoint)
		marcFileName := fmt.Sprintf("%s_20211216.xml", r.EADID)
		if err = ioutil.WriteFile(marcFileName, marcBytes, 0777); err != nil {
			fmt.Println(err)
		}

	}

}

func FindResourceInfo(resourceID string) (ResourceInfo, error) {
	for k, v := range resourceMap {
		if k == resourceID {
			return v, nil
		}
	}
	return ResourceInfo{}, fmt.Errorf("NOT FOUND")
}

func getResourceInfo() {
	repositories := []int{2, 3, 6}

	for _, i := range repositories {
		resources, err := client.GetResourceIDs(i)
		if err != nil {
			panic(err)
		}
		for _, j := range resources {
			resourceInfo = append(resourceInfo, ResourceInfo{
				ID:          j,
				RepoID:      i,
				Identifiers: "",
				Title:       "",
			})
		}
	}

	chunkedResources := chunkResources()

	resultChannel := make(chan []ResourceInfo)
	for i, chunk := range chunkedResources {
		go getIdentifiers(chunk, resultChannel, i+1)
	}

	results := []ResourceInfo{}
	for range chunkedResources {
		chunk := <-resultChannel
		log.Println("INFO Adding", len(chunk), "uris to uri list")
		results = append(results, chunk...)
	}

	output, _ := os.Create("resources.tsv")
	writer := bufio.NewWriter(output)

	for _, r := range results {
		writer.WriteString(fmt.Sprintf("%d\t%d\t%s\t%s\t%s\n", r.RepoID, r.ID, r.Identifiers, r.Title, r.EADID))
		writer.Flush()
	}
}

func getIdentifiers(resourceChunk []ResourceInfo, resultChannel chan []ResourceInfo, workerID int) {
	numResources := len(resourceChunk)
	log.Printf("Worker %d starting, processing %d resources", workerID, numResources)
	results := []ResourceInfo{}
	for i, r := range resourceChunk {
		resource, err := client.GetResource(r.RepoID, r.ID)
		if err != nil {
			continue
		}
		r.Identifiers = resource.MergeIDs()
		r.Title = resource.Title
		r.EADID = resource.EADID
		results = append(results, r)
		if i > 0 && i%100 == 0 {
			log.Printf("Worker %d completed %d 0f %d resources", workerID, i, numResources)
		}
	}
	log.Printf("Worker %d finished", workerID)
	resultChannel <- results

}

func chunkResources() [][]ResourceInfo {
	var divided [][]ResourceInfo

	chunkSize := (len(resourceInfo) + workers - 1) / workers

	for i := 0; i < len(resourceInfo); i += chunkSize {
		end := i + chunkSize

		if end > len(resourceInfo) {
			end = len(resourceInfo)
		}

		divided = append(divided, resourceInfo[i:end])
	}
	return divided
}
