package main

import (
	"bufio"
	"container/list"
	"context"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/html"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type URL struct {
	URL string
	root string
	depth int
}

type Record struct {
	ID      primitive.ObjectID `bson:"_id,omitempty"`
	keyword string        	   `bson:"keyword,omitempty"`
	url     []Url         	   `bson:"url,omitempty"`
}

type Url struct {
	address string `bson:"address,omitempty"`
	freq 	int    `bson:"freq,omitempty"`
}

var ctx context.Context

func getTitle(pageContent string) []byte {
	titleStartIndex := strings.Index(pageContent, "<title>")
	if titleStartIndex == -1 {
		fmt.Println("No title element found")
	}
	// The start index of the title is the index of the first
	// character, the < symbol. We don't want to include
	// <title> as part of the final value, so let's offset
	// the index by the number of characers in <title>
	titleStartIndex += 7

	// Find the index of the closing tag
	titleEndIndex := strings.Index(pageContent, "</title>")
	if titleEndIndex == -1 {
		fmt.Println("No closing tag for title found.")
		return nil
	}

	pageTitle := []byte(pageContent[titleStartIndex:titleEndIndex])

	// Print out the result
	return pageTitle
}

func extractKeywords(pageText string, db *mongo.Database, url URL) {
	m1 := regexp.MustCompile(`[^a-z^A-Z^\d-_']`)
	pageText = m1.ReplaceAllString(pageText, " ")
	keywords := strings.Fields(pageText)
	for _, elem := range keywords {
		keyword, err := db.Collection("keyword").Find(ctx, bson.M{"keyword" : elem})
		if err != nil {
			log.Println(err)
			continue
		}
		var keywordsFiltered []Record
		if err = keyword.All(ctx, &keywordsFiltered); err != nil {
			log.Println(err)
			continue
		}
		if len(keywordsFiltered) == 0 {
			db.Collection("keyword").InsertOne(ctx, bson.D{
				{Key : "keyword", Value : elem},
				{Key : "url" , Value : bson.A{
					bson.D{
						{Key : "address", Value : url.URL},
						{Key: "freq", Value : 1},
					}},
				},
			})
		} else {
			result, err := db.Collection("keyword").UpdateOne(
				ctx,
				bson.M{
					"keyword" : elem,
					"url.address" : url.URL,
				},
				bson.D{
					{"$inc", bson.D{
						{"url.$.freq", 1},
					}},
				},
			)
			if err != nil {
				log.Println(err)
			}
			if result.MatchedCount == 0 {
				db.Collection("keyword").UpdateOne(
					ctx,
					bson.M{
						"keyword" : elem,
					},
					bson.M{
							"$push" : bson.M{
								"url" : bson.M{
									"address" : url.URL,
									"freq" : 1,
								},
							},
					},
				)
			}
		}
	}
}

func scrape(wg *sync.WaitGroup, db *mongo.Database, url URL, queue *list.List) {
	defer wg.Done()

	fmt.Printf("%s ", url.URL)
	response, err := http.Get(url.URL)
	if err != nil {
		log.Println(err)
		return
	}
	defer response.Body.Close()

	// Get the response body as a string
	dataInBytes, err := ioutil.ReadAll(response.Body)
	pageContent := string(dataInBytes)

	title := getTitle(pageContent)
	fmt.Println(string(title))

	node, err := html.Parse(strings.NewReader(pageContent))
	if err != nil {
		log.Println(err)
		return
	}
	document := goquery.NewDocumentFromNode(node)
	if err != nil {
		log.Println("Error loading HTTP response body. ", err)
		return
	}

	// Find all links and process them with the function
	// defined earlier
	document.Find("a").Each(func(index int, item *goquery.Selection) {
		href, exists := item.Attr("href")
		if exists {
			if len(href) != 0 && href[0] == '/' {
				href = "https:" + href
			}
			newURL := URL {
				href,
				url.root,
				url.depth + 1,
			}
			queue.PushBack(newURL)
		}
	})

	result := db.Collection("url").FindOne(ctx, bson.M{"url" : url.URL})

	if result.Err() != mongo.ErrNoDocuments {
		return
	}

	extractKeywords(document.Text(), db, url)

	db.Collection("url").InsertOne(ctx, bson.D{
		{"url",  url.URL},
	})
}

func urlBfs(db *mongo.Database, queue *list.List) {
	var wg sync.WaitGroup
	for queue.Len() > 0{
		currURL, err := queue.Remove(queue.Front()).(URL)
		if !err {
			log.Println(err)
			continue
		}
		if currURL.depth > 1 {
			continue
		}
		wg.Add(1)
		go scrape(&wg, db, currURL, queue)
		wg.Wait()
	}

}

func main() {
	clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	ctx := context.TODO()
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, nil)

	if err != nil {
		log.Fatal(err)
	}

	defer client.Disconnect(ctx)
	fmt.Println("Connected to MongoDB!")

	db := client.Database("spider")

	start := time.Now()
	values := list.New()

	f, err := os.Open("spider_root.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	s := bufio.NewScanner(f)
	for s.Scan() {
		newURL := URL{
			URL:   s.Text(),
			root: s.Text(),
			depth: 0,
		}
		values.PushBack(newURL)
	}
	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}

	urlBfs(db, values)
	elapsed := time.Since(start)
	log.Printf("Crawling took %s", elapsed)
}
