package elastic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

var client *esClient
var Store *store

type ErrorFn func(err error)
type Config struct {
	Host              string
	Port              int
	User              string
	Pass              string
	TLSConfig         *tls.Config
	StoreManager      func() (*store, error)
	OnConnectingError ErrorFn
	OnConnected       func()
}

type esClient struct {
	conn *elasticsearch.Client
}

var handleConnect func()
var handleError ErrorFn
var lastErr error

func genHandleError(inFn ErrorFn) ErrorFn {
	return func(err error) {
		if inFn != nil {
			var e1, e2 string
			if err != nil {
				e1 = err.Error()
			}
			if lastErr != nil {
				e2 = lastErr.Error()
			}
			if e1 == context.DeadlineExceeded.Error() {
				return
			}
			if e1 != e2 {
				if err != nil {
					inFn(err)
				}
			}
			lastErr = err
		}
	}
}

func (c *esClient) pingHandler(dur time.Duration) {
	var connected bool
	for {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			res, err := c.conn.Info(c.conn.Info.WithContext(ctx))
			if err != nil {
				connected = false
				handleError(err)
				log.Println("ping err : ", err)
				return
			}
			defer res.Body.Close()

			if handleConnect != nil && !connected {
				handleConnect()
				connected = true
			}

			handleError(nil)
		}()

		time.Sleep(dur)
	}
}

func Init(cnf *Config) error {
	if cnf == nil {
		cnf = &Config{
			Host: "localhost",
			Port: 9200,
			User: "elastic",
			Pass: "changeme",
		}
	}

	if client != nil {
		return nil
	}

	handleError = genHandleError(cnf.OnConnectingError)
	handleConnect = cnf.OnConnected

	var err error
	var r map[string]interface{}

	// load TLS config
	tlsConfig := cnf.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	cfg := elasticsearch.Config{

		Addresses: []string{
			fmt.Sprintf("http://%s:%d", cnf.Host, cnf.Port),
		},
		Username: cnf.User,
		Password: cnf.Pass,
		Transport: &http.Transport{
			DialContext:     (&net.Dialer{Timeout: time.Second * 3}).DialContext,
			//TLSClientConfig: tlsConfig,
		},
		// ...
	}
	esc, err := elasticsearch.NewClient(cfg)
	if err != nil {
		handleError(err)
		return fmt.Errorf("error in create elastic client : %s", err.Error())
	}

	res, err := esc.Info()
	if err != nil {
		handleError(err)
		return fmt.Errorf("error in connecting to elasticsearch : %s", err.Error())
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		return fmt.Errorf("Error: %s", res.String())
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return fmt.Errorf("Error parsing the response body: %s", err)
	}

	client = &esClient{
		conn: esc,
	}

	if cnf.StoreManager != nil {
		var err error
		Store, err = cnf.StoreManager()
		if err != nil {
			return fmt.Errorf("Error building store : %s", err.Error())
		}
	}

	go client.pingHandler(time.Second * 5)

	log.Println("connected to elasticserach")
	return nil
}

func Get(index string, id string) ([]byte, error) {
	var r map[string]interface{}
	req := esapi.GetRequest{
		Index:      index,
		DocumentID: id,
	}

	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		log.Fatalf("Eror gettingr response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error get document ID=%s", res.Status(), id)
		return nil, fmt.Errorf("Error get document ID=%s Status=%s", id, res.Status())
	} else {
		// Deserialize the response into a map.

		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
			return nil, err
		}
	}

	if _, ok := r["_source"]; !ok {
		return nil, fmt.Errorf("_source not found in response")
	}

	data, _ := json.Marshal(r["_source"])

	return data, nil
}

func QueryRaw(index []string, body string) (map[string]interface{}, error) {
	var r map[string]interface{}

	// Perform the search request.
	req := esapi.SearchRequest{
		Index: index,
		Body:  bytes.NewReader([]byte(body)),
		FilterPath: []string{"-_shards", "-took", "-timed_out",
			"-hits.total", "-hits.max_score", "-hits.hits._index",
			"-hits.hits._type", "-hits.hits._score"},
	}

	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		log.Fatalf("Eror query response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return getError(res), nil
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return nil, err
	}
	return r, nil
}

func UpdateByQuery(index []string, body map[string]interface{}) (map[string]interface{}, error) {
	var r map[string]interface{}
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	req := esapi.UpdateByQueryRequest{
		Index: index,
		Body:  &buf,
	}

	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		log.Fatalf("Eror query response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return getError(res), nil
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return nil, err
	}
	return r, nil
}

type BulkResponse struct {
	Responses []map[string]interface{}
	Error     map[string]interface{}
}

func BulkSerach(batchQuery string, indices []string) (BulkResponse, error) {

	req := esapi.MsearchRequest{
		Index: indices,
		Body:  bytes.NewReader([]byte(batchQuery)),
	}

	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		return BulkResponse{}, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return BulkResponse{
			Responses: nil,
			Error:     getError(res),
		}, nil
	}

	var result BulkResponse
	err = json.NewDecoder(res.Body).Decode(&result)

	return result, nil
}

func getError(res *esapi.Response) map[string]interface{} {
	if res.IsError() {
		errResult := map[string]interface{}{
			"error": map[string]interface{}{
				"reason": res.Status(),
				"status": res.StatusCode,
			},
		}
		return errResult
	}
	return nil
}

func Query(index []string, body string) ([]map[string]interface{}, error) {

	res, err := QueryRaw(index, body)
	if err != nil {
		return nil, err
	}

	// Print the ID and document source for each hit.
	var rows []map[string]interface{}
	for _, hit := range res["hits"].(map[string]interface{})["hits"].([]interface{}) {
		row := hit.(map[string]interface{})["_source"].(map[string]interface{})
		rows = append(rows, row)
	}

	return rows, nil
}

func Index(index string, data []byte, id string) error {
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
		// Pretty:     true,
		// Timeout:    100,
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document ID=%s", res.Status(), id)
		return fmt.Errorf("Error indexing document : %s", res.Status())
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
			return err
		}
	}

	return nil
}

func DeleteIndex(index string) error {

	// Perform the request with the client.
	_, err := client.conn.Indices.Delete([]string{index})
	if err != nil {
		log.Printf("Error deleting response: %s", err)
		return err
	}

	return nil
}

func IndexAny(index string, data any, id string) error {
	dataByte, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = Index(index, dataByte, id)
	if err != nil {
		return err
	}
	return nil
}

func GetAll(index string) ([]map[string]interface{}, error, int) {
	var r map[string]interface{}
	var rows []map[string]interface{}

	req := esapi.SearchRequest{
		Index: []string{index},
		Size:  esapi.IntPtr(10000),
	}

	res, err := req.Do(context.Background(), client.conn)
	if err != nil {
		log.Fatalf("Eror in get all response: %s", err)
		return nil, err, 1
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error get all document index=%s", res.Status(), index)
		return nil, fmt.Errorf("Error get all document index=%s Status=%s", index, res.Status()), res.StatusCode
	} else {
		// Deserialize the response into a map.

		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
			return nil, err, 2
		}

		for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
			row := hit.(map[string]interface{})["_source"].(map[string]interface{})
			rows = append(rows, row)
		}

	}

	return rows, nil, 0
}
