package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"sync"
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	records []Record
	mu      sync.Mutex
}

func (s *Log) DecodeJson(w http.ResponseWriter, r *http.Request) {

	var record struct {
		Value []byte `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
		http.Error(w, "It can't deserialize the json", http.StatusBadRequest)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = append(s.records, Record{record.Value, uint64(len(s.records))})
	w.WriteHeader(http.StatusOK)

}

func (s *Log) GetRecord(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Offset uint64 `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "It can't deserialize the json", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if request.Offset <= uint64(len(s.records)-1) {
		if err := json.NewEncoder(w).Encode(s.records[int(request.Offset)-1]); err != nil {
			http.Error(w, "It can't serialize the struct to json", http.StatusInternalServerError)
			return
		}
		fmt.Println("record: ", s.records[request.Offset])
		return
	} else {
		http.Error(w, "Record not found in the slice", http.StatusNotFound)
	}
}

func main() {
	logs := &Log{}
	r := mux.NewRouter()
	r.HandleFunc("/", logs.DecodeJson).Methods(http.MethodPost)
	r.HandleFunc("/", logs.GetRecord).Methods(http.MethodGet)
	fmt.Println("Log is running on port 8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		fmt.Printf("Error starting server: %s\n", err)
	}
}
