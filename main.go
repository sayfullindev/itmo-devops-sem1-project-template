package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var db *sql.DB

type Product struct {
	ID         int
	Name       string
	Category   string
	Price      float32
	CreateDate string
}

type PostResponse struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
}

func initDB() error {
	conndata := "host=localhost port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable"
	var err error
	db, err = sql.Open("postgres", conndata)
	if err != nil {
		return err
	}
	return db.Ping()
}

func handlePostPrices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "failed to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Failed to read zip archive", http.StatusBadRequest)
		return
	}

	var csvFile *zip.File
	for _, filezip := range zipReader.File {
		if filezip.Name == "test_data.csv" {
			csvFile = filezip
			break
		}
	}

	if csvFile == nil {
		http.Error(w, "test_data.csv not found in archive", http.StatusBadRequest)
		return
	}

	fileReader, err := csvFile.Open()
	if err != nil {
		http.Error(w, "Failed to open data.csv", http.StatusBadRequest)
		return
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	records, err := csvReader.ReadAll()
	if err != nil {
		http.Error(w, "Failed to parse CSV", http.StatusBadRequest)
		return
	}

	for _, record := range records[1:] {
		if len(record) != 5 {
			continue
		}
		id, _ := strconv.Atoi(strings.TrimSpace(record[0]))
		name := strings.TrimSpace(record[1])
		category := strings.TrimSpace(record[2])
		price, _ := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)
		createDate := strings.TrimSpace(record[4])

		sql := "INSERT INTO prices (id, name, category, price, create_date) VALUES ($1, $2, $3, $4, $5)"
		_, err = db.Exec(sql, id, name, category, price, createDate)

		if err != nil {
			log.Printf("Failed to insert record: %v", err)
		}
	}

	var totalItems int
	var totalCategories int
	var totalPrice float64

	db.QueryRow("SELECT COUNT(*) FROM prices ").Scan(&totalItems)
	db.QueryRow("SELECT COUNT(DISTINCT category) FROM prices ").Scan(&totalCategories)
	db.QueryRow("SELECT SUM(price) FROM prices ").Scan(&totalPrice)

	response := PostResponse{
		TotalItems:      totalItems,
		TotalCategories: totalCategories,
		TotalPrice:      totalPrice,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

}

func handleGetPrices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	rows, err := db.Query("SELECT id, name, category, price, create_date FROM prices ORDER BY id;")
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.CreateDate)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		products = append(products, p)
	}
	var csvBuffer bytes.Buffer
	csvWriter := csv.NewWriter(&csvBuffer)

	for _, p := range products {
		record := []string{
			strconv.Itoa(p.ID),
			p.Name,
			p.Category,
			fmt.Sprintf("%2.f", p.Price),
			p.CreateDate,
		}
		csvWriter.Write(record)
	}
	csvWriter.Flush()

	var zipBuffer bytes.Buffer
	zipWriter := *zip.NewWriter(&zipBuffer)
	csvFile, err := zipWriter.Create("data.csv")
	if err != nil {
		http.Error(w, "Failed to create zip archive", http.StatusInternalServerError)
		return
	}

	_, err = csvFile.Write(csvBuffer.Bytes())
	if err != nil {
		http.Error(w, "Failed to write to zip archive", http.StatusInternalServerError)
	}

	zipWriter.Close()

	w.Header().Set("Content-type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(zipBuffer.Bytes())

}

func main() {
	err := initDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	log.Printf("Sucess connection to database")

	r := mux.NewRouter()
	r.HandleFunc("/api/v0/prices", handleGetPrices).
		Methods(http.MethodGet)
	r.HandleFunc("/api/v0/prices", handlePostPrices).
		Methods(http.MethodPost)

	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
