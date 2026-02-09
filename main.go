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

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Failed to post data", http.StatusInternalServerError)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		CREATE TEMP TABLE temp_upload (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255),
			category VARCHAR(100),
			price NUMERIC(10,2),
			create_date VARCHAR(50)
		) ON COMMIT DROP
	`)

	if err != nil {
		http.Error(w, "Failed to create temp table", http.StatusInternalServerError)
		return
	}

	for _, record := range records[1:] {
		if len(record) != 5 {
			continue
		}

		name := strings.TrimSpace(record[1])
		category := strings.TrimSpace(record[2])
		price, _ := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)
		createDate := strings.TrimSpace(record[4])

		sql := "INSERT INTO temp_upload (name, category, price, create_date) VALUES ($1, $2, $3, $4)"
		_, err = tx.Exec(sql, name, category, price, createDate)

		if err != nil {
			log.Printf("Failed to insert record: %v", err)
			http.Error(w, "Failed to post data", http.StatusInternalServerError)
			return
		}
	}

	var totalItems int
	var totalCategories int
	var totalPrice float64

	err = tx.QueryRow(`
		SELECT 
			COUNT(*), 
			COUNT(DISTINCT category),
			SUM(price)
		FROM temp_upload
		`).Scan(&totalItems, &totalCategories, &totalPrice)

	if err != nil {
		log.Printf("Failed to count statistics: %v", err)
		http.Error(w, "Failed to load statistics", http.StatusInternalServerError)
		return
	}

	response := PostResponse{
		TotalItems:      totalItems,
		TotalCategories: totalCategories,
		TotalPrice:      totalPrice,
	}

	_, err = tx.Exec(`INSERT INTO prices SELECT * FROM temp_upload`)
	if err != nil {
		log.Printf("Failed to insert into main table: %v", err)
		http.Error(w, "Failed to save data", http.StatusInternalServerError)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit: %v", err)
		http.Error(w, "Failed to commit", http.StatusInternalServerError)
		return
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
	if err = rows.Err(); err != nil {
		log.Printf("Error in row iteration: %v", err)
		http.Error(w, "Failed to read all data", http.StatusInternalServerError)
		return
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
	_, err = w.Write(zipBuffer.Bytes())

	if err != nil {
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
		return
	}

}

func main() {
	err := initDB()
	if err != nil {
		log.Printf("Failed to connect to database: %v", err)
		return
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
		log.Printf("Failed to start server: %v", err)
		return
	}
}
