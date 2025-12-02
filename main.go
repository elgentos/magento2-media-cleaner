package main

import (
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	DBHost         string
	DBPort         string
	DBName         string
	DBUser         string
	DBPass         string
	DBTablePrefix  string
	MediaPath      string
	WorkerCount    int
}

type FileInfo struct {
	RelativePath string
	Hash         string
	Size         int64
}

type Stats struct {
	TotalFiles        int64
	CachedFiles       int64
	UnusedFiles       int64
	MissingFiles      int64
	DuplicateFiles    int64
	RemovedUnused     int64
	RemovedDuplicates int64
	RemovedOrphans    int64
	BytesFreed        int64
	UpdatedVarchar    int64
	UpdatedGallery    int64
}

func main() {
	// CLI flags
	listUnused := flag.Bool("u", false, "List unused media files")
	listMissing := flag.Bool("m", false, "List missing media files")
	listDupes := flag.Bool("d", false, "List duplicated files")
	removeUnused := flag.Bool("r", false, "Remove unused product images")
	removeOrphans := flag.Bool("o", false, "Remove orphaned media gallery rows")
	removeDupes := flag.Bool("x", false, "Remove duplicated files and update database")

	// Configuration flags
	dbHost := flag.String("db-host", "localhost", "Database host")
	dbPort := flag.String("db-port", "3306", "Database port")
	dbName := flag.String("db-name", "", "Database name")
	dbUser := flag.String("db-user", "", "Database user")
	dbPass := flag.String("db-pass", "", "Database password")
	dbPrefix := flag.String("db-prefix", "", "Database table prefix")
	mediaPath := flag.String("media-path", "", "Path to pub/media/catalog/product")
	workers := flag.Int("workers", 10, "Number of parallel workers for file scanning")

	flag.Parse()

	// Validate required flags
	if *dbName == "" || *dbUser == "" || *mediaPath == "" {
		fmt.Println("Error: -db-name, -db-user, and -media-path are required")
		flag.Usage()
		os.Exit(1)
	}

	config := Config{
		DBHost:        *dbHost,
		DBPort:        *dbPort,
		DBName:        *dbName,
		DBUser:        *dbUser,
		DBPass:        *dbPass,
		DBTablePrefix: *dbPrefix,
		MediaPath:     *mediaPath,
		WorkerCount:   *workers,
	}

	// Connect to database
	db, err := connectDB(config)
	if err != nil {
		fmt.Printf("Database connection error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Verify media path exists
	if _, err := os.Stat(config.MediaPath); os.IsNotExist(err) {
		fmt.Printf("Cannot find \"%s\" folder.\n", config.MediaPath)
		fmt.Println("It appears there are no product images to analyze.")
		os.Exit(1)
	}

	stats := &Stats{}

	// Scan filesystem with parallel workers
	fmt.Println("Scanning filesystem...")
	filesMap, hashMap := scanFilesystem(config, stats)

	// Fetch media gallery entries from database
	fmt.Println("Querying database...")
	dbPaths, err := getMediaGalleryPaths(db, config)
	if err != nil {
		fmt.Printf("Error querying database: %v\n", err)
		os.Exit(1)
	}

	// Convert to map for faster lookups
	dbPathsMap := make(map[string]bool, len(dbPaths))
	for _, path := range dbPaths {
		dbPathsMap[path] = true
	}

	// Find unused files (in filesystem but not in DB)
	unusedFiles := []string{}
	for path := range filesMap {
		if !dbPathsMap[path] {
			atomic.AddInt64(&stats.UnusedFiles, 1)
			unusedFiles = append(unusedFiles, path)
		}
	}

	// Find missing files (in DB but not in filesystem)
	missingFiles := []string{}
	for path := range dbPathsMap {
		if _, exists := filesMap[path]; !exists {
			atomic.AddInt64(&stats.MissingFiles, 1)
			missingFiles = append(missingFiles, path)
		}
	}

	// Process actions based on flags
	if *listUnused {
		fmt.Println("\nUnused files:")
		for _, path := range unusedFiles {
			fmt.Println(path)
		}
	}

	if *removeUnused {
		fmt.Println("\nRemoving unused files...")
		for _, path := range unusedFiles {
			fullPath := filepath.Join(config.MediaPath, path)
			if info, err := os.Stat(fullPath); err == nil {
				if err := os.Remove(fullPath); err == nil {
					atomic.AddInt64(&stats.RemovedUnused, 1)
					atomic.AddInt64(&stats.BytesFreed, info.Size())
					fmt.Printf("Removed: %s\n", path)
				}
			}
		}
	}

	if *listMissing {
		fmt.Println("\nMissing files:")
		for _, path := range missingFiles {
			fmt.Println(path)
		}
	}

	if *removeOrphans {
		fmt.Println("\nRemoving orphaned database rows...")
		removed, err := removeOrphanedRows(db, config, missingFiles)
		if err != nil {
			fmt.Printf("Error removing orphaned rows: %v\n", err)
		} else {
			atomic.AddInt64(&stats.RemovedOrphans, removed)
		}
	}

	if *listDupes {
		fmt.Println("\nDuplicate files:")
		for hash, files := range hashMap {
			if len(files) > 1 {
				fmt.Printf("Hash %s:\n", hash)
				for _, file := range files {
					fmt.Printf("  - %s\n", file.RelativePath)
				}
			}
		}
	}

	if *removeDupes {
		fmt.Println("\nRemoving duplicate files...")
		for _, files := range hashMap {
			if len(files) > 1 {
				// Keep first file, remove others
				original := files[0].RelativePath
				for i := 1; i < len(files); i++ {
					duplicate := files[i]
					fullPath := filepath.Join(config.MediaPath, duplicate.RelativePath)

					// Update database references
					vUpdated, gUpdated, err := updateDatabaseForDuplicate(db, config, original, duplicate.RelativePath)
					if err != nil {
						fmt.Printf("Error updating database for %s: %v\n", duplicate.RelativePath, err)
						continue
					}

					// Remove file
					if err := os.Remove(fullPath); err == nil {
						atomic.AddInt64(&stats.RemovedDuplicates, 1)
						atomic.AddInt64(&stats.BytesFreed, duplicate.Size)
						atomic.AddInt64(&stats.UpdatedVarchar, vUpdated)
						atomic.AddInt64(&stats.UpdatedGallery, gUpdated)
						fmt.Printf("Removed duplicate: %s -> %s\n", duplicate.RelativePath, original)
					}
				}
			}
		}
	}

	// Print summary
	printStats(stats, len(dbPaths))
}

func connectDB(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.DBUser, config.DBPass, config.DBHost, config.DBPort, config.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func scanFilesystem(config Config, stats *Stats) (map[string]FileInfo, map[string][]FileInfo) {
	filesMap := make(map[string]FileInfo)
	hashMap := make(map[string][]FileInfo)

	var mu sync.Mutex
	var wg sync.WaitGroup

	// Channel for file paths
	fileChan := make(chan string, 1000)

	// Start worker pool
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range fileChan {
				processFile(path, config.MediaPath, stats, &mu, filesMap, hashMap)
			}
		}()
	}

	// Walk filesystem and send paths to workers
	go func() {
		filepath.Walk(config.MediaPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				fileChan <- path
			}
			return nil
		})
		close(fileChan)
	}()

	wg.Wait()

	return filesMap, hashMap
}

func processFile(fullPath, basePath string, stats *Stats, mu *sync.Mutex,
	filesMap map[string]FileInfo, hashMap map[string][]FileInfo) {

	relPath := strings.TrimPrefix(fullPath, basePath)
	if relPath == "" {
		return
	}

	// Skip cache directory
	if strings.HasPrefix(relPath, "/cache/") || strings.HasPrefix(relPath, "cache/") {
		atomic.AddInt64(&stats.CachedFiles, 1)
		return
	}

	// Calculate hash
	hash, err := hashFile(fullPath)
	if err != nil {
		return
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		return
	}

	fileInfo := FileInfo{
		RelativePath: relPath,
		Hash:         hash,
		Size:         info.Size(),
	}

	mu.Lock()
	atomic.AddInt64(&stats.TotalFiles, 1)
	filesMap[relPath] = fileInfo
	hashMap[hash] = append(hashMap[hash], fileInfo)

	// Count duplicates
	if len(hashMap[hash]) > 1 {
		atomic.AddInt64(&stats.DuplicateFiles, 1)
	}
	mu.Unlock()
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func getMediaGalleryPaths(db *sql.DB, config Config) ([]string, error) {
	tableName := config.DBTablePrefix + "catalog_product_entity_media_gallery"
	query := fmt.Sprintf("SELECT value FROM %s", tableName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			continue
		}
		paths = append(paths, value)
	}

	return paths, nil
}

func removeOrphanedRows(db *sql.DB, config Config, missingFiles []string) (int64, error) {
	if len(missingFiles) == 0 {
		return 0, nil
	}

	tableName := config.DBTablePrefix + "catalog_product_entity_media_gallery"

	// Process in batches to avoid "too many placeholders" error
	// MySQL max placeholders is 65535, use 5000 for safety
	const batchSize = 5000
	var totalAffected int64

	for i := 0; i < len(missingFiles); i += batchSize {
		end := i + batchSize
		if end > len(missingFiles) {
			end = len(missingFiles)
		}

		batch := missingFiles[i:end]

		// Build IN clause for this batch
		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, file := range batch {
			placeholders[j] = "?"
			args[j] = file
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE value IN (%s)",
			tableName, strings.Join(placeholders, ","))

		result, err := db.Exec(query, args...)
		if err != nil {
			return totalAffected, err
		}

		affected, _ := result.RowsAffected()
		totalAffected += affected

		fmt.Printf("Processed batch %d-%d: removed %d rows\n", i+1, end, affected)
	}

	return totalAffected, nil
}

func updateDatabaseForDuplicate(db *sql.DB, config Config, original, duplicate string) (int64, int64, error) {
	varcharTable := config.DBTablePrefix + "catalog_product_entity_varchar"
	galleryTable := config.DBTablePrefix + "catalog_product_entity_media_gallery"

	// Update varchar table
	vResult, err := db.Exec(
		fmt.Sprintf("UPDATE %s SET value = ? WHERE value = ?", varcharTable),
		original, duplicate,
	)
	if err != nil {
		return 0, 0, err
	}
	vRows, _ := vResult.RowsAffected()

	// Update gallery table
	gResult, err := db.Exec(
		fmt.Sprintf("UPDATE %s SET value = ? WHERE value = ?", galleryTable),
		original, duplicate,
	)
	if err != nil {
		return vRows, 0, err
	}
	gRows, _ := gResult.RowsAffected()

	return vRows, gRows, nil
}

func printStats(stats *Stats, dbEntries int) {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Printf("Media Gallery entries: %d\n", dbEntries)
	fmt.Printf("Files in directory: %d\n", stats.TotalFiles)
	fmt.Printf("Cached images: %d\n", stats.CachedFiles)
	fmt.Printf("Unused files: %d\n", stats.UnusedFiles)
	fmt.Printf("Missing files: %d\n", stats.MissingFiles)
	fmt.Printf("Duplicated files: %d\n", stats.DuplicateFiles)
	fmt.Println(strings.Repeat("=", 50))

	if stats.RemovedUnused > 0 {
		fmt.Printf("Removed unused files: %d\n", stats.RemovedUnused)
	}
	if stats.RemovedOrphans > 0 {
		fmt.Printf("Removed orphaned rows: %d\n", stats.RemovedOrphans)
	}
	if stats.RemovedDuplicates > 0 {
		fmt.Printf("Removed duplicated files: %d\n", stats.RemovedDuplicates)
		fmt.Printf("Updated catalog_product_entity_varchar rows: %d\n", stats.UpdatedVarchar)
		fmt.Printf("Updated catalog_product_entity_media_gallery rows: %d\n", stats.UpdatedGallery)
	}
	if stats.BytesFreed > 0 {
		fmt.Printf("Disk space freed: %.2f MB\n", float64(stats.BytesFreed)/1024/1024)
	}
	fmt.Println(strings.Repeat("=", 50))
}
