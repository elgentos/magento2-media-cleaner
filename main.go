package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	_ "github.com/go-sql-driver/mysql"
	"github.com/cespare/xxhash/v2"
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
	Hash         uint64
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

type DuplicateMapping struct {
	Original  string
	Duplicate string
	FullPath  string
	Size      int64
}

func main() {
	// Custom usage function to show double dashes for long flags
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Operation flags:\n")
		fmt.Fprintf(os.Stderr, "  -u, --list-unused         List unused media files\n")
		fmt.Fprintf(os.Stderr, "  -m, --list-missing        List missing media files\n")
		fmt.Fprintf(os.Stderr, "  -d, --list-duplicates     List duplicated files\n")
		fmt.Fprintf(os.Stderr, "  -r, --remove-unused       Remove unused product images\n")
		fmt.Fprintf(os.Stderr, "  -o, --remove-orphans      Remove orphaned media gallery rows\n")
		fmt.Fprintf(os.Stderr, "  -x, --remove-duplicates   Remove duplicated files and update database\n")
		fmt.Fprintf(os.Stderr, "\nConfiguration flags:\n")
		fmt.Fprintf(os.Stderr, "  --magento-root string     Path to Magento root directory (optional, auto-detects)\n")
		fmt.Fprintf(os.Stderr, "  --db-host string          Database host (default: localhost)\n")
		fmt.Fprintf(os.Stderr, "  --db-port string          Database port (default: 3306)\n")
		fmt.Fprintf(os.Stderr, "  --db-name string          Database name\n")
		fmt.Fprintf(os.Stderr, "  --db-user string          Database user\n")
		fmt.Fprintf(os.Stderr, "  --db-pass string          Database password\n")
		fmt.Fprintf(os.Stderr, "  --db-prefix string        Database table prefix\n")
		fmt.Fprintf(os.Stderr, "  --media-path string       Path to pub/media/catalog/product\n")
		fmt.Fprintf(os.Stderr, "  --workers int             Number of parallel workers (default: 10)\n")
		fmt.Fprintf(os.Stderr, "\nNote: Configuration values are read from app/etc/env.php if not provided\n")
	}

	// Operation flags with both short and long names
	var listUnused, listMissing, listDupes, removeUnused, removeOrphans, removeDupes bool

	flag.BoolVar(&listUnused, "list-unused", false, "List unused media files")
	flag.BoolVar(&listUnused, "u", false, "List unused media files (shorthand)")

	flag.BoolVar(&listMissing, "list-missing", false, "List missing media files")
	flag.BoolVar(&listMissing, "m", false, "List missing media files (shorthand)")

	flag.BoolVar(&listDupes, "list-duplicates", false, "List duplicated files")
	flag.BoolVar(&listDupes, "d", false, "List duplicated files (shorthand)")

	flag.BoolVar(&removeUnused, "remove-unused", false, "Remove unused product images")
	flag.BoolVar(&removeUnused, "r", false, "Remove unused product images (shorthand)")

	flag.BoolVar(&removeOrphans, "remove-orphans", false, "Remove orphaned media gallery rows")
	flag.BoolVar(&removeOrphans, "o", false, "Remove orphaned media gallery rows (shorthand)")

	flag.BoolVar(&removeDupes, "remove-duplicates", false, "Remove duplicated files and update database")
	flag.BoolVar(&removeDupes, "x", false, "Remove duplicated files and update database (shorthand)")

	// Configuration flags
	magentoRoot := flag.String("magento-root", "", "Path to Magento root directory (optional, auto-detects if not provided)")
	dbHost := flag.String("db-host", "localhost", "Database host (optional, reads from app/etc/env.php if not provided)")
	dbPort := flag.String("db-port", "3306", "Database port (optional, reads from app/etc/env.php if not provided)")
	dbName := flag.String("db-name", "", "Database name (optional, reads from app/etc/env.php if not provided)")
	dbUser := flag.String("db-user", "", "Database user (optional, reads from app/etc/env.php if not provided)")
	dbPass := flag.String("db-pass", "", "Database password (optional, reads from app/etc/env.php if not provided)")
	dbPrefix := flag.String("db-prefix", "", "Database table prefix (optional, reads from app/etc/env.php if not provided)")
	mediaPath := flag.String("media-path", "", "Path to pub/media/catalog/product (optional, defaults to <magento_root>/pub/media/catalog/product)")
	workers := flag.Int("workers", 10, "Number of parallel workers for file scanning")

	flag.Parse()

	var config Config
	var resolvedMagentoRoot string
	var envConfig Config
	loadedFromEnv := false

	// Try to find and load Magento root
	var err error
	if *magentoRoot != "" {
		// User provided explicit Magento root
		envPath := filepath.Join(*magentoRoot, "app", "etc", "env.php")
		if _, err := os.Stat(envPath); os.IsNotExist(err) {
			fmt.Printf("Error: Invalid Magento root directory '%s' (app/etc/env.php not found)\n", *magentoRoot)
			os.Exit(1)
		}
		resolvedMagentoRoot = *magentoRoot
	} else {
		// Auto-detect Magento root (only if db credentials not fully provided)
		startPath := *mediaPath
		if startPath == "" {
			startPath, _ = os.Getwd()
		}

		resolvedMagentoRoot, err = findMagentoRoot(startPath)
	}

	// If we found a Magento root, try to load env.php
	if resolvedMagentoRoot != "" {
		fmt.Printf("Found Magento root: %s\n", resolvedMagentoRoot)

		envConfig, err = loadConfigFromEnvPHP(resolvedMagentoRoot)
		if err != nil {
			fmt.Printf("Warning: Could not read env.php: %v\n", err)
		} else {
			loadedFromEnv = true
		}

		// Set media path default if not provided
		if *mediaPath == "" {
			*mediaPath = filepath.Join(resolvedMagentoRoot, "pub", "media", "catalog", "product")
		}
	}

	// Build config: Start with env.php values (if loaded), then override with CLI flags
	if loadedFromEnv {
		config = envConfig
	} else {
		// Initialize with defaults
		config = Config{
			DBHost: "localhost",
			DBPort: "3306",
		}
	}

	// Override with CLI flags if explicitly provided
	// Check if flags were explicitly set by user (not just defaults)
	hostSet := false
	portSet := false
	nameSet := false
	userSet := false
	passSet := false
	prefixSet := false

	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "db-host":
			hostSet = true
		case "db-port":
			portSet = true
		case "db-name":
			nameSet = true
		case "db-user":
			userSet = true
		case "db-pass":
			passSet = true
		case "db-prefix":
			prefixSet = true
		}
	})

	// Apply overrides
	if hostSet {
		config.DBHost = *dbHost
	}
	if portSet {
		config.DBPort = *dbPort
	}
	if nameSet {
		config.DBName = *dbName
	}
	if userSet {
		config.DBUser = *dbUser
	}
	if passSet {
		config.DBPass = *dbPass
	}
	if prefixSet {
		sanitized := sanitizeTablePrefix(*dbPrefix)
		if sanitized != *dbPrefix {
			fmt.Printf("Warning: db-prefix sanitized from '%s' to '%s'\n", *dbPrefix, sanitized)
		}
		config.DBTablePrefix = sanitized
	}

	// Set media path and workers
	if *mediaPath != "" {
		config.MediaPath = *mediaPath
	}
	config.WorkerCount = *workers

	// Validate required fields
	if config.DBName == "" || config.DBUser == "" {
		fmt.Println("Error: Database name and user are required.")
		fmt.Println("Please either:")
		fmt.Println("  1. Run this command from within a Magento installation,")
		fmt.Println("  2. Provide -magento-root flag, or")
		fmt.Println("  3. Provide -db-name and -db-user flags")
		flag.Usage()
		os.Exit(1)
	}

	if config.MediaPath == "" {
		fmt.Println("Error: -media-path is required when not using -magento-root")
		flag.Usage()
		os.Exit(1)
	}

	// Print configuration summary
	if loadedFromEnv {
		fmt.Printf("Loaded database configuration from env.php")
		// Check if any CLI flags override env.php
		overrides := []string{}
		if hostSet {
			overrides = append(overrides, "host")
		}
		if portSet {
			overrides = append(overrides, "port")
		}
		if nameSet {
			overrides = append(overrides, "name")
		}
		if userSet {
			overrides = append(overrides, "user")
		}
		if passSet {
			overrides = append(overrides, "password")
		}
		if prefixSet {
			overrides = append(overrides, "prefix")
		}
		if len(overrides) > 0 {
			fmt.Printf(" (overridden: %s)", strings.Join(overrides, ", "))
		}
		fmt.Println()
	}

	fmt.Printf("  Database: %s@%s:%s/%s\n", config.DBUser, config.DBHost, config.DBPort, config.DBName)
	if config.DBTablePrefix != "" {
		fmt.Printf("  Table prefix: %s\n", config.DBTablePrefix)
	}
	fmt.Printf("  Media path: %s\n", config.MediaPath)

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
	startTime := time.Now()

	// Scan filesystem with parallel workers
	fmt.Println("\nScanning filesystem...")
	scanStart := time.Now()
	filesMap, hashMap := scanFilesystem(config, stats)
	scanDuration := time.Since(scanStart)

	// Fetch media gallery entries from database
	fmt.Println("Querying database...")
	dbStart := time.Now()
	dbPaths, err := getMediaGalleryPaths(db, config)
	if err != nil {
		fmt.Printf("Error querying database: %v\n", err)
		os.Exit(1)
	}
	dbDuration := time.Since(dbStart)

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
	if listUnused {
		fmt.Println("\nUnused files:")
		for _, path := range unusedFiles {
			fmt.Println(path)
		}
	}

	if removeUnused {
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

	if listMissing {
		fmt.Println("\nMissing files:")
		for _, path := range missingFiles {
			fmt.Println(path)
		}
	}

	if removeOrphans {
		fmt.Println("\nRemoving orphaned database rows...")
		removed, err := removeOrphanedRows(db, config, missingFiles)
		if err != nil {
			fmt.Printf("Error removing orphaned rows: %v\n", err)
		} else {
			atomic.AddInt64(&stats.RemovedOrphans, removed)
		}
	}

	if listDupes {
		fmt.Println("\nDuplicate files:")
		for hash, files := range hashMap {
			if len(files) > 1 {
				fmt.Printf("Hash %016x:\n", hash)
				for _, file := range files {
					fmt.Printf("  - %s\n", file.RelativePath)
				}
			}
		}
	}

	if removeDupes {
		fmt.Println("\nRemoving duplicate files...")
		duplicateStart := time.Now()

		// Collect all duplicate mappings
		var allMappings []DuplicateMapping
		for _, files := range hashMap {
			if len(files) > 1 {
				original := files[0].RelativePath
				for i := 1; i < len(files); i++ {
					duplicate := files[i]
					allMappings = append(allMappings, DuplicateMapping{
						Original:  original,
						Duplicate: duplicate.RelativePath,
						FullPath:  filepath.Join(config.MediaPath, duplicate.RelativePath),
						Size:      duplicate.Size,
					})
				}
			}
		}

		fmt.Printf("Found %d duplicates to process\n", len(allMappings))

		// Process in batches of 5000
		const batchSize = 5000
		totalBatches := (len(allMappings) + batchSize - 1) / batchSize

		for i := 0; i < len(allMappings); i += batchSize {
			end := i + batchSize
			if end > len(allMappings) {
				end = len(allMappings)
			}

			batch := allMappings[i:end]
			batchNum := (i / batchSize) + 1

			fmt.Printf("Processing batch %d/%d (%d duplicates)...\n", batchNum, totalBatches, len(batch))

			// Update database
			vUpdated, gUpdated, err := updateDatabaseForDuplicatesBatch(db, config, batch)
			if err != nil {
				fmt.Printf("Error updating batch %d: %v\n", batchNum, err)
				continue // Skip file deletion for failed batch
			}

			// Delete files only after successful database update
			for _, mapping := range batch {
				if err := os.Remove(mapping.FullPath); err == nil {
					atomic.AddInt64(&stats.RemovedDuplicates, 1)
					atomic.AddInt64(&stats.BytesFreed, mapping.Size)
				}
			}

			atomic.AddInt64(&stats.UpdatedVarchar, vUpdated)
			atomic.AddInt64(&stats.UpdatedGallery, gUpdated)
		}

		duplicateDuration := time.Since(duplicateStart)
		fmt.Printf("\nDuplicate removal completed in %v\n", duplicateDuration.Round(time.Millisecond))
	}

	// Print summary
	totalDuration := time.Since(startTime)
	printStats(stats, len(dbPaths), scanDuration, dbDuration, totalDuration)
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

func scanFilesystem(config Config, stats *Stats) (map[string]FileInfo, map[uint64][]FileInfo) {
	// Channel for file paths
	fileChan := make(chan string, 10000)

	// Start recursive directory walker in a single goroutine
	var walkerWg sync.WaitGroup
	walkerWg.Add(1)
	go func() {
		defer walkerWg.Done()
		walkDirectoryRecursive(config.MediaPath, fileChan)
		close(fileChan)
	}()

	// Worker-local maps for each worker
	type workerResult struct {
		filesMap map[string]FileInfo
		hashMap  map[uint64][]FileInfo
	}

	resultChan := make(chan workerResult, config.WorkerCount)
	var wg sync.WaitGroup

	// Start file processing workers with local maps
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localFiles := make(map[string]FileInfo, 50000)
			localHashes := make(map[uint64][]FileInfo, 10000)

			for path := range fileChan {
				processFileLocal(path, config.MediaPath, stats, localFiles, localHashes)
			}

			resultChan <- workerResult{
				filesMap: localFiles,
				hashMap:  localHashes,
			}
		}()
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Wait for walker to finish
	walkerWg.Wait()

	// Merge all worker results
	finalFilesMap := make(map[string]FileInfo, 500000)
	finalHashMap := make(map[uint64][]FileInfo, 100000)

	for result := range resultChan {
		// Merge files
		for path, fileInfo := range result.filesMap {
			finalFilesMap[path] = fileInfo
		}

		// Merge hashes
		for hash, files := range result.hashMap {
			finalHashMap[hash] = append(finalHashMap[hash], files...)
		}
	}

	// Count duplicates correctly (once per group, not per file)
	for _, files := range finalHashMap {
		if len(files) > 1 {
			atomic.AddInt64(&stats.DuplicateFiles, int64(len(files)-1))
		}
	}

	return finalFilesMap, finalHashMap
}

// walkDirectoryRecursive recursively walks directories and sends files to fileChan
func walkDirectoryRecursive(dir string, fileChan chan<- string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	// Image extensions to filter
	imageExts := map[string]bool{
		".jpg":  true,
		".jpeg": true,
		".png":  true,
		".gif":  true,
		".webp": true,
		".avif": true,
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())

		if entry.IsDir() {
			// Recursively process subdirectory
			walkDirectoryRecursive(fullPath, fileChan)
		} else {
			// Only process image files
			ext := strings.ToLower(filepath.Ext(entry.Name()))
			if imageExts[ext] {
				fileChan <- fullPath
			}
		}
	}
}

func processFileLocal(fullPath, basePath string, stats *Stats,
	filesMap map[string]FileInfo, hashMap map[uint64][]FileInfo) {

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

	// No mutex needed - worker-local maps
	atomic.AddInt64(&stats.TotalFiles, 1)
	filesMap[relPath] = fileInfo
	hashMap[hash] = append(hashMap[hash], fileInfo)
}

func hashFile(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := xxhash.New()
	// Hash only the first 4 MB for performance
	limitedReader := io.LimitReader(f, 4<<20)
	if _, err := io.Copy(h, limitedReader); err != nil {
		return 0, err
	}

	return h.Sum64(), nil
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

func updateDatabaseForDuplicatesBatch(db *sql.DB, config Config, mappings []DuplicateMapping) (int64, int64, error) {
	if len(mappings) == 0 {
		return 0, 0, nil
	}

	varcharTable := config.DBTablePrefix + "catalog_product_entity_varchar"
	galleryTable := config.DBTablePrefix + "catalog_product_entity_media_gallery"

	// Build SQL for batch updates
	varcharSQL, varcharArgs := buildBatchUpdateSQL(varcharTable, mappings)
	gallerySQL, galleryArgs := buildBatchUpdateSQL(galleryTable, mappings)

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Update varchar table
	vResult, err := tx.Exec(varcharSQL, varcharArgs...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to update varchar table: %v", err)
	}
	vRows, _ := vResult.RowsAffected()

	// Update gallery table
	gResult, err := tx.Exec(gallerySQL, galleryArgs...)
	if err != nil {
		return vRows, 0, fmt.Errorf("failed to update gallery table: %v", err)
	}
	gRows, _ := gResult.RowsAffected()

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return vRows, gRows, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return vRows, gRows, nil
}

func buildBatchUpdateSQL(tableName string, mappings []DuplicateMapping) (string, []interface{}) {
	var caseClauses []string
	var whereValues []string
	var args []interface{}

	for _, mapping := range mappings {
		// CASE WHEN value = ? THEN ?
		caseClauses = append(caseClauses, "WHEN ? THEN ?")
		args = append(args, mapping.Duplicate, mapping.Original)

		// WHERE value IN (?, ...)
		whereValues = append(whereValues, "?")
		args = append(args, mapping.Duplicate)
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET value = CASE value %s END WHERE value IN (%s)",
		tableName,
		strings.Join(caseClauses, " "),
		strings.Join(whereValues, ", "),
	)

	return sql, args
}

func printStats(stats *Stats, dbEntries int, scanDuration, dbDuration, totalDuration time.Duration) {
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

	// Performance timing
	fmt.Println("\nPerformance:")
	fmt.Printf("Filesystem scan: %v\n", scanDuration.Round(time.Millisecond))
	fmt.Printf("Database query: %v\n", dbDuration.Round(time.Millisecond))
	fmt.Printf("Total time: %v\n", totalDuration.Round(time.Millisecond))

	if stats.TotalFiles > 0 && scanDuration > 0 {
		filesPerSecond := float64(stats.TotalFiles) / scanDuration.Seconds()
		fmt.Printf("Files processed: %.0f files/second\n", filesPerSecond)
	}

	fmt.Println(strings.Repeat("=", 50))
}

func findMagentoRoot(startPath string) (string, error) {
	// Start from the given path and traverse up until we find app/etc/env.php
	currentPath := startPath

	for {
		envPath := filepath.Join(currentPath, "app", "etc", "env.php")
		if _, err := os.Stat(envPath); err == nil {
			return currentPath, nil
		}

		// Move up one directory
		parentPath := filepath.Dir(currentPath)
		if parentPath == currentPath {
			// Reached root directory without finding Magento
			return "", fmt.Errorf("could not find Magento root directory (app/etc/env.php not found)")
		}
		currentPath = parentPath
	}
}

func parseEnvPHP(envPath string) (map[string]interface{}, error) {
	content, err := os.ReadFile(envPath)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	text := string(content)

	// Find the 'db' section - need to handle nested arrays properly
	dbStart := strings.Index(text, "'db' =>")
	if dbStart == -1 {
		return result, fmt.Errorf("'db' section not found in env.php")
	}

	// Find the matching closing bracket for the db section
	// We need to count brackets to handle nested arrays
	dbSection := extractBalancedSection(text[dbStart:])

	// Extract table_prefix from db section
	prefixPattern := regexp.MustCompile(`'table_prefix'\s*=>\s*'([^']*)'`)
	prefixMatch := prefixPattern.FindStringSubmatch(dbSection)
	if len(prefixMatch) > 1 {
		result["table_prefix"] = prefixMatch[1]
	} else {
		result["table_prefix"] = ""
	}

	// Find connection -> default section
	connStart := strings.Index(dbSection, "'connection' =>")
	if connStart == -1 {
		return result, fmt.Errorf("'connection' section not found in env.php")
	}

	connSection := extractBalancedSection(dbSection[connStart:])

	defaultStart := strings.Index(connSection, "'default' =>")
	if defaultStart == -1 {
		return result, fmt.Errorf("'default' connection not found in env.php")
	}

	defaultSection := extractBalancedSection(connSection[defaultStart:])

	// Extract individual fields
	result["host"] = extractValue(defaultSection, "host")
	result["dbname"] = extractValue(defaultSection, "dbname")
	result["username"] = extractValue(defaultSection, "username")
	result["password"] = extractValue(defaultSection, "password")

	return result, nil
}

// extractBalancedSection extracts content within balanced brackets starting from text
func extractBalancedSection(text string) string {
	// Find the opening bracket
	start := strings.Index(text, "[")
	if start == -1 {
		return ""
	}

	depth := 0
	for i := start; i < len(text); i++ {
		if text[i] == '[' {
			depth++
		} else if text[i] == ']' {
			depth--
			if depth == 0 {
				return text[start:i+1]
			}
		}
	}
	return ""
}

func extractValue(text, key string) string {
	pattern := regexp.MustCompile(fmt.Sprintf(`'%s'\s*=>\s*'([^']*)'`, key))
	match := pattern.FindStringSubmatch(text)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func loadConfigFromEnvPHP(magentoRoot string) (Config, error) {
	envPath := filepath.Join(magentoRoot, "app", "etc", "env.php")

	envData, err := parseEnvPHP(envPath)
	if err != nil {
		return Config{}, fmt.Errorf("failed to parse env.php: %v", err)
	}

	config := Config{
		DBHost:        getStringValue(envData, "host", "localhost"),
		DBPort:        "3306", // Default MySQL port
		DBName:        getStringValue(envData, "dbname", ""),
		DBUser:        getStringValue(envData, "username", ""),
		DBPass:        getStringValue(envData, "password", ""),
		DBTablePrefix: sanitizeTablePrefix(getStringValue(envData, "table_prefix", "")),
	}

	// Extract port from host if it contains a colon
	if strings.Contains(config.DBHost, ":") {
		parts := strings.Split(config.DBHost, ":")
		config.DBHost = parts[0]
		config.DBPort = parts[1]
	}

	return config, nil
}

func getStringValue(data map[string]interface{}, key, defaultVal string) string {
	if val, ok := data[key]; ok {
		if strVal, ok := val.(string); ok {
			return strVal
		}
	}
	return defaultVal
}

// sanitizeTablePrefix removes any characters that are not alphanumeric or underscore
// This prevents SQL injection when the prefix is concatenated into table names
func sanitizeTablePrefix(prefix string) string {
	var result strings.Builder
	for _, r := range prefix {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			result.WriteRune(r)
		}
	}
	return result.String()
}
