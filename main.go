package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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
		config.DBTablePrefix = *dbPrefix
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

	// Scan filesystem with parallel workers
	fmt.Println("\nScanning filesystem...")
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
				fmt.Printf("Hash %s:\n", hash)
				for _, file := range files {
					fmt.Printf("  - %s\n", file.RelativePath)
				}
			}
		}
	}

	if removeDupes {
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

	// Channel for file paths - larger buffer for better throughput
	fileChan := make(chan string, 10000)
	// Channel for directories to process in parallel
	dirChan := make(chan string, 100)
	// Track directories in flight
	var dirsInFlight int64

	// Start file processing workers
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range fileChan {
				processFile(path, config.MediaPath, stats, &mu, filesMap, hashMap)
			}
		}()
	}

	// Start directory walker workers (parallel directory scanning)
	var dirWg sync.WaitGroup
	numDirWalkers := config.WorkerCount / 2
	if numDirWalkers < 2 {
		numDirWalkers = 2
	}

	for i := 0; i < numDirWalkers; i++ {
		dirWg.Add(1)
		go func() {
			defer dirWg.Done()
			for dir := range dirChan {
				walkDirectory(dir, config.MediaPath, fileChan, dirChan, &dirsInFlight)
				// Decrement after processing directory
				if atomic.AddInt64(&dirsInFlight, -1) == 0 {
					// Last directory finished, close the channel
					close(dirChan)
				}
			}
		}()
	}

	// Start with the root directory
	atomic.AddInt64(&dirsInFlight, 1)
	dirChan <- config.MediaPath

	// Wait for all directory walkers to finish
	dirWg.Wait()
	close(fileChan)

	// Wait for all file processors to finish
	wg.Wait()

	return filesMap, hashMap
}

// walkDirectory processes a single directory and sends subdirs to dirChan
func walkDirectory(dir string, basePath string, fileChan chan<- string, dirChan chan<- string, dirsInFlight *int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	subdirs := []string{}

	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())

		if entry.IsDir() {
			// Collect subdirectories
			subdirs = append(subdirs, fullPath)
		} else {
			// Send file for processing
			fileChan <- fullPath
		}
	}

	// Send all subdirectories at once (increment counter before sending)
	if len(subdirs) > 0 {
		atomic.AddInt64(dirsInFlight, int64(len(subdirs)))
		for _, subdir := range subdirs {
			dirChan <- subdir
		}
	}
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

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return strconv.FormatUint(h.Sum64(), 16), nil
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
		DBTablePrefix: getStringValue(envData, "table_prefix", ""),
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
