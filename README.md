# Magento 2 Media Cleaner (Go)

## Work in progress - don't try this at home, you'll probably loose some data!

A standalone Go application for analyzing and cleaning Magento 2 catalog media files. This is a high-performance refactor of [sivaschenko/magento2-clean-media](https://github.com/sivaschenko/magento2-clean-media) with parallel file scanning and optimized memory usage.

## Features

- **Parallel Processing**: Uses goroutine pool for concurrent file hashing
- **Fast Analysis**: Scans 20k+ files efficiently with configurable worker count
- **In-Memory Comparison**: Lightweight memory footprint for comparing filesystem vs database
- **Multiple Operations**:
  - List unused files (in filesystem but not in database)
  - List missing files (in database but not in filesystem)
  - List duplicate files (same content, different paths)
  - Remove unused files from filesystem
  - Remove orphaned database entries
  - Remove duplicate files and update database references
- **Progress Reporting**: Detailed statistics and freed disk space reporting

## Installation

### Binary Release

Download the latest binary from the [Releases](https://github.com/elgentos/magento2-media-cleaner/releases) page.

### Build from Source

```bash
git clone https://github.com/elgentos/magento2-media-cleaner.git
cd magento2-media-cleaner
go build -o magento2-media-cleaner
```

## Usage

### Basic Information

```bash
./magento2-media-cleaner \
  -db-name="magento2" \
  -db-user="root" \
  -db-pass="password" \
  -media-path="/var/www/html/pub/media/catalog/product"
```

### List Operations

```bash
# List unused files (in filesystem but not referenced in DB)
./magento2-media-cleaner -u -db-name="magento2" -db-user="root" -media-path="/path/to/media"

# List missing files (referenced in DB but not in filesystem)
./magento2-media-cleaner -m -db-name="magento2" -db-user="root" -media-path="/path/to/media"

# List duplicate files
./magento2-media-cleaner -d -db-name="magento2" -db-user="root" -media-path="/path/to/media"
```

### Cleanup Operations

```bash
# Remove unused files from filesystem
./magento2-media-cleaner -r -db-name="magento2" -db-user="root" -media-path="/path/to/media"

# Remove orphaned database entries (for missing files)
./magento2-media-cleaner -o -db-name="magento2" -db-user="root" -media-path="/path/to/media"

# Remove duplicate files and update all DB references to point to original
./magento2-media-cleaner -x -db-name="magento2" -db-user="root" -media-path="/path/to/media"

# Combine operations
./magento2-media-cleaner -r -o -x -db-name="magento2" -db-user="root" -media-path="/path/to/media"
```

## Configuration Options

### Required Flags

- `-db-name`: Database name
- `-db-user`: Database username
- `-media-path`: Absolute path to `pub/media/catalog/product` directory

### Optional Flags

- `-db-host`: Database host (default: `localhost`)
- `-db-port`: Database port (default: `3306`)
- `-db-pass`: Database password (default: empty)
- `-db-prefix`: Database table prefix (default: empty)
- `-workers`: Number of parallel workers for file scanning (default: `10`)

### Operation Flags

- `-u`: List unused media files
- `-m`: List missing media files
- `-d`: List duplicated files
- `-r`: Remove unused product images
- `-o`: Remove orphaned media gallery rows
- `-x`: Remove duplicated files and update database

## Example Output

```
Scanning filesystem...
Querying database...

==================================================
Media Gallery entries: 15234
Files in directory: 18942
Cached images: 2341
Unused files: 3708
Missing files: 0
Duplicated files: 127
==================================================
Removed unused files: 3708
Removed duplicated files: 127
Updated catalog_product_entity_varchar rows: 89
Updated catalog_product_entity_media_gallery rows: 127
Disk space freed: 1247.32 MB
==================================================
```

## Architecture

The application follows your suggested architecture:

1. **Goroutine Pool**: Configurable worker pool scans filesystem and hashes files in parallel
2. **Single DB Connection**: Reuses one database connection for all queries
3. **In-Memory Comparison**: Builds hash maps and compares sets in memory (efficient at 20k entries)
4. **Progress Reporting**: Atomic counters track operations and report detailed statistics

### Key Components

- **File Scanner**: Walks directory tree and dispatches files to worker pool
- **Worker Pool**: Concurrent goroutines hash files with MD5
- **Database Layer**: Queries `catalog_product_entity_media_gallery` for all media paths
- **Comparator**: Builds sets and identifies unused/missing/duplicate files
- **Cleanup Engine**: Removes files and updates database with transaction safety

## Performance

- **Parallel Hashing**: 10 workers can process ~1000 files/second on SSD
- **Memory Efficient**: ~100MB RAM for 20k files
- **Fast Comparison**: O(n) complexity using hash maps

## Database Tables

The application interacts with these Magento 2 tables:

- `catalog_product_entity_media_gallery`: Main media gallery entries
- `catalog_product_entity_varchar`: Product attributes (image, small_image, thumbnail)

## Safety Notes

- Always backup your database before running cleanup operations
- Test with list flags (`-u`, `-m`, `-d`) before running removal flags
- The application skips the `cache/` directory automatically
- Removed files cannot be recovered - use with caution

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

MIT License - see LICENSE file for details

## Credits

- Original PHP extension by [Sergii Ivashchenko](https://github.com/sivaschenko/magento2-clean-media)
- Go refactor by [Elgentos](https://github.com/elgentos)
