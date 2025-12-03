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

### Automatic Configuration (Recommended)

The tool can automatically read database credentials from `app/etc/env.php` and derive the media path:

```bash
# Option 1: Run from Magento root directory (fully automatic)
cd /var/www/html/magento
./magento2-media-cleaner -u

# Option 2: Specify Magento root from anywhere (derives media path automatically)
./magento2-media-cleaner -u -magento-root="/var/www/html/magento"

# Option 3: Specify media path (auto-detects Magento root by traversing up)
./magento2-media-cleaner -u -media-path="/var/www/html/magento/pub/media/catalog/product"
```

The tool will automatically:
- Find the Magento root directory (by searching for `app/etc/env.php`)
- Read database credentials from `app/etc/env.php`
- Derive the media path as `<magento_root>/pub/media/catalog/product`

### Manual Configuration & Overrides

You can provide database credentials manually via command line flags. If you specify `-magento-root`, the tool will load `env.php` first and then **override** with any CLI flags you provide:

```bash
# Full manual configuration (no env.php)
./magento2-media-cleaner \
  -db-name="magento2" \
  -db-user="root" \
  -db-pass="password" \
  -media-path="/var/www/html/pub/media/catalog/product"

# Load from env.php but override specific values (e.g., use localhost instead of Docker host)
./magento2-media-cleaner \
  -magento-root="/var/www/html/magento" \
  -db-host="localhost" \
  -db-port="3308"

# Override just the database name
./magento2-media-cleaner \
  -magento-root="/var/www/html/magento" \
  -db-name="different_database"
```

**How overrides work:**
- The tool loads configuration from `env.php` if available
- Any explicitly provided CLI flags override the corresponding `env.php` values
- The tool shows which values were overridden: `(overridden: host, port)`

### List Operations

```bash
# List unused files (in filesystem but not referenced in DB)
./magento2-media-cleaner --list-unused
# or use shorthand:
./magento2-media-cleaner -u

# List missing files (referenced in DB but not in filesystem)
./magento2-media-cleaner --list-missing
# or use shorthand:
./magento2-media-cleaner -m

# List duplicate files
./magento2-media-cleaner --list-duplicates
# or use shorthand:
./magento2-media-cleaner -d
```

### Cleanup Operations

```bash
# Remove unused files from filesystem
./magento2-media-cleaner --remove-unused
# or use shorthand:
./magento2-media-cleaner -r

# Remove orphaned database entries (for missing files)
./magento2-media-cleaner --remove-orphans
# or use shorthand:
./magento2-media-cleaner -o

# Remove duplicate files and update all DB references to point to original
./magento2-media-cleaner --remove-duplicates
# or use shorthand:
./magento2-media-cleaner -x

# Combine operations (can mix long and short flags)
./magento2-media-cleaner --remove-unused --remove-orphans --remove-duplicates
# or use shorthand:
./magento2-media-cleaner -r -o -x
```

## Configuration Options

### Optional Flags

All configuration flags are now optional. The tool will attempt to read from `app/etc/env.php` if not provided:

- `-magento-root`: Path to Magento root directory (auto-detects if not provided)
- `-db-name`: Database name (reads from env.php if not provided)
- `-db-user`: Database username (reads from env.php if not provided)
- `-db-pass`: Database password (reads from env.php if not provided)
- `-db-host`: Database host (reads from env.php if not provided, default: `localhost`)
- `-db-port`: Database port (reads from env.php if not provided, default: `3306`)
- `-db-prefix`: Database table prefix (reads from env.php if not provided)
- `-media-path`: Absolute path to `pub/media/catalog/product` directory (derives from magento-root if not provided)
- `-workers`: Number of parallel workers for file scanning (default: `10`)

### Operation Flags

All operation flags support both long descriptive names and short aliases:

**List Operations:**
- `--list-unused` / `-u`: List unused media files
- `--list-missing` / `-m`: List missing media files
- `--list-duplicates` / `-d`: List duplicated files

**Cleanup Operations:**
- `--remove-unused` / `-r`: Remove unused product images
- `--remove-orphans` / `-o`: Remove orphaned media gallery rows
- `--remove-duplicates` / `-x`: Remove duplicated files and update database

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

The application uses a high-performance parallel architecture:

1. **Parallel Directory Walking**: Multiple goroutines walk the directory tree concurrently
2. **Worker Pool**: Configurable worker pool hashes files in parallel
3. **Single DB Connection**: Reuses one database connection for all queries
4. **In-Memory Comparison**: Builds hash maps and compares sets in memory (efficient at 20k entries)
5. **Progress Reporting**: Atomic counters track operations and report detailed statistics

### Key Components

- **Directory Walkers**: Parallel goroutines walk subdirectories concurrently using `os.ReadDir`
- **File Scanner**: Discovers files and dispatches them to worker pool via buffered channels
- **Worker Pool**: Concurrent goroutines hash files with xxHash (extremely fast non-cryptographic hash)
- **Database Layer**: Queries `catalog_product_entity_media_gallery` for all media paths
- **Comparator**: Builds sets and identifies unused/missing/duplicate files
- **Cleanup Engine**: Removes files and updates database with transaction safety

### Parallel Walking Design

- Uses `os.ReadDir` instead of `filepath.Walk` for parallel directory traversal
- Directory walkers: `workers / 2` goroutines (minimum 2)
- File processors: `workers` goroutines (default 10)
- Large buffered channels (10K files, 100 dirs) for high throughput
- Atomic counter tracks directories in-flight to detect completion

## Performance

- **Parallel Directory Walking**: Eliminates single-threaded `filepath.Walk` bottleneck
- **Parallel Hashing**: 10 workers can process ~1000 files/second on SSD using xxHash
- **Memory Efficient**: ~100MB RAM for 20k files
- **Fast Comparison**: O(n) complexity using hash maps
- **xxHash**: Non-cryptographic hash algorithm optimized for speed (faster than MD5/SHA)
- **Scales Well**: Performance increases with number of CPU cores

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
