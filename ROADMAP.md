# Roadmap

This document outlines planned features and improvements for CSV-ETL.

## Current Features (v1.0)

- [x] CSV file sources and destinations
- [x] Visual drag-and-drop mapping editor
- [x] Transform types: direct, constant, date_format, lookup, suffix, prefix, formula, conditional
- [x] File preview with validation before conversion
- [x] Inline editing of source files
- [x] Row-level error reporting

## Planned Features

### Database Support

#### Sources
- [ ] PostgreSQL
- [ ] MySQL / MariaDB
- [ ] SQLite
- [ ] Microsoft SQL Server
- [ ] MongoDB

#### Destinations
- [ ] PostgreSQL
- [ ] MySQL / MariaDB
- [ ] SQLite
- [ ] Microsoft SQL Server
- [ ] MongoDB

#### Database Features
- [ ] Connection string management with secure credential storage
- [ ] Schema introspection for automatic column detection
- [ ] Query builder for source data selection
- [ ] Batch insert/upsert for destinations
- [ ] Transaction support with rollback on errors

### Additional Data Sources
- [ ] Excel files (.xlsx, .xls)
- [ ] JSON files
- [ ] XML files
- [ ] REST API endpoints
- [ ] Google Sheets
- [ ] S3 / Cloud storage

### ETL Enhancements
- [ ] Scheduled/automated conversions
- [ ] Data validation rules
- [ ] Lookup tables from external sources
- [ ] Aggregation transforms (sum, count, avg)
- [ ] Row filtering in visual editor
- [ ] Multi-step pipelines (chain transformations)

### Authentication & User Management
- [ ] User registration and login
- [ ] OAuth providers (Google, GitHub)
- [ ] User profiles with settings
- [ ] Password reset flow

### Permissions & Sharing
- [ ] Private sources, destinations, and mappings (default)
- [ ] Public sources, destinations, and mappings (discoverable by all users)
- [ ] Share with specific users:
  - [ ] View-only permission
  - [ ] Edit permission
- [ ] Organization/team support
- [ ] Activity audit log

### UI/UX Improvements
- [ ] Dark mode
- [ ] Conversion history and logs
- [ ] Mapping templates library
- [ ] Bulk file processing
- [ ] Progress indicators for large files
- [ ] User dashboard with owned and shared resources

## Contributing

Interested in helping? Check out [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
