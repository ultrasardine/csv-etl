# Contributing to CSV-ETL

Thank you for your interest in contributing to CSV-ETL! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment:

```bash
make install
```

## Development Workflow

1. Create a new branch for your feature or fix:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and ensure they follow the project's coding standards

3. Run linting and formatting:
```bash
make lint
make format
```

4. Run tests:
```bash
make test
```

5. Commit your changes with a clear, descriptive message

6. Push to your fork and submit a pull request

## Code Style

- Follow PEP 8 guidelines
- Use type hints for function signatures
- Keep functions focused and small
- Write docstrings for public functions and classes
- Use meaningful variable and function names

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure all tests pass
- Keep PRs focused on a single feature or fix
- Update documentation if needed

## Reporting Issues

When reporting issues, please include:

- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Python version and OS
- Any relevant error messages or logs

## Adding New Extractors/Transformers

If you want to add support for a new data source:

1. Create an extractor in `src/converter/extractors/`
2. Create a transformer in `src/converter/transformers/`
3. Add corresponding models in `src/converter/models/`
4. Update the CLI if needed
5. Add documentation and examples

## Questions?

Feel free to open an issue for any questions or discussions.
