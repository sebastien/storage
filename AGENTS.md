# Agent Guidelines for Storage

## Build/Test Commands
- Run all tests: `make test` or `PYTHONPATH=src/py python -m unittest discover tests -p "*.py"`
- Run single test: `PYTHONPATH=src/py python -m unittest tests.storage_base.TestClass.test_method`
- Clean build files: `make clean`
- Check with pychecker: `make check`

## Code Style
- **Indentation**: Use TABS (size 4) for Python files (see `.editorconfig`)
- **Imports**: Group stdlib, then local modules; use `from .module import X` for relative imports
- **Types**: Use type hints (`typing` module: `ClassVar`, `Optional`, `List`, `Type`, etc.)
- **Naming**: Classes=PascalCase, functions/vars=camelCase, CONSTANTS=UPPER_SNAKE_CASE
- **Class Variables**: Use `ClassVar` type hint for class-level attributes
- **Error Handling**: Raise exceptions with descriptive messages (e.g., `Exception("Type not supported: %s" % type)`)
- **Comments**: Include module docstrings, TODO/FIXME markers for future work
- **License**: BSD License, FFCTN copyright headers in test files

## Architecture
- Storage backends in `src/py/storage/backends/`: MemoryBackend, DirectoryBackend, DBMBackend
- Core abstractions: `Storable`, `StoredObject`, `ObjectStorage`, `RawStorage`
- Tests mirror source structure in `tests/` directory
