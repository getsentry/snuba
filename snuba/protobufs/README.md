# To build
```
# once
brew install protobuf
. .venv/bin/activate
pip install -r requirements-build.txt
pip install mypy-protobuf


protoc --proto_path=. --python_out=. --mypy_out=. $(find . -name '*.proto')
```
