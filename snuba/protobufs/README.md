# To build
```
brew install protobuf # once
protoc --proto_path=. --python_out=. --pyi_out=. $(find . -name '*.proto')
```
