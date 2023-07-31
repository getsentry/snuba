# Running


To run the app locally:

```bash
# assuming you're in the venv
snuba admin
```

The server should be running on http://127.0.0.1:1219

# Developing the Javascript

You must have node and yarn installed. To do so:

```
volta install node
volta install yarn
make build-admin
```

Then

```
make watch-admin
```

this will start the yarn debug server and live reload your javascript changes. If you change environment variables you'll have to restart the server

# Releasing new javascript

The admin tool is automatically built as part of our normal CI flow.
